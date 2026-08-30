from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import anyio
import jwt
from jwt import PyJWKClient
from mcp.server.auth.provider import AccessToken, TokenVerifier


class IdentityError(RuntimeError):
    pass


class AuthorizationError(RuntimeError):
    pass


class StaticTokenVerifier(TokenVerifier):
    """Development/test verifier loaded from an operator-owned JSON file.

    The file is a mapping from bearer token to AccessToken-compatible fields.
    It is intentionally not a production credential store; production remote
    deployments should use signed JWT access tokens from an external IdP.
    """

    def __init__(self, tokens: dict[str, AccessToken]) -> None:
        self._tokens = tokens

    @classmethod
    def from_file(cls, path: str) -> "StaticTokenVerifier":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise IdentityError("Static token file must be a JSON object")
        tokens: dict[str, AccessToken] = {}
        for token, definition in raw.items():
            if not isinstance(token, str) or not token or not isinstance(definition, dict):
                raise IdentityError("Static token entries must map non-empty token strings to objects")
            scopes = definition.get("scopes", [])
            if not isinstance(scopes, list) or not all(isinstance(item, str) for item in scopes):
                raise IdentityError(f"Static token {token!r} has invalid scopes")
            claims = definition.get("claims", {})
            if not isinstance(claims, dict):
                raise IdentityError(f"Static token {token!r} has invalid claims")
            client_id = definition.get("client_id")
            if not isinstance(client_id, str) or not client_id:
                raise IdentityError(f"Static token {token!r} requires client_id")
            tokens[token] = AccessToken(
                token=token,
                client_id=client_id,
                scopes=scopes,
                expires_at=definition.get("expires_at"),
                subject=definition.get("subject"),
                claims=claims,
            )
        return cls(tokens)

    async def verify_token(self, token: str) -> AccessToken | None:
        return self._tokens.get(token)


class JwtTokenVerifier(TokenVerifier):
    """Validate signed OAuth access tokens using an IdP JWKS endpoint."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        jwks_url: str,
        algorithms: Iterable[str] = ("RS256",),
    ) -> None:
        self.issuer = issuer.rstrip("/")
        self.audience = audience
        self.algorithms = tuple(algorithms)
        if not self.algorithms:
            raise IdentityError("At least one JWT algorithm is required")
        self._jwks = PyJWKClient(jwks_url)

    @staticmethod
    def _scopes(claims: dict[str, Any]) -> list[str]:
        raw = claims.get("scope", claims.get("scp", []))
        if isinstance(raw, str):
            return [item for item in raw.split() if item]
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, str)]
        return []

    def _verify_sync(self, token: str) -> AccessToken | None:
        try:
            signing_key = self._jwks.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=list(self.algorithms),
                audience=self.audience,
                issuer=self.issuer,
                options={"require": ["exp", "iss", "aud"]},
            )
        except jwt.PyJWTError:
            return None

        client_id = claims.get("client_id") or claims.get("azp") or claims.get("appid") or claims.get("sub")
        if not isinstance(client_id, str) or not client_id:
            return None
        subject = claims.get("sub")
        if subject is not None and not isinstance(subject, str):
            subject = None
        expires_at = claims.get("exp")
        if expires_at is not None and not isinstance(expires_at, int):
            return None
        return AccessToken(
            token=token,
            client_id=client_id,
            scopes=self._scopes(claims),
            expires_at=expires_at,
            subject=subject,
            claims=claims,
        )

    async def verify_token(self, token: str) -> AccessToken | None:
        return await anyio.to_thread.run_sync(self._verify_sync, token)


@dataclass(frozen=True)
class AuthorizationPolicy:
    """Small, deterministic RBAC layer independent of any particular IdP."""

    read_scope: str = "semantic-saga:read"
    execute_scope: str = "semantic-saga:execute"
    admin_scope: str = "semantic-saga:admin"

    READ_TOOLS = frozenset({"get_saga"})
    MUTATION_TOOLS = frozenset(
        {
            "begin_saga",
            "execute_saga_step",
            "commit_saga",
            "rollback_saga",
            "trigger_rollback",
        }
    )

    def authorize(self, tool_name: str, *, roles: Iterable[str], scopes: Iterable[str]) -> None:
        role_set = {item.lower() for item in roles}
        scope_set = set(scopes)
        if "admin" in role_set or self.admin_scope in scope_set:
            return
        if tool_name in self.READ_TOOLS and (
            role_set.intersection({"viewer", "operator"}) or self.read_scope in scope_set or self.execute_scope in scope_set
        ):
            return
        if tool_name in self.MUTATION_TOOLS and ("operator" in role_set or self.execute_scope in scope_set):
            return
        raise AuthorizationError(f"Principal is not authorized to call {tool_name}")
