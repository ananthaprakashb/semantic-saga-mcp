import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from semantic_saga_mcp.auth import JwtTokenVerifier


class FakeJwks:
    def __init__(self, public_key):
        self.public_key = public_key

    def get_signing_key_from_jwt(self, token):
        return SimpleNamespace(key=self.public_key)


class JwtTokenVerifierTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.public_key = self.private_key.public_key()
        self.issuer = "https://idp.example.com/tenant/"
        self.audience = "https://mcp.example.com"
        self.verifier = JwtTokenVerifier(
            issuer=self.issuer,
            audience=self.audience,
            jwks_url="https://idp.example.com/unused-jwks",
            algorithms=["RS256"],
        )
        self.verifier._jwks = FakeJwks(self.public_key)

    def encode(self, **overrides):
        claims = {
            "iss": self.issuer,
            "aud": self.audience,
            "exp": int(time.time()) + 300,
            "sub": "alice",
            "client_id": "agent-client",
            "tenant_id": "acme",
            "roles": ["operator"],
            "scope": "semantic-saga semantic-saga:execute",
        }
        claims.update(overrides)
        return jwt.encode(claims, self.private_key, algorithm="RS256")

    async def test_accepts_valid_signed_token_and_preserves_claims(self):
        accepted = await self.verifier.verify_token(self.encode())
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted.subject, "alice")
        self.assertEqual(accepted.client_id, "agent-client")
        self.assertEqual(accepted.claims["tenant_id"], "acme")
        self.assertIn("semantic-saga:execute", accepted.scopes)

    async def test_rejects_wrong_audience(self):
        self.assertIsNone(await self.verifier.verify_token(self.encode(aud="https://other.example.com")))

    async def test_rejects_wrong_issuer_including_trailing_slash_difference(self):
        self.assertIsNone(await self.verifier.verify_token(self.encode(iss=self.issuer.rstrip("/"))))

    async def test_rejects_expired_token(self):
        self.assertIsNone(await self.verifier.verify_token(self.encode(exp=int(time.time()) - 60)))


if __name__ == "__main__":
    unittest.main()
