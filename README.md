# semantic-saga-mcp

A standalone [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that applies the Saga pattern to agentic workflows. It executes allow-listed side effects, journals their state, and automatically invokes compensating actions in reverse order when a step fails or the MCP client requests rollback.

Version 0.2 uses the official MCP Python SDK v2 for modern stdio and Streamable HTTP. It supports the MCP `2026-07-28` stateless request model while retaining compatibility with older handshake-era clients through the SDK. The old dedicated HTTP+SSE transport remains available only as a deprecated migration path.

## Guarantees

- **Write-ahead intent:** a step is stored as `EXECUTING` before its forward request. After a process crash, such an uncertain step is eligible for compensation.
- **Automatic rollback:** a failed step changes the saga to failed and rolls back it and previously completed steps. The failed request is included because a network error may occur after a remote mutation.
- **Reverse-order compensation:** completed mutations unwind from newest to oldest.
- **Idempotency:** forward and compensation requests receive stable `Idempotency-Key` headers. Endpoints **must honor these keys** because networks cannot provide exactly-once delivery.
- **Pluggable storage:** the dependency-free in-memory store is the default. A durable adapter enables recovery after a restart, and the storage protocol keeps the coordinator independent of SQLite, Redis, or PostgreSQL.
- **Transport-independent ownership:** modern HTTP saga access is scoped by a stable execution identity rather than a sticky MCP transport session, so a new connection can continue a durable saga by explicit `saga_id` when it has the same ownership scope.
- **Strict schema enforcement:** Pydantic strict models reject missing, mistyped, or unexpected tool arguments before coordinator code can run, including when requests arrive through the official low-level MCP SDK.
- **Safe action surface:** agents select administrator-configured actions; they cannot supply arbitrary URLs or credentials.
- **Fail-closed remote binding:** non-local Streamable HTTP requires an explicit Host allowlist and, by default, a trusted reverse-proxy identity before saga tools can execute.

This is a coordination framework, not an ACID transaction spanning independent systems. A compensation can itself fail. That state is reported as `ROLLBACK_FAILED` for operator or client retry rather than being hidden.

## Quick start

Python 3.10 or newer is required.

```bash
python -m pip install -e .
semantic-saga-mcp
```

Installing the package is recommended because it also installs runtime dependencies. Tests can nevertheless import the `src` layout directly from a checkout, so `python -m unittest discover -s tests -v` does not require an editable install after dependencies have been installed.

Example MCP client configuration:

```json
{
  "mcpServers": {
    "semantic-saga": {
      "command": "semantic-saga-mcp",
      "args": ["--actions", "/absolute/path/actions.json", "--database", "/absolute/path/sagas.db"]
    }
  }
}
```

Environment variables `SAGA_ACTIONS_FILE` and `SAGA_DATABASE` are alternatives to CLI flags.

### Built-in file transaction

The server always registers a `create_text_file` action backed by `FileTransactionTool`. Invoke it through `execute_saga_step` with `input` containing a relative `.txt` `path` and string `content`. Files are confined to `./saga-files` by default; use `--file-root` or `SAGA_FILE_ROOT` to choose another root. Compensation deletes only that step's file, and existing files are never overwritten.

Run the complete local demonstration with:

```bash
python examples/file_transaction_demo.py
```

The demo creates `demo-1.txt`, `demo-2.txt`, and `demo-3.txt`, deliberately fails the fourth action, and prints the reverse-order deletion of the first three files as automatic rollback runs.

### Claude Desktop

A ready-to-customize Claude Desktop configuration is provided at [`docs/claude_desktop_config.json`](docs/claude_desktop_config.json). To use it locally:

1. From this repository, create a virtual environment and install the server:

   ```bash
   python -m venv .venv
   .venv/bin/python -m pip install -e .
   ```

2. In the JSON file, replace every `/ABSOLUTE/PATH/TO/semantic-saga-mcp` with this repository's absolute path.
3. Copy the resulting `mcpServers.semantic-saga` entry into the `mcpServers` object in your Claude Desktop configuration, then restart Claude Desktop.

The configuration starts the local stdio transport, enables durable SQLite recovery, and places files created by the built-in `create_text_file` action in this repository's `saga-files` directory. No HTTP action configuration is required for the file transaction demo.

### Dry run

Pass `--dry-run` (or set `SAGA_DRY_RUN=true`) to validate a failure and rollback flow without making any HTTP requests. Each forward action is rendered and logged to stderr, then deliberately fails so the coordinator enters rollback. The expected compensation request is rendered and logged rather than sent. Sensitive authentication headers are redacted from previews.

### Storage and crash recovery

Without `--database`, the server uses `SagaStore`, a thread-safe in-memory adapter. This is convenient for development, but its journal disappears when the process exits. Enable the included durable SQLite adapter with `--database ./semantic-saga.db`. On startup, the server finds interrupted rollbacks and uncertain `EXECUTING` steps in durable storage and resumes compensation automatically.

Developers can plug in Redis or PostgreSQL without changing the coordinator by implementing `SagaStoreProtocol` from `semantic_saga_mcp.store`. Its domain-level methods cover saga and step creation, lookup, updates, ordered step listing, and recovery discovery. Mutations in a durable implementation must be committed before returning; `create_step` must also allocate its per-saga sequence atomically. Inject the adapter with `Coordinator(custom_store, actions)` and invoke `resume_pending_rollbacks()` once the action registry is available during application startup.

## Remote Streamable HTTP

The default `stdio` transport is intended for local IDE and desktop integrations. For a local HTTP integration, run:

```bash
semantic-saga-mcp --transport streamable-http \
  --host 127.0.0.1 --port 8000 \
  --actions ./examples/actions.json \
  --database ./semantic-saga.db
```

Clients connect to `http://127.0.0.1:8000/mcp`. The official MCP SDK handles modern `2026-07-28` discovery and older initialization-era compatibility on this endpoint. Modern requests are not owned by an MCP transport session; the explicit `saga_id` is the durable workflow handle.

### Non-local deployment

For a real hostname, put Semantic Saga behind TLS and an authenticated reverse proxy. The server requires an explicit Host allowlist and trusted identity mode when binding beyond localhost:

```bash
semantic-saga-mcp --transport streamable-http \
  --host 0.0.0.0 --port 8000 \
  --allowed-host mcp.example.com \
  --allowed-host 'mcp.example.com:*' \
  --allowed-origin https://agents.example.com \
  --trust-identity-headers \
  --actions ./examples/actions.json \
  --database ./semantic-saga.db
```

The authenticated proxy must:

1. authenticate the caller before forwarding `/mcp`;
2. remove any inbound client-supplied `X-Semantic-Saga-Tenant` and `X-Semantic-Saga-Principal` headers;
3. inject a validated `X-Semantic-Saga-Principal` on every forwarded request, plus `X-Semantic-Saga-Tenant` when tenant scoping is available; and
4. prevent clients from bypassing the proxy and reaching the Semantic Saga process directly.

The server hashes the tenant/principal pair before using it as the stored ownership scope. Raw proxy identity is not written into the saga ownership key. If an HTTP `Authorization` header is present outside protected proxy mode, its value is also hashed for ownership isolation; **that hashing does not validate or authenticate the credential**.

Native MCP OAuth/OIDC authorization is intentionally deferred to the enterprise identity phase. Until then, the trusted reverse proxy is the security boundary for non-local deployment.

`--allow-unauthenticated-http` (or `SAGA_ALLOW_UNAUTHENTICATED_HTTP=true`) bypasses the remote identity requirement. It exists for controlled private-network migration and should not be used for an internet-facing action server.

Environment variables for remote transport include `SAGA_TRANSPORT`, `SAGA_HOST`, `SAGA_PORT`, `SAGA_ALLOWED_HOSTS`, `SAGA_ALLOWED_ORIGINS`, `SAGA_TRUST_IDENTITY_HEADERS`, and `SAGA_ALLOW_UNAUTHENTICATED_HTTP`. Host and Origin lists are comma-separated.

### Deprecated dedicated SSE transport

The previous dedicated SSE transport is retained temporarily for migration:

```bash
semantic-saga-mcp --transport sse --host 127.0.0.1 --port 8000
```

It uses `GET /sse` plus a session-specific `POST /messages?session_id=...` endpoint and retains the older `2025-06-18` behavior. New deployments should use Streamable HTTP instead.

## Configure actions

Action configuration is controlled by the server operator. Each action pairs one forward HTTP request with one rollback request:

```json
{
  "charge_card": {
    "forward": {
      "url": "https://payments.internal/charges",
      "method": "POST",
      "headers": {"Authorization": "Bearer configured-secret"},
      "body": {"amount": "${input.amount}", "account": "${input.account}"},
      "timeout_seconds": 15
    },
    "rollback": {
      "url": "https://payments.internal/refunds",
      "method": "POST",
      "headers": {"Authorization": "Bearer configured-secret"},
      "body": {"charge_id": "${result.charge_id}"}
    }
  }
}
```

An entire string may be a typed template value. Supported roots are `input`, `result`, `saga`, and `step`, for example `${input.amount}`, `${result.charge_id}`, `${saga.id}`, and `${step.id}`. Do not commit secrets in an action file; generate a protected runtime configuration instead.

## MCP tools

| Tool | Purpose |
| --- | --- |
| `begin_saga` | Creates an `ACTIVE` saga and returns its ID. |
| `execute_saga_step` | Runs a configured action. A failure automatically starts rollback. |
| `commit_saga` | Finalizes a successful saga and prevents later rollback. |
| `rollback_saga` | Explicitly compensates eligible steps in reverse order. |
| `trigger_rollback` | Immediately compensates eligible steps after an error. |
| `get_saga` | Returns the durable saga and step journal. |

## MCP prompt

The `saga-coordinator` prompt is advertised through `prompts/list` and returned by `prompts/get`. It tells an LLM to wrap multi-step infrastructure changes in the Saga Coordinator and to invoke `trigger_rollback` immediately after an error.

A typical client flow is:

1. Call `begin_saga` and retain `id`.
2. Call `execute_saga_step` for each mutation with that `saga_id`.
3. Call `commit_saga` only when the whole workflow is accepted.
4. Call `rollback_saga` on a client-side validation error or hallucination. Server-side action errors trigger this automatically.

## Development

```bash
python -m unittest discover -s tests -v
```

Pull requests also run CI across Python 3.10-3.13, build the package, and perform a real Streamable HTTP smoke test. That smoke test creates a saga through one MCP HTTP client, disconnects it, and retrieves the same saga through a new client to verify transport-stateless continuation.

The stdio transport writes only MCP protocol messages to stdout. Keep application diagnostics on stderr so clients can parse the protocol stream.

## Publishing to PyPI

Package metadata, the MIT license, typed-package marker, console entry point, PR CI, and a trusted-publishing GitHub Actions workflow are included. Before publishing a release:

1. Update `__version__` in `src/semantic_saga_mcp/__init__.py` and commit it.
2. Run the tests and build validation locally:

   ```bash
   python -m pip install --upgrade build twine
   python -m unittest discover -s tests -v
   python -m build
   python -m twine check dist/*
   ```

3. Confirm both the wheel and source distribution contain `LICENSE`, `README.md`, and `semantic_saga_mcp/py.typed`.
4. Configure a PyPI Trusted Publisher for this repository with workflow `publish.yml` and environment `pypi`.
5. Publish a GitHub release. The workflow builds once, validates the distributions, and publishes that exact artifact to PyPI using OpenID Connect rather than a long-lived API token.

PyPI does not allow a released filename/version to be replaced. Increment `__version__` for every release, including corrections to a failed or incomplete publication.
