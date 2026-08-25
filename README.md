# semantic-saga-mcp

A standalone [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that applies the Saga pattern to agentic workflows. It executes allow-listed side effects, keeps a durable SQLite journal, and automatically invokes compensating actions in reverse order when a step fails or the MCP client requests rollback.

## Guarantees

- **Write-ahead intent:** a step is stored as `EXECUTING` before its forward request. After a process crash, such an uncertain step is eligible for compensation.
- **Automatic rollback:** a failed step changes the saga to failed and rolls back it and previously completed steps. The failed request is included because a network error may occur after a remote mutation.
- **Reverse-order compensation:** completed mutations unwind from newest to oldest.
- **Idempotency:** forward and compensation requests receive stable `Idempotency-Key` headers. Endpoints **must honor these keys** because networks cannot provide exactly-once delivery.
- **Durability and inspection:** sagas, results, errors, and retry counts live in SQLite and are available through `get_saga`.
- **Session isolation:** every saga is owned by its transport session. Lookups, commits, steps, and rollbacks from another connected agent behave as if that saga does not exist.
- **Schema enforcement:** Pydantic strict models reject missing, mistyped, or unexpected JSON-RPC tool arguments before coordinator code can run.
- **Safe action surface:** agents select administrator-configured actions; they cannot supply arbitrary URLs or credentials.

This is a coordination framework, not an ACID transaction spanning independent systems. A compensation can itself fail. That state is reported as `ROLLBACK_FAILED` for operator or client retry rather than being hidden.

## Quick start

Python 3.11 or newer is required.

```bash
python -m pip install -e .
semantic-saga-mcp --actions ./examples/actions.json --database ./semantic-saga.db
```

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

### Remote SSE transport

The default `stdio` transport is intended for local IDE and desktop integrations. For remote agents, run the MCP SSE transport instead:

```bash
semantic-saga-mcp --transport sse --host 0.0.0.0 --port 8000 \
  --actions ./examples/actions.json --database ./semantic-saga.db
```

Set `SAGA_TRANSPORT`, `SAGA_HOST`, and `SAGA_PORT` instead of the corresponding flags if desired. SSE clients connect to `GET /sse`; the server emits that connection's unique `POST /messages?session_id=...` endpoint. Deploy behind TLS and authentication at a trusted reverse proxy when exposing the service outside a private network.

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
| `get_saga` | Returns the durable saga and step journal. |

A typical client flow is:

1. Call `begin_saga` and retain `id`.
2. Call `execute_saga_step` for each mutation with that `saga_id`.
3. Call `commit_saga` only when the whole workflow is accepted.
4. Call `rollback_saga` on a client-side validation error or hallucination. Server-side action errors trigger this automatically.

## Development

```bash
python -m unittest discover -s tests -v
```

The MCP transport writes only JSON-RPC messages to stdout. Keep application diagnostics on stderr so clients can parse the protocol stream.
