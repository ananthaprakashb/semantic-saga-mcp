from __future__ import annotations

import contextlib
import os
import time
from contextvars import ContextVar, copy_context
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Mapping

from opentelemetry import context as otel_context
from opentelemetry import metrics, propagate, trace
from opentelemetry.trace import Status, StatusCode


TRACE_META_KEYS = ("traceparent", "tracestate", "baggage")
_actor_context: ContextVar[tuple[str | None, str | None]] = ContextVar(
    "semantic_saga_actor", default=(None, None)
)


def _primitive_attributes(values: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return OTel-safe attributes without serializing arbitrary payloads."""
    result: dict[str, Any] = {}
    for key, value in (values or {}).items():
        if value is None:
            continue
        if isinstance(value, (str, bool, int, float)):
            result[str(key)] = value
        elif isinstance(value, (list, tuple)) and all(isinstance(item, (str, bool, int, float)) for item in value):
            result[str(key)] = list(value)
    return result


def _parse_otlp_headers(value: str | None) -> dict[str, str] | None:
    if not value:
        return None
    result: dict[str, str] = {}
    for item in value.split(","):
        key, separator, header_value = item.partition("=")
        key = key.strip()
        if not separator or not key:
            raise RuntimeError("OTLP headers must use comma-separated key=value entries")
        result[key] = header_value.strip()
    return result


def current_trace_ids() -> tuple[str | None, str | None]:
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if not span_context.is_valid:
        return None, None
    return f"{span_context.trace_id:032x}", f"{span_context.span_id:016x}"


def current_actor() -> tuple[str | None, str | None]:
    return _actor_context.get()


@contextlib.contextmanager
def actor_scope(principal_id: str | None, principal_type: str | None) -> Iterator[None]:
    token = _actor_context.set((principal_id, principal_type))
    try:
        yield
    finally:
        _actor_context.reset(token)


def inject_current_trace(headers: dict[str, str]) -> None:
    """Inject W3C Trace Context into a downstream HTTP carrier."""
    propagate.inject(headers)


def mcp_trace_carrier(meta: Mapping[str, Any] | None, headers: Mapping[str, Any] | None = None) -> dict[str, str]:
    """Build a W3C carrier from MCP `_meta`, falling back to HTTP headers."""
    carrier: dict[str, str] = {}
    for key in TRACE_META_KEYS:
        value = (meta or {}).get(key)
        if isinstance(value, str) and value:
            carrier[key] = value
    if headers:
        lowered = {str(key).lower(): value for key, value in headers.items()}
        for key in TRACE_META_KEYS:
            if key not in carrier:
                value = lowered.get(key)
                if isinstance(value, str) and value:
                    carrier[key] = value
    return carrier


@contextlib.contextmanager
def attach_mcp_trace(meta: Mapping[str, Any] | None, headers: Mapping[str, Any] | None = None) -> Iterator[None]:
    carrier = mcp_trace_carrier(meta, headers)
    if not carrier:
        yield
        return
    token = otel_context.attach(propagate.extract(carrier))
    try:
        yield
    finally:
        otel_context.detach(token)


def capture_otel_context() -> Any:
    return otel_context.get_current()


@contextlib.contextmanager
def attach_otel_context(ctx: Any | None) -> Iterator[None]:
    if ctx is None:
        yield
        return
    token = otel_context.attach(ctx)
    try:
        yield
    finally:
        otel_context.detach(token)


def submit_with_current_context(pool: Any, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Submit work while preserving the current ContextVar/OTel parent context."""
    ctx = copy_context()
    return pool.submit(ctx.run, fn, *args, **kwargs)


@dataclass
class Telemetry:
    service_name: str = "semantic-saga-mcp"

    def __post_init__(self) -> None:
        self.tracer = trace.get_tracer(self.service_name)
        self.meter = metrics.get_meter(self.service_name)
        self.saga_counter = self.meter.create_counter(
            "semantic_saga.saga.operations", unit="{operation}", description="Saga lifecycle operations."
        )
        self.action_attempt_counter = self.meter.create_counter(
            "semantic_saga.action.attempts", unit="{attempt}", description="Forward action execution attempts."
        )
        self.compensation_attempt_counter = self.meter.create_counter(
            "semantic_saga.compensation.attempts", unit="{attempt}", description="Compensation attempts."
        )
        self.approval_counter = self.meter.create_counter(
            "semantic_saga.approval.decisions", unit="{decision}", description="Human or operator approval decisions."
        )
        self.recovery_counter = self.meter.create_counter(
            "semantic_saga.recovery.operations", unit="{operation}", description="Recovery claims and operator escalations."
        )
        self.policy_counter = self.meter.create_counter(
            "semantic_saga.policy.decisions", unit="{decision}", description="Governance policy decisions by effect/backend."
        )
        self.action_duration = self.meter.create_histogram(
            "semantic_saga.action.duration", unit="s", description="Forward action duration including configured retries."
        )
        self.compensation_duration = self.meter.create_histogram(
            "semantic_saga.compensation.duration", unit="s", description="Compensation duration including configured retries."
        )
        self.policy_duration = self.meter.create_histogram(
            "semantic_saga.policy.duration", unit="s", description="Governance policy evaluation duration."
        )

    @contextlib.contextmanager
    def span(self, name: str, attributes: Mapping[str, Any] | None = None) -> Iterator[Any]:
        with self.tracer.start_as_current_span(name, attributes=_primitive_attributes(attributes)) as span:
            try:
                yield span
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)[:256]))
                raise

    def event(self, name: str, attributes: Mapping[str, Any] | None = None) -> None:
        span = trace.get_current_span()
        if span.is_recording():
            span.add_event(name, _primitive_attributes(attributes))

    def record_action(self, *, action: str, attempts: int, outcome: str, duration_seconds: float) -> None:
        attrs = {"semantic_saga.action": action, "semantic_saga.outcome": outcome}
        self.action_attempt_counter.add(attempts, attrs)
        self.action_duration.record(duration_seconds, attrs)

    def record_compensation(self, *, action: str, attempts: int, outcome: str, duration_seconds: float) -> None:
        attrs = {"semantic_saga.action": action, "semantic_saga.outcome": outcome}
        self.compensation_attempt_counter.add(attempts, attrs)
        self.compensation_duration.record(duration_seconds, attrs)


def configure_telemetry(
    *, service_name: str = "semantic-saga-mcp", endpoint: str | None = None, headers: str | None = None,
) -> Telemetry:
    """Configure OTLP traces/metrics when an endpoint is supplied."""
    endpoint = endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    exporter_headers = _parse_otlp_headers(headers or os.getenv("OTEL_EXPORTER_OTLP_HEADERS"))
    if endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "OTLP export requires the optional observability dependencies; install semantic-saga-mcp[otel]"
            ) from exc

        resource = Resource.create({"service.name": service_name})
        base_endpoint = endpoint.rstrip("/")
        trace_provider = TracerProvider(resource=resource)
        trace_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=base_endpoint + "/v1/traces", headers=exporter_headers))
        )
        trace.set_tracer_provider(trace_provider)
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=base_endpoint + "/v1/metrics", headers=exporter_headers)
        )
        metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))
    return Telemetry(service_name=service_name)


def monotonic_seconds() -> float:
    return time.monotonic()
