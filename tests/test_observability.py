import unittest

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace

from semantic_saga_mcp.observability import (
    Telemetry,
    _parse_otlp_headers,
    actor_scope,
    attach_mcp_trace,
    current_actor,
    current_trace_ids,
    inject_current_trace,
    mcp_trace_carrier,
)


class TraceCarrierTests(unittest.TestCase):
    def test_mcp_meta_wins_and_headers_are_fallback(self):
        meta_parent = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01"
        header_parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        carrier = mcp_trace_carrier(
            {"traceparent": meta_parent, "baggage": "tenant=ignored-for-auth"},
            {"Traceparent": header_parent, "Tracestate": "vendor=value"},
        )
        self.assertEqual(carrier["traceparent"], meta_parent)
        self.assertEqual(carrier["tracestate"], "vendor=value")
        self.assertEqual(carrier["baggage"], "tenant=ignored-for-auth")

    def test_otlp_headers_parse_to_mapping(self):
        self.assertEqual(
            _parse_otlp_headers("Authorization=Bearer abc,x-team=saga"),
            {"Authorization": "Bearer abc", "x-team": "saga"},
        )
        self.assertIsNone(_parse_otlp_headers(None))
        with self.assertRaisesRegex(RuntimeError, "key=value"):
            _parse_otlp_headers("broken")

    def test_actor_scope_is_request_local(self):
        self.assertEqual(current_actor(), (None, None))
        with actor_scope("alice", "user"):
            self.assertEqual(current_actor(), ("alice", "user"))
        self.assertEqual(current_actor(), (None, None))


try:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
except ImportError:  # base installation intentionally carries only the OTel API
    TracerProvider = None


@unittest.skipIf(TracerProvider is None, "OpenTelemetry SDK optional extra not installed")
class OpenTelemetrySdkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.exporter = InMemorySpanExporter()
        cls.provider = TracerProvider()
        cls.provider.add_span_processor(SimpleSpanProcessor(cls.exporter))
        # A test process gets one global provider. If another test already set
        # one, use it instead of attempting an unsupported replacement.
        existing = trace.get_tracer_provider()
        if type(existing).__name__ == "ProxyTracerProvider":
            trace.set_tracer_provider(cls.provider)
            cls.active_provider = cls.provider
        else:
            cls.active_provider = existing

    def setUp(self):
        self.exporter.clear()

    def test_mcp_parent_context_flows_to_child_and_downstream(self):
        parent_trace_id = "0af7651916cd43dd8448eb211c80319c"
        parent_span_id = "00f067aa0ba902b7"
        traceparent = f"00-{parent_trace_id}-{parent_span_id}-01"
        telemetry = Telemetry("phase6-test")

        with attach_mcp_trace({"traceparent": traceparent}):
            with telemetry.span("semantic_saga.test.child"):
                trace_id, span_id = current_trace_ids()
                self.assertEqual(trace_id, parent_trace_id)
                self.assertNotEqual(span_id, parent_span_id)
                headers = {}
                inject_current_trace(headers)
                self.assertIn("traceparent", headers)
                parts = headers["traceparent"].split("-")
                self.assertEqual(parts[1], parent_trace_id)
                self.assertEqual(parts[2], span_id)

    def test_extracted_context_can_be_used_without_auth_side_effects(self):
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        carrier = {"traceparent": traceparent}
        extracted = propagate.extract(carrier)
        token = otel_context.attach(extracted)
        try:
            trace_id, span_id = current_trace_ids()
            self.assertEqual(trace_id, "4bf92f3577b34da6a3ce929d0e0e4736")
            self.assertEqual(span_id, "00f067aa0ba902b7")
            self.assertEqual(current_actor(), (None, None))
        finally:
            otel_context.detach(token)


if __name__ == "__main__":
    unittest.main()
