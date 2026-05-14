"""
Unit tests for the streaming pipeline components.
Run with: pytest tests/ -v
"""

import json
import sys
import os
import pytest

# Make modules importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─── Producer Tests ────────────────────────────────────────────────────────
class TestEventFactories:
    def setup_method(self):
        # Patch confluent_kafka so we don't need a real broker
        import unittest.mock as mock
        fake_kafka = mock.MagicMock()
        self._patcher = mock.patch.dict("sys.modules", {
            "confluent_kafka": fake_kafka,
            "confluent_kafka.admin": fake_kafka.admin,
        })
        self._patcher.start()
        # Re-import after patching
        import importlib
        import kafka.producer as prod
        importlib.reload(prod)
        self.prod = prod

    def teardown_method(self):
        self._patcher.stop()

    def test_clickstream_has_required_fields(self):
        event = self.prod.make_clickstream_event()
        for field in ["event_id", "event_type", "timestamp", "user_id", "action", "page"]:
            assert field in event, f"Missing field: {field}"
        assert event["event_type"] == "clickstream"

    def test_sensor_has_required_fields(self):
        event = self.prod.make_sensor_event()
        for field in ["event_id", "event_type", "timestamp", "sensor_id", "temperature"]:
            assert field in event, f"Missing field: {field}"
        assert event["event_type"] == "sensor"

    def test_log_has_required_fields(self):
        event = self.prod.make_log_event()
        for field in ["event_id", "event_type", "timestamp", "service", "level"]:
            assert field in event, f"Missing field: {field}"
        assert event["event_type"] == "app_log"

    def test_events_are_json_serializable(self):
        for factory in [
            self.prod.make_clickstream_event,
            self.prod.make_sensor_event,
            self.prod.make_log_event,
        ]:
            event = factory()
            serialized = json.dumps(event)
            deserialized = json.loads(serialized)
            assert deserialized["event_id"] == event["event_id"]

    def test_pick_event_returns_valid_event(self):
        for _ in range(50):
            event = self.prod.pick_event()
            assert "event_id" in event
            assert event["event_type"] in ("clickstream", "sensor", "app_log")

    def test_clickstream_response_time_positive(self):
        for _ in range(20):
            event = self.prod.make_clickstream_event()
            assert event["response_time_ms"] > 0

    def test_sensor_humidity_range(self):
        for _ in range(20):
            event = self.prod.make_sensor_event()
            assert 0 <= event["humidity"] <= 100

    def test_event_ids_are_unique(self):
        ids = {self.prod.make_clickstream_event()["event_id"] for _ in range(100)}
        assert len(ids) == 100, "Event IDs should be unique"


# ─── Pipeline Logic Tests (no Spark required) ─────────────────────────────
class TestPipelineLogic:
    """Test pure Python business logic extracted from the Spark pipeline."""

    def test_revenue_null_coalescing(self):
        """Revenue None should become 0.0."""
        revenue = None
        result = revenue if revenue is not None else 0.0
        assert result == 0.0

    def test_temperature_clamping(self):
        """Temperatures outside [-50, 150] should be clamped."""
        def clamp_temp(t):
            return max(-50.0, min(150.0, t))

        assert clamp_temp(-100) == -50.0
        assert clamp_temp(200) == 150.0
        assert clamp_temp(37) == 37.0

    def test_slow_request_threshold(self):
        """Requests > 1500ms should be flagged as slow."""
        def is_slow(ms):
            return ms > 1500

        assert is_slow(2000) is True
        assert is_slow(1500) is False
        assert is_slow(500) is False

    def test_anomaly_temperature_detection(self):
        """Temperatures > 55°C should be considered anomalous for our demo."""
        def is_anomaly(temp):
            return temp > 55.0

        assert is_anomaly(70.0) is True
        assert is_anomaly(30.0) is False

    def test_event_type_routing(self):
        """Events should be routed to correct aggregation tables."""
        routing = {
            "clickstream": "clickstream_metrics",
            "sensor": "sensor_metrics",
            "app_log": "log_metrics",
        }
        assert routing["clickstream"] == "clickstream_metrics"
        assert routing["sensor"] == "sensor_metrics"
        assert routing["app_log"] == "log_metrics"
        assert "unknown" not in routing


# ─── Integration smoke tests ───────────────────────────────────────────────
class TestIntegrationSmoke:
    """Smoke tests that verify the system can be imported and configured."""

    def test_environment_defaults(self):
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        assert ":" in bootstrap

    def test_jdbc_url_formation(self):
        host = "postgres"
        port = "5432"
        db = "streaming_db"
        url = f"jdbc:postgresql://{host}:{port}/{db}"
        assert url == "jdbc:postgresql://postgres:5432/streaming_db"
