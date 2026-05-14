"""Package shim exposing root-level producer module as kafka.producer."""

from producer import *

__all__ = [
    "make_clickstream_event",
    "make_sensor_event",
    "make_log_event",
    "pick_event",
    "delivery_report",
    "wait_for_kafka",
    "main",
    "BOOTSTRAP_SERVERS",
    "EVENTS_PER_SECOND",
    "TOPIC_RAW",
]
