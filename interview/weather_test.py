import pytest
from . import weather


def test_sample_yields():
    samples = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 15.0},
        {"type": "sample", "stationName": "A", "timestamp": 3, "temperature": 5.0},
        {"type": "sample", "stationName": "B", "timestamp": 4, "temperature": 20.0},
    ]
    output = list(weather.process_events(samples))
    assert output == samples

def test_non_sample_ignored():
    events = [
        {"type": "control", "command": "snapshot"},
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
    ]
    output = list(weather.process_events(events))
    # Only the sample should be yielded
    assert output == [events[1]]

def test_high_low_logic():
    samples = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 15.0},
        {"type": "sample", "stationName": "A", "timestamp": 3, "temperature": 5.0},
    ]
    # We can't access internal state, but we can check that all samples are yielded
    output = list(weather.process_events(samples))
    assert output == samples

def test_snapshot_with_data():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 15.0},
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    assert output[-1] == {
        "type": "snapshot",
        "asOf": 2,
        "stations": {"A": {"high": 15.0, "low": 10.0}},
    }

def test_snapshot_no_data():
    events = [
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    assert not output

def test_reset_with_data():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 15.0},
        {"type": "control", "command": "reset"},
        {"type": "sample", "stationName": "A", "timestamp": 3, "temperature": 5.0},
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    # After reset, only the last sample should be in the snapshot
    assert output[2] == {"type": "reset", "asOf": 2}
    assert output[4] == {
        "type": "snapshot",
        "asOf": 3,
        "stations": {"A": {"high": 5.0, "low": 5.0}},
    }

def test_reset_no_data():
    events = [
        {"type": "control", "command": "reset"},
    ]
    output = list(weather.process_events(events))
    assert not output

def test_unknown_type():
    events = [
        {"type": "unknown_type"},
    ]
    with pytest.raises(Exception) as excinfo:
        list(weather.process_events(events))
    assert excinfo.value.args[0] == "Unknown message type: unknown_type."

def test_unknown_control_command():
    events = [
        {"type": "control", "command": "unknown_command"},
    ]
    with pytest.raises(Exception) as excinfo:
        list(weather.process_events(events))
    assert excinfo.value.args[0] == "Unknown control command: unknown_command."
