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

def test_missing_timestamp():
    events = [
        {"type": "sample", "stationName": "A", "temperature": 10.0},
    ]
    with pytest.raises(ValueError) as excinfo:
        list(weather.process_events(events))
    assert "missing 'timestamp'" in str(excinfo.value)

def test_missing_station_name():
    events = [
        {"type": "sample", "timestamp": 1, "temperature": 10.0},
    ]
    with pytest.raises(KeyError):
        list(weather.process_events(events))

def test_missing_temperature():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1},
    ]
    with pytest.raises(KeyError):
        list(weather.process_events(events))

def test_interleaved_stations():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "B", "timestamp": 2, "temperature": 20.0},
        {"type": "sample", "stationName": "A", "timestamp": 3, "temperature": 5.0},
        {"type": "sample", "stationName": "B", "timestamp": 4, "temperature": 25.0},
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    assert output[-1] == {
        "type": "snapshot",
        "asOf": 4,
        "stations": {
            "A": {"high": 10.0, "low": 5.0},
            "B": {"high": 25.0, "low": 20.0},
        },
    }

def test_large_number_of_stations():
    events = [
        {"type": "sample", "stationName": f"S{i}", "timestamp": i, "temperature": float(i)}
        for i in range(1000)
    ] + [{"type": "control", "command": "snapshot"}]
    output = list(weather.process_events(events))
    # Only check that the snapshot has all stations
    snapshot = output[-1]
    assert snapshot["type"] == "snapshot"
    assert len(snapshot["stations"]) == 1000

def test_snapshot_after_reset_no_new_data():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "control", "command": "reset"},
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    # Only reset should be yielded
    assert len(output) == 2
    assert output[0]["type"] == "sample"
    assert output[1]["type"] == "reset"

def test_one_sample_per_station():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "B", "timestamp": 2, "temperature": 20.0},
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    stations = output[-1]["stations"]
    assert stations["A"]["high"] == stations["A"]["low"] == 10.0
    assert stations["B"]["high"] == stations["B"]["low"] == 20.0

def test_reset_after_reset():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "control", "command": "reset"},
        {"type": "control", "command": "reset"},
    ]
    output = list(weather.process_events(events))
    # Only the first reset should yield
    assert sum(1 for o in output if o["type"] == "reset") == 1

def test_duplicate_timestamp():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 15.0},
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    stations = output[-1]["stations"]
    assert stations["A"]["high"] == 15.0
    assert stations["A"]["low"] == 10.0

def test_negative_and_extreme_temperatures():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": -50.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 150.0},
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    stations = output[-1]["stations"]
    assert stations["A"]["high"] == 150.0
    assert stations["A"]["low"] == -50.0

def test_non_integer_timestamp():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": "not_an_int", "temperature": 10.0},
    ]
    # Should not raise, but will treat as a value for high/low logic
    output = list(weather.process_events(events))
    assert output[0]["timestamp"] == "not_an_int"

def test_sample_with_extra_fields():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0, "humidity": 50},
        {"type": "control", "command": "snapshot"},
    ]
    output = list(weather.process_events(events))
    stations = output[-1]["stations"]
    assert stations["A"]["high"] == 10.0
    assert stations["A"]["low"] == 10.0
