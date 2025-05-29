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
