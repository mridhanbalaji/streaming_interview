from typing import Any, Iterable, Generator, Dict, Optional


def _process_sample(
    line: dict[str, Any],
    stations: Dict[str, dict],
    latest_timestamp: Optional[int]
) -> int:
    """
    Update the high/low temperature for a station and the latest timestamp.
    Args:
        line: The incoming sample message.
        stations: The dict tracking high/low per station.
        latest_timestamp: The most recent timestamp seen so far.
    Returns:
        The updated latest timestamp.
    """
    station = line["stationName"]
    temp = line["temperature"]
    ts = line.get("timestamp")
    if ts is None:
        raise ValueError("Sample message missing 'timestamp'")
    latest_timestamp = ts
    # Initialize the station if it doesn't exist and update the high and low
    if station not in stations:
        stations[station] = {"high": temp, "low": temp}
    else:
        stations[station]["high"] = max(stations[station]["high"], temp)
        stations[station]["low"] = min(stations[station]["low"], temp)
    return latest_timestamp

def _make_snapshot(latest_timestamp: int, stations: Dict[str, dict]) -> dict:
    """
    Create a snapshot output dict with the current state.
    Args:
        latest_timestamp: The most recent timestamp seen so far.
        stations: The dict tracking high/low per station.
    Returns:
        A dict representing the snapshot message.
    """
    return {
        "type": "snapshot",
        "asOf": latest_timestamp,
        "stations": stations.copy(),
    }

def _make_reset(latest_timestamp: int) -> dict:
    """
    Create a reset output dict.
    Args:
        latest_timestamp: The most recent timestamp seen so far.
    Returns:
        A dict representing the reset message.
    """
    return {
        "type": "reset",
        "asOf": latest_timestamp,
    }

def _raise_unknown_control(command: str):
    """Raise an error for unknown control commands."""
    raise ValueError(f"Unknown control command: {command}.")

def _raise_unknown_type(msg_type: str):
    """Raise an error for unknown message types."""
    raise ValueError(f"Unknown message type: {msg_type}.")

def process_events(
    events: Iterable[dict[str, Any]]
) -> Generator[dict[str, Any], None, None]:
    """
    Process a stream of weather sample and control messages, yielding output as required.
    Maintains only the necessary state for memory efficiency.
    Args:
        events: An iterable of input messages (dicts).
    Yields:
        Output messages (dicts) as required by control messages.
    """
    stations: dict[str, dict[str, float]] = {}  # Tracks high/low per station
    latest_timestamp: Optional[int] = None  # Tracks the most recent timestamp seen

    for line in events:
        msg_type = line.get("type")
        if msg_type == "sample":
            # Update state with the new sample
            latest_timestamp = _process_sample(line, stations, latest_timestamp)
            yield line
        elif msg_type == "control":
            command = line.get("command")
            if command == "snapshot":
                # Output a snapshot if there is data
                if stations and latest_timestamp is not None:
                    yield _make_snapshot(latest_timestamp, stations)
            elif command == "reset":
                # Output a reset message and clear state if there is data
                if stations and latest_timestamp is not None:
                    yield _make_reset(latest_timestamp)
                    stations.clear()
                    latest_timestamp = None
            else:
                _raise_unknown_control(str(command))
        else:
            _raise_unknown_type(str(msg_type))
