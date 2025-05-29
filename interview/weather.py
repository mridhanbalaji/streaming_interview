from typing import Any, Iterable, Generator


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    # Keep track of the latest timestamp and the high and low for each station as dictonary for mememory and time efficent lookup and storage
    stations = {}
    latest_timestamp = None
    for line in events:
        msg_type = line.get("type")
        if msg_type == "sample":
            station = line["stationName"]
            temp = line["temperature"]
            ts = line["timestamp"]
            
            # Update the latest timestamp
            latest_timestamp = ts
            
            # Initialize the station if it doesn't exist and update the high and low
            if station not in stations:
                stations[station] = {"high": temp, "low": temp}
            else:
                stations[station]["high"] = max(stations[station]["high"], temp)
                stations[station]["low"] = min(stations[station]["low"], temp)

            # For now, yield the sample message for testability
            yield line
        elif msg_type == "control":
            command = line.get("command")
            if command == "snapshot":
                if stations and latest_timestamp is not None:
                    yield {
                        "type": "snapshot",
                        "asOf": latest_timestamp,
                        "stations": stations.copy(),
                    }
            elif command == "reset":
                if stations and latest_timestamp is not None:
                    yield {
                        "type": "reset",
                        "asOf": latest_timestamp,
                    }
                    stations.clear()
                    latest_timestamp = None
            else:
                raise Exception(f"Unknown control command: {command}.")
        else:
            raise Exception(f"Unknown message type: {msg_type}.")
