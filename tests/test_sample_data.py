import json
from datetime import datetime
from pathlib import Path

SAMPLE_DATA_DIR = Path(__file__).parents[1] / "sample_data"


def load_events() -> dict[str, list[dict]]:
    batches = {}

    for path in sorted(SAMPLE_DATA_DIR.glob("batch_*.json")):
        batches[path.name] = [
            json.loads(line)
            for line in path.read_text().splitlines()
            if line.strip()
        ]

    return batches


def test_expected_batches_and_record_counts() -> None:
    batches = load_events()

    assert list(batches) == [
        "batch_001.json",
        "batch_002.json",
        "batch_003.json",
    ]
    assert [len(events) for events in batches.values()] == [4, 5, 4]


def test_dataset_contains_duplicate_event() -> None:
    events = [
        event
        for batch_events in load_events().values()
        for event in batch_events
    ]
    event_ids = [event["event_id"] for event in events]

    assert len(event_ids) == 13
    assert len(set(event_ids)) == 12
    assert event_ids.count("evt-002") == 2
    

def test_dataset_contains_deliberately_invalid_events() -> None:
    events = [
        event
        for batch_events in load_events().values()
        for event in batch_events
    ]
    events_by_id = {event["event_id"]: event for event in events}

    assert events_by_id["evt-007"]["booking_id"] is None
    assert events_by_id["evt-008"]["passenger_count"] < 0
    assert events_by_id["evt-008"]["amount"] < 0
    assert events_by_id["evt-012"]["event_type"] == "BOOKING_CONFIRMED"


def test_batch_three_contains_late_booking_update() -> None:
    batches = load_events()

    earlier_processed_update = next(
        event
        for event in batches["batch_002.json"]
        if event["event_id"] == "evt-005"
    )
    late_arriving_update = next(
        event
        for event in batches["batch_003.json"]
        if event["event_id"] == "evt-009"
    )

    assert late_arriving_update["booking_id"] == earlier_processed_update["booking_id"]
    assert datetime.fromisoformat(
        late_arriving_update["event_timestamp"]
    ) < datetime.fromisoformat(earlier_processed_update["event_timestamp"])