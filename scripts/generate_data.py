import csv
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)

NUM_RECORDS = 50_000
DAYS_OF_HISTORY = 30

ISSUE_RATE = 0.08

CITIES = ["Vancouver", "Toronto", "Montreal", "Ottawa", "Calgary"]
NETWORK_TYPES = ["4G", "5G"]

OUTPUT_PATH = Path("data/raw/telecom_measurements.csv")

PROFILES = {
    "Vancouver": {
        "4G": {"download": (75, 20), "upload": (16, 5), "latency": (38, 10)},
        "5G": {"download": (190, 45), "upload": (35, 10), "latency": (23, 7)},
    },
    "Toronto": {
        "4G": {"download": (85, 22), "upload": (20, 6), "latency": (32, 8)},
        "5G": {"download": (280, 55), "upload": (50, 12), "latency": (14, 4)},
    },
    "Montreal": {
        "4G": {"download": (80, 20), "upload": (18, 5), "latency": (35, 9)},
        "5G": {"download": (210, 45), "upload": (38, 10), "latency": (20, 6)},
    },
    "Ottawa": {
        "4G": {"download": (90, 18), "upload": (22, 5), "latency": (28, 7)},
        "5G": {"download": (180, 35), "upload": (34, 8), "latency": (22, 6)},
    },
    "Calgary": {
        "4G": {"download": (110, 18), "upload": (30, 5), "latency": (18, 4)},
        "5G": {"download": (165, 40), "upload": (25, 8), "latency": (27, 8)},
    },
}

CITY_WEIGHTS = [0.18, 0.28, 0.20, 0.16, 0.18]

NETWORK_WEIGHTS = {
    "Vancouver": [0.40, 0.60],
    "Toronto": [0.30, 0.70],
    "Montreal": [0.45, 0.55],
    "Ottawa": [0.50, 0.50],
    "Calgary": [0.60, 0.40],
}

FIELDS = [
    "id",
    "timestamp",
    "city",
    "network_type",
    "download_speed_mbps",
    "upload_speed_mbps",
    "latency_ms",
]

ISSUE_TYPES = [
    "missing_value",
    "outlier",
    "inconsistent_format",
    "invalid_timestamp",
    "invalid_numeric",
    "future_timestamp",
    "duplicate_id",
]

ISSUE_WEIGHTS = [20, 15, 15, 10, 15, 5, 20]


def positive_normal(mean, std_dev, minimum=0.1):
    return round(max(minimum, random.gauss(mean, std_dev)), 2)


def generate_timestamp(start, end):
    seconds = random.randint(0, int((end - start).total_seconds()))
    timestamp = start + timedelta(seconds=seconds)
    return timestamp.isoformat().replace("+00:00", "Z")


def generate_measurement(index, start, end):
    city = random.choices(CITIES, weights=CITY_WEIGHTS, k=1)[0]
    network = random.choices(
        NETWORK_TYPES,
        weights=NETWORK_WEIGHTS[city],
        k=1,
    )[0]

    profile = PROFILES[city][network]

    return {
        "id": f"M{index:06d}",
        "timestamp": generate_timestamp(start, end),
        "city": city,
        "network_type": network,
        "download_speed_mbps": positive_normal(*profile["download"]),
        "upload_speed_mbps": positive_normal(*profile["upload"]),
        "latency_ms": positive_normal(*profile["latency"]),
    }


def introduce_quality_issue(record, previous_records):
    issue = random.choices(
        ISSUE_TYPES,
        weights=ISSUE_WEIGHTS,
        k=1,
    )[0]

    if issue == "missing_value":
        field = random.choice([
            "city",
            "network_type",
            "download_speed_mbps",
            "upload_speed_mbps",
            "latency_ms",
        ])
        record[field] = ""

    elif issue == "outlier":
        field = random.choice([
            "download_speed_mbps",
            "upload_speed_mbps",
            "latency_ms",
        ])
        record[field] = random.choice([-50, 9999, 2500])

    elif issue == "inconsistent_format":
        if record["city"]:
            record["city"] = random.choice([
                record["city"].lower(),
                f" {record['city']} ",
                record["city"].upper(),
            ])
        else:
            record["network_type"] = "5g"

    elif issue == "invalid_timestamp":
        record["timestamp"] = random.choice([
            "not-a-timestamp",
            "2026-99-45T25:61:00Z",
            "",
        ])

    elif issue == "invalid_numeric":
        field = random.choice([
            "download_speed_mbps",
            "upload_speed_mbps",
            "latency_ms",
        ])
        record[field] = random.choice(["unknown", "N/A", "fast"])

    elif issue == "future_timestamp":
        future = datetime.now(timezone.utc) + timedelta(days=365)
        record["timestamp"] = future.isoformat().replace("+00:00", "Z")

    elif issue == "duplicate_id":
        if previous_records:
            original = random.choice(previous_records)
            record["id"] = original["id"]

    return record, issue


def main():
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=DAYS_OF_HISTORY)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    issue_counts = Counter()
    affected_records = 0

    for index in range(1, NUM_RECORDS + 1):
        record = generate_measurement(index, start, end)

        if random.random() < ISSUE_RATE:
            record, issue = introduce_quality_issue(record, rows)
            issue_counts[issue] += 1
            affected_records += 1

        rows.append(record)

    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows):,} records")

if __name__ == "__main__":
    main()
