import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random


fake = Faker()


def generate_data(num_records=10000):
    """
    Generate synthetic telecom network measurements.
    """

    cities = {
        "Calgary": (51.0447, -114.0719),
        "Toronto": (43.6532, -79.3832),
        "Vancouver": (49.2827, -123.1207)
    }

    devices = [
        "iPhone 15",
        "Samsung Galaxy S25",
        "Google Pixel 9",
        "OnePlus 13"
    ]

    network_types = [
        "4G",
        "5G"
    ]


    data = []

    for i in range(num_records):

        city = random.choice(list(cities.keys()))

        latitude, longitude = cities[city]


        record = {
            "measurement_id": i + 1,

            "timestamp": fake.date_time_between(
                start_date="-30d",
                end_date="now"
            ),

            "user_id": f"USER_{random.randint(1, 5000)}",

            "device_model": random.choice(devices),

            "cell_tower_id":
                f"TOWER_{random.randint(1, 200)}",

            "city": city,

            "latitude": latitude + np.random.normal(0, 0.05),

            "longitude": longitude + np.random.normal(0, 0.05),

            "network_type": random.choice(network_types),

            "download_speed_mbps": round(
                np.random.normal(80, 30), 2
            ),

            "upload_speed_mbps": round(
                np.random.normal(15, 5), 2
            ),

            "latency_ms": round(
                np.random.normal(40, 15), 2
            ),

            "signal_strength_dbm": round(
                np.random.normal(-75, 10), 2
            )
        }

        data.append(record)


    return pd.DataFrame(data)



if __name__ == "__main__":

    df = generate_data(10000)

    output_path = (
        "../data/raw/network_measurements.csv"
    )

    df.to_csv(
        output_path,
        index=False
    )

    print("Data generated successfully!")
    print(df.head())