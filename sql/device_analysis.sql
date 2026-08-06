SELECT
    device_model,
    ROUND(avg_download_speed_mbps, 2) AS download_speed,
    ROUND(avg_latency_ms, 2) AS latency,
    measurements
FROM telecom_database.device_summary
WHERE measurements >= 10
ORDER BY download_speed DESC
LIMIT 10;