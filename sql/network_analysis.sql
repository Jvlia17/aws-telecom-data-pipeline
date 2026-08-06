SELECT
    network_type,
    ROUND(avg_download_speed_mbps, 2) AS avg_download_speed_mbps,
    ROUND(avg_upload_speed_mbps, 2) AS avg_upload_speed_mbps,
    ROUND(avg_latency_ms, 2) AS avg_latency_ms,
    measurements
FROM telecom_database.network_summary
ORDER BY avg_download_speed_mbps DESC;