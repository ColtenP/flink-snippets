WITH vehicle_event_telemetry (
    SELECT
        ve.vehicleId,
        ve.eventType,
        vt.speed,
        vt.latitude,
        vt.longitude,
        ve.eventTimestamp,
        vt.telemetryTimestamp
    FROM vehicle_events ve, vehicle_telemetry vt
    WHERE
        ve.vehicleId = vt.vehicleId AND
        ve.eventTimestamp BETWEEN vt.telemetryTimestamp - INTERVAL '5' SECOND AND
        vt.telemetryTimestamp
)
SELECT
    *
FROM vehicle_event_telemetry vet
WHERE vet.speed IS NOT NULL