package flink.snippets.join.sql.models;

import java.util.UUID;

public class VehicleTelemetry {
    public final UUID vehicleId;
    public final Double speed;
    public final Double latitude;
    public final Double longitude;
    public final Long telemetryTimestamp;

  public VehicleTelemetry(UUID vehicleId, Double speed, Double latitude, Double longitude, Long telemetryTimestamp) {
    this.vehicleId = vehicleId;
    this.speed = speed;
    this.latitude = latitude;
    this.longitude = longitude;
    this.telemetryTimestamp = telemetryTimestamp;
  }
}
