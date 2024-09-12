package flink.snippets.join.sql.models;

import java.util.UUID;

public class VehicleEvent {
    public final UUID vehicleId;
    public final VehicleEventType eventType;
    public final Long eventTimestamp;

  public VehicleEvent(UUID vehicleId, VehicleEventType eventType, Long eventTimestamp) {
    this.vehicleId = vehicleId;
    this.eventType = eventType;
    this.eventTimestamp = eventTimestamp;
  }
}
