package flink.snippets.authentication.detection.models;

import java.util.UUID;

public class LoginMetadata {
  public final UUID transactionId;
  public final UUID userId;
  public final UUID locationId;
  public final UUID deviceId;
  public final Long eventTimestamp;

  public LoginMetadata(UUID userId, UUID locationId, UUID deviceId) {
    this.transactionId = UUID.randomUUID();
    this.userId = userId;
    this.locationId = locationId;
    this.deviceId = deviceId;
    this.eventTimestamp = System.currentTimeMillis();

    System.out.printf(
        "LoginMetadata(transactionId=%s, userId=%s, locationId=%s, deviceId=%s, eventTimestamp=%s)%n",
        transactionId,
        userId,
        locationId,
        deviceId,
        eventTimestamp
    );
  }
}
