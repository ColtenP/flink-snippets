package flink.snippets.authentication.detection.models;

import java.util.UUID;

public class LoginNotification {
  public final UUID transactionId;
  public final UUID userId;
  public final LoginDetermination determination;
  public final Long eventTimestamp;

  public LoginNotification(UUID transactionId, UUID userId, boolean knownLocation, boolean knownDevice) {
    this.transactionId = transactionId;
    this.userId = userId;
    if (!knownLocation && !knownDevice) {
      this.determination = LoginDetermination.UNKNOWN_LOCATION_AND_DEVICE;
    } else if (knownLocation && !knownDevice) {
      this.determination = LoginDetermination.UNKNOWN_LOCATION;
    } else if (!knownLocation) {
      this.determination = LoginDetermination.UNKNOWN_DEVICE;
    } else {
      this.determination = LoginDetermination.PASSED;
    }
    this.eventTimestamp = System.currentTimeMillis();
  }
}
