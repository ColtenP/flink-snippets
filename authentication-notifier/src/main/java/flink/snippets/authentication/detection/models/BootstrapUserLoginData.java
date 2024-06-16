package flink.snippets.authentication.detection.models;

import java.util.Set;
import java.util.UUID;

public class BootstrapUserLoginData {
  public final UUID userId;
  public final Set<UUID> locations;
  public final Set<UUID> devices;

  public BootstrapUserLoginData(UUID userId, Set<UUID> locations, Set<UUID> devices) {
    this.userId = userId;
    this.locations = locations;
    this.devices = devices;
  }
}
