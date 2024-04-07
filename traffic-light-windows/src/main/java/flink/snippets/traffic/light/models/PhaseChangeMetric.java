package flink.snippets.traffic.light.models;

import java.util.UUID;

public class PhaseChangeMetric {
  public final UUID intersectionId;
  public final Long windowStart;
  public final Long windowEnd;
  public final Integer phases;
  public final Integer emergencyPhases;

  public PhaseChangeMetric(UUID intersectionId, Integer phases, Integer emergencyPhases) {
    this.intersectionId = intersectionId;
    this.windowStart = Long.MIN_VALUE;
    this.windowEnd = Long.MAX_VALUE;
    this.phases = phases;
    this.emergencyPhases = emergencyPhases;
  }

  public PhaseChangeMetric(UUID intersectionId, Long windowStart, Long windowEnd, Integer phases, Integer emergencyPhases) {
    this.intersectionId = intersectionId;
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.phases = phases;
    this.emergencyPhases = emergencyPhases;
  }
}
