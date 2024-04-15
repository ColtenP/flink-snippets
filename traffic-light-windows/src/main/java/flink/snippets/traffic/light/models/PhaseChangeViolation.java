package flink.snippets.traffic.light.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.UUID;

public class PhaseChangeViolation {
  public final UUID intersectionId;
  public final UUID fromEventId;
  public final UUID toEventId;
  public final Integer fromPhase;
  public final Integer toPhase;
  public final Long eventTimestamp;

  public PhaseChangeViolation(TrafficLightPhaseEvent fromEvent, TrafficLightPhaseEvent toEvent) {
    this.intersectionId = fromEvent.intersectionId;
    this.fromEventId = fromEvent.eventId;
    this.toEventId = toEvent.eventId;
    this.fromPhase = fromEvent.phase;
    this.toPhase = toEvent.phase;
    this.eventTimestamp = toEvent.eventTimestamp;
  }

  @Override
  public String toString() {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
