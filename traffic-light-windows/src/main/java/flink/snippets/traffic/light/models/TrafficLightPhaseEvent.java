package flink.snippets.traffic.light.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.UUID;

public class TrafficLightPhaseEvent {
    public final UUID intersectionId;
    public final UUID eventId;
    public final Long eventTimestamp;
    public final Integer phase;

    public TrafficLightPhaseEvent(UUID intersectionId, UUID eventId, Long eventTimestamp, Integer phase) {
        this.intersectionId = intersectionId;
        this.eventId = eventId;
        this.eventTimestamp = eventTimestamp;
        this.phase = phase;
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
