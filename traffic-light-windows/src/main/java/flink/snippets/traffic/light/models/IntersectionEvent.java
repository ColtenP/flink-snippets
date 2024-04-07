package flink.snippets.traffic.light.models;

import java.util.UUID;

public class IntersectionEvent {
    public final UUID intersectionId;
    public final UUID eventId;
    public final Long eventTimestamp;
    public final Integer phase;

    public IntersectionEvent(UUID intersectionId, UUID eventId, Long eventTimestamp, Integer phase) {
        this.intersectionId = intersectionId;
        this.eventId = eventId;
        this.eventTimestamp = eventTimestamp;
        this.phase = phase;
    }
}
