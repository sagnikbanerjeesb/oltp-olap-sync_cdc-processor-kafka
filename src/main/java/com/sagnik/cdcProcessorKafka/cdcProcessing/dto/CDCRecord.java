package com.sagnik.cdcProcessorKafka.cdcProcessing.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.sagnik.cdcProcessorKafka.cdcProcessing.ChangeEvent;
import lombok.Data;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class CDCRecord {
    private Payload payload;

    public ChangeEvent toChangeEvent() {
        return ChangeEvent.builder()
                .operation(payload.cdcOperation())
                .before(Optional.ofNullable(payload.getBefore()).orElse(Map.of()))
                .after(Optional.ofNullable(payload.getAfter()).orElse(Map.of()))
                .timestamp(Instant.ofEpochMilli(payload.getTimestamp()))
                .build();
    }
}
