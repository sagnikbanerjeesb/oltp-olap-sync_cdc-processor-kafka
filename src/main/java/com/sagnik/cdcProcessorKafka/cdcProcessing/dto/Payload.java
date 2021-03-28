package com.sagnik.cdcProcessorKafka.cdcProcessing.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sagnik.cdcProcessorKafka.cdcProcessing.CDCOperation;
import lombok.Data;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Payload {
    private Map<String, Object> before;
    private Map<String, Object> after;
    private DebeziumCDCOperation op;
    @JsonProperty("ts_ms")
    private Long timestamp;

    public CDCOperation cdcOperation() {
        return op.getCdcOperation();
    }
}
