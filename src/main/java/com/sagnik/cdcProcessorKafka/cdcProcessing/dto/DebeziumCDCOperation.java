package com.sagnik.cdcProcessorKafka.cdcProcessing.dto;

import com.sagnik.cdcProcessorKafka.cdcProcessing.CDCOperation;

import static com.sagnik.cdcProcessorKafka.cdcProcessing.CDCOperation.*;

public enum DebeziumCDCOperation {
    r(INITIAL_LOAD),
    c(CREATE),
    u(UPDATE),
    d(DELETE);

    private CDCOperation cdcOperation;

    DebeziumCDCOperation(CDCOperation cdcOperation) {
        this.cdcOperation = cdcOperation;
    }

    public CDCOperation getCdcOperation() {
        return cdcOperation;
    }
}
