package com.sagnik.cdcProcessorKafka.cdcProcessing;

public enum CDCOperation {
    INITIAL_LOAD,
    CREATE,
    UPDATE,
    DELETE;
}
