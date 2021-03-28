package com.sagnik.cdcProcessorKafka.cdcProcessing;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StudentCDCProcessor extends CDCKafkaProcessor {
    private static final String TOPIC = "oltp-olap-sync.public.student";

    public StudentCDCProcessor() {
        super(TOPIC);
    }

    @Override
    protected void processCDCEvent(ChangeEvent cdcRecord) {
        log.info("PROCESSING Student CDC Event: {}", cdcRecord.toString());
    }
}
