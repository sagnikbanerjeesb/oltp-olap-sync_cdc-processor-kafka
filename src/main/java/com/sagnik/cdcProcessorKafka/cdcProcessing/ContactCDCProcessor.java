package com.sagnik.cdcProcessorKafka.cdcProcessing;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContactCDCProcessor extends CDCKafkaProcessor {
    private static final String TOPIC = "oltp-olap-sync.public.contact";

    public ContactCDCProcessor() {
        super(TOPIC);
    }

    @Override
    protected void processCDCEvent(String cdcRecord) {
        log.info("PROCESSING Contact CDC Event: {}", cdcRecord);
    }
}
