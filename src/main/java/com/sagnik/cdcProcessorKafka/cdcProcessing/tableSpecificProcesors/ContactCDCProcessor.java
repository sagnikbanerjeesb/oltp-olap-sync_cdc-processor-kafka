package com.sagnik.cdcProcessorKafka.cdcProcessing.tableSpecificProcesors;

import com.sagnik.cdcProcessorKafka.cdcProcessing.CDCKafkaProcessor;
import com.sagnik.cdcProcessorKafka.cdcProcessing.ChangeEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContactCDCProcessor extends CDCKafkaProcessor {
    private static final String TOPIC = "oltp-olap-sync.public.contact";

    public ContactCDCProcessor() {
        super(TOPIC);
    }

    @Override
    protected void processCDCEvent(ChangeEvent cdcRecord) {
        log.info("PROCESSING Contact CDC Event: {}", cdcRecord.toString());
    }
}
