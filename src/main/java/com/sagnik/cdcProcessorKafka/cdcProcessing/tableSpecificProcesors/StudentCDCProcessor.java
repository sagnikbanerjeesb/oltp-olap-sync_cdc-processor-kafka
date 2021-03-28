package com.sagnik.cdcProcessorKafka.cdcProcessing.tableSpecificProcesors;

import com.sagnik.cdcProcessorKafka.cdcProcessing.CDCKafkaProcessor;
import com.sagnik.cdcProcessorKafka.cdcProcessing.ChangeEvent;
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
