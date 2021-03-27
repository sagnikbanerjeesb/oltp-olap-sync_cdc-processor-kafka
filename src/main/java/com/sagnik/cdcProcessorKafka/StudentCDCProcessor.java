package com.sagnik.cdcProcessorKafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class StudentCDCProcessor implements Runnable {
    private final String topic = "oltp-olap-sync.public.student";
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean continueProcessing = new AtomicBoolean(true);

    public StudentCDCProcessor() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "cdc-processor");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(props);
    }

    public void stop() {
        continueProcessing.set(false);
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                final Optional<String> partitionsString = partitions.stream().map(partition -> "" + partition.partition()).reduce((a, b) -> a + ", " + b);
                log.info("Partitions revoked: [{}]", partitionsString.orElse(""));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                final Optional<String> partitionsString = partitions.stream().map(partition -> "" + partition.partition()).reduce((a, b) -> a + ", " + b);
                log.info("Partitions assigned: [{}]", partitionsString.orElse(""));

                final Map<TopicPartition, OffsetAndMetadata> committedOffset = consumer.committed(new HashSet<>(partitions));
                committedOffset.forEach((partition, metadata) -> {
                    long offset;
                    if (metadata == null) {
                        offset = 0L;
                    } else {
                        offset = metadata.offset();
                    }
                    consumer.seek(partition, offset);
                });
            }
        });

        while (continueProcessing.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5L));
            for (ConsumerRecord<String, String> record : records) {
                log.info("RECEIVED: offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
            }

            consumer.commitSync();
        }

        log.info("Aborting");

        consumer.close();
    }
}
