package com.sagnik.cdcProcessorKafka.cdcProcessing;

import com.sagnik.cdcProcessorKafka.gracefulShutdown.Stoppable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class CDCKafkaProcessor implements Runnable, Stoppable {
    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean continueProcessing;

    public CDCKafkaProcessor(String topic) {
        this.topic = topic;
        this.continueProcessing = new AtomicBoolean(true);
        this.consumer = constructKafkaConsumer();
    }

    private KafkaConsumer constructKafkaConsumer() {
        // TODO externalise properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "cdc-processor");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer(props);
    }

    @Override
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
                log.debug("RECEIVED: offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                processCDCEvent(record.value());
            }

            log.debug("Committing offsets");
            consumer.commitSync();
        }

        log.info("Aborting");

        consumer.close();
    }

    abstract protected void processCDCEvent(String cdcRecord); // will evolve
}
