package com.sagnik.cdcProcessorKafka.cdcProcessing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagnik.cdcProcessorKafka.cdcProcessing.dto.CDCRecord;
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
    private final AtomicBoolean continueProcessing;
    private final ObjectMapper objectMapper;

    public CDCKafkaProcessor(String topic) {
        this.topic = topic;
        this.continueProcessing = new AtomicBoolean(true);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void stop() {
        continueProcessing.set(false);
    }

    @Override
    public void run() {
        try (KafkaConsumer<String, String> consumer = constructKafkaConsumer()){
            while (continueProcessing.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5L)); // FIXME hard coded poll interval
                for (ConsumerRecord<String, String> record : records) {
                    log.debug("RECEIVED: offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                    processCDCEvent(transformKafkaRecordToDomain(record));
                }

                log.debug("Committing offsets");
                consumer.commitSync();
            }

            log.info("Aborting");
        } catch (Exception e) {
            log.error("Exception while processing kafka event", e);
        }
    }

    private ChangeEvent transformKafkaRecordToDomain(ConsumerRecord<String, String> record) throws com.fasterxml.jackson.core.JsonProcessingException {
        return objectMapper.readValue(record.value(), CDCRecord.class).toChangeEvent();
    }

    private KafkaConsumer<String, String> constructKafkaConsumer() {
        Properties props = constructKafkaConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        subscribeToTopic(consumer);

        return consumer;
    }

    private Properties constructKafkaConsumerProps() {
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
        return props;
    }

    private void subscribeToTopic(KafkaConsumer<String, String> consumer) {
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
    }

    abstract protected void processCDCEvent(ChangeEvent cdcRecord); // will evolve
}
