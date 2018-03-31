package com.github.dddpaul.kafka.rewind;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class Seeker {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private KafkaConsumer<Object, Object> consumer;
    private String topic;
    private Map<String, LocalDate> offsets;
    private long timeout;

    public Seeker(KafkaConsumer<Object, Object> consumer, String topic, Map<String, LocalDate> offsets, long timeout) {
        this.consumer = consumer;
        this.topic = topic;
        this.offsets = offsets;
        this.timeout = timeout;
    }

    public void start() {
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(timeout);

        Map<TopicPartition, Long> partitionsTimestamps = offsets.entrySet().stream()
                .collect(toMap(
                        e -> new TopicPartition(topic, Integer.valueOf(e.getKey())),
                        e -> e.getValue()
                                .atStartOfDay(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli()));

        Map<TopicPartition, OffsetAndTimestamp> offsetsAndTimestamps = consumer.offsetsForTimes(partitionsTimestamps);
        offsetsAndTimestamps.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .forEach(e -> consumer.seek(e.getKey(), e.getValue().offset()));

        log.info("Seek to {}", offsetsAndTimestamps);
        consumer.commitSync();
    }
}
