package com.github.dddpaul.kafka.rewind.core;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;

public class SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private volatile boolean isRunning = true;

    private KafkaConsumer<Object, Object> consumer;
    private String topic;
    private long timeout;

    public SimpleConsumer(KafkaConsumer<Object, Object> consumer, String topic, long timeout) {
        this.consumer = consumer;
        this.topic = topic;
        this.timeout = timeout;
    }

    public void start() {
        consumer.subscribe(Collections.singletonList(topic));

        while (isRunning && !Thread.interrupted()) {
            ConsumerRecords<Object, Object> records = consumer.poll(timeout);
            records.forEach(r -> {
                LocalDateTime timestamp = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(r.timestamp()),
                        ZoneId.systemDefault()
                );
                System.out.println(String.format("Timestamp: %s, partition: %s, value: %s",
                        timestamp, new TopicPartition(topic, r.partition()), r.value()));
            });
            consumer.commitSync();
        }
    }

    public void stop() {
        isRunning = false;
    }
}
