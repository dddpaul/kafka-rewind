package com.github.dddpaul.kafka.rewind;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class TestConsumer<K, V> implements Callable<List<ConsumerRecord<K, V>>> {

    private final KafkaConsumer<K, V> consumer;
    private final String topic;
    private final int recordsToPoll;

    TestConsumer(KafkaConsumer<K, V> consumer, String topic, int recordsToPoll) {
        this.consumer = consumer;
        this.topic = topic;
        this.recordsToPoll = recordsToPoll;
    }

    @Override
    public List<ConsumerRecord<K, V>> call() throws InterruptedException {
        consumer.subscribe(Collections.singletonList(topic));
        List<ConsumerRecord<K, V>> result = new ArrayList<>(recordsToPoll);
        int i = 0;
        try {
            while ((i < recordsToPoll) && (!Thread.currentThread().isInterrupted())) {
                ConsumerRecords<K, V> records = consumer.poll(0);
                i++;
                for (ConsumerRecord<K, V> r : records) {
                    result.add(r);
                    if (i >= recordsToPoll) {
                        break;
                    }
                }
                consumer.commitSync();
                Thread.sleep(100);
            }
            return result;
        } finally {
            consumer.close();
        }
    }
}
