package com.github.dddpaul.kafka.rewind;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.MINUTES;

/**
 * See https://jeqo.github.io/post/2017-01-31-kafka-rewind-consumers-offset/
 */
@SpringBootApplication
@Slf4j
public class Application implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("group.id", "group2");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        boolean flag = true;

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("topic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(3000);

            if (flag) {
                Map<TopicPartition, Long> query = new HashMap<>();
                query.put(
                        new TopicPartition("topic", 0),
                        Instant.now().minus(9, MINUTES).toEpochMilli());

                Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);
                result.forEach((key, value) -> consumer.seek(key, value.offset()));
                flag = false;
            }

            records.forEach(r -> {
                LocalDateTime timestamp = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(r.timestamp()),
                        ZoneId.systemDefault()
                );
                log.info("Timestamp: {}, record: {}", timestamp, r);
            });
        }
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(Application.class)
                .web(false)
                .run(args);
    }
}
