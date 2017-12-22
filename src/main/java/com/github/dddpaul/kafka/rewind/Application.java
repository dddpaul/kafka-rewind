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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.stream.Collectors.toMap;

/**
 * See https://jeqo.github.io/post/2017-01-31-kafka-rewind-consumers-offset/
 * <p>
 * --servers=kafka:9092 --id=group --topic=topic --offset=0:2017-12-21 --offset=1:2017-12-21 --offset=2:2017-12-21
 */
@SpringBootApplication
@Slf4j
public class Application implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
        boolean flag = true;

        Map<String, Object> props = Map.of(
                "bootstrap.servers", args.getOptionValues("servers").get(0),
                "group.id", args.getOptionValues("id").get(0),
                "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"

        );

        String topic = args.getOptionValues("topic").get(0);
        List<String> offsets = args.getOptionValues("offset");

        Map<TopicPartition, Long> query = offsets.stream()
                .map(s -> s.split(":"))
                .collect(toMap(
                        e -> new TopicPartition(topic, Integer.parseInt(e[0])),
                        e -> LocalDate.parse(e[1]).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()));

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<Object, Object> records = consumer.poll(3000);

            if (flag) {
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
