package com.github.dddpaul.kafka.rewind;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

/**
 * See https://jeqo.github.io/post/2017-01-31-kafka-rewind-consumers-offset/
 * <p>
 * --servers=kafka:9092 --id=group --topic=topic --offset=0:2017-12-21 --offset=1:2017-12-21 --consume
 */
@SpringBootApplication
public class Application implements ApplicationRunner {

    private static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void run(ApplicationArguments args) {
        Map<String, Object> props = Map.of(
                "bootstrap.servers", args.getOptionValues("servers").get(0),
                "group.id", args.getOptionValues("id").get(0),
                "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"
        );

        String topic = args.getOptionValues("topic").get(0);
        List<String> offsets = args.getOptionValues("offset");
        boolean consume = args.containsOption("consume");
        boolean seek = true;

        Map<TopicPartition, Long> partitionsTimestamps = offsets.stream()
                .map(s -> s.split(":"))
                .collect(toMap(
                        e -> new TopicPartition(topic, Integer.parseInt(e[0])),
                        e -> LocalDate.parse(e[1]).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()));

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<Object, Object> records = consumer.poll(3000);

            if (seek) {
                Map<TopicPartition, OffsetAndTimestamp> partitionsOffsets = consumer.offsetsForTimes(partitionsTimestamps);
                partitionsOffsets.forEach((key, value) -> consumer.seek(key, value.offset()));
                seek = false;
                log.info("Seek to {}", partitionsOffsets);
            }

            if (!consume) {
                consumer.commitSync();
                break;
            }

            records.forEach(r -> {
                LocalDateTime timestamp = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(r.timestamp()),
                        ZoneId.systemDefault()
                );
                System.out.println(String.format("Timestamp: %s, partition: %s, value: %s",
                        timestamp, new TopicPartition(topic, r.partition()), r.value()));
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
