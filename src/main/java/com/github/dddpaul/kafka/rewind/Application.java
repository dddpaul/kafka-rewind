package com.github.dddpaul.kafka.rewind;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

/**
 * See https://jeqo.github.io/post/2017-01-31-kafka-rewind-consumers-offset/
 * <p>
 * --servers=kafka:9092 --id=group --topic=topic --offset=0:2017-12-21 --offset=1:2017-12-21 --consume
 */
public class Application {

    private static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Option(names = {"-s", "--servers"}, description = "Comma-delimited list of Kafka brokers")
    String servers = "localhost:9092";

    @Option(names = {"-g", "--group-id"}, description = "Consumer group ID", required = true)
    String groupId;

    @Option(names = {"-t", "--topic"}, description = "Topic name", required = true)
    String topic;

    @Option(names = {"-o", "--offset"}, description = "Partition to offset map", required = true)
    Map<String, LocalDate> offsets;

    @Option(names = {"-c", "--consume"}, description = "Consume after seek")
    boolean consume = false;

    public void start() {
        Map<String, Object> props = Map.of(
                "bootstrap.servers", servers,
                "group.id", groupId,
                "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"
        );

        boolean seek = true;

        Map<TopicPartition, Long> partitionsTimestamps = offsets.entrySet().stream()
                .collect(toMap(
                        e -> new TopicPartition(topic, Integer.valueOf(e.getKey())),
                        e -> e.getValue().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()));

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
        CommandLine.populateCommand(new Application(), args).start();
    }
}
