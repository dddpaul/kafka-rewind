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
import java.util.concurrent.CountDownLatch;

import static java.util.stream.Collectors.toMap;

/**
 * Kafka consumer offset rewind tool
 * If timestamp is in future - do nothing
 * See https://jeqo.github.io/post/2017-01-31-kafka-rewind-consumers-offset/
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static volatile boolean isRunning = true;
    private static final CountDownLatch latch = new CountDownLatch(1);

    @Option(names = {"-s", "--servers"}, description = "Comma-delimited list of Kafka brokers")
    private String servers = "localhost:9092";

    @Option(names = {"-g", "--group-id"}, description = "Consumer group ID", required = true)
    private String groupId;

    @Option(names = {"-t", "--topic"}, description = "Topic name", required = true)
    private String topic;

    @Option(names = {"-o", "--offset"}, description = "Partition to timestamp map", required = true)
    private Map<String, LocalDate> offsets;

    @Option(names = {"-c", "--consume"}, description = "Consume after seek")
    private boolean consume = false;

    @Option(names = {"-k", "--key-deserializer"}, description = "Consumer key deserializer")
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    @Option(names = {"-v", "--value-deserializer"}, description = "Consumer value deserializer")
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    @Option(names = {"-p", "--poll-timeout"}, description = "Consumer poll timeout, ms")
    private long timeout = 3000;

    @Option(names = {"-h", "--help"}, description = "Display a help message", usageHelp = true)
    private boolean help = false;

    public void start() {
        Map<String, Object> props = Map.of(
                "bootstrap.servers", servers,
                "group.id", groupId,
                "key.deserializer", keyDeserializer,
                "value.deserializer", valueDeserializer,
                "auto.offset.reset", "earliest",
                "enable.auto.commit", "false"
        );

        boolean seek = true;

        Map<TopicPartition, Long> partitionsTimestamps = offsets.entrySet().stream()
                .collect(toMap(
                        e -> new TopicPartition(topic, Integer.valueOf(e.getKey())),
                        e -> e.getValue()
                                .atStartOfDay(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli()));

        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (isRunning && !Thread.interrupted()) {
                ConsumerRecords<Object, Object> records = consumer.poll(timeout);

                if (seek) {
                    Map<TopicPartition, OffsetAndTimestamp> partitionsOffsets = consumer.offsetsForTimes(partitionsTimestamps);
                    partitionsOffsets.entrySet().stream()
                            .filter(e -> e.getValue() != null)
                            .forEach(e -> consumer.seek(e.getKey(), e.getValue().offset()));
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

                consumer.commitSync();
            }
        }
        latch.countDown();
    }

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false;
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        Application app = CommandLine.populateCommand(new Application(), args);
        if (app.help) {
            CommandLine.usage(app, System.err);
            return;
        }
        app.start();
    }
}
