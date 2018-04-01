package com.github.dddpaul.kafka.rewind;

import com.github.dddpaul.kafka.rewind.consumers.Seeker;
import com.github.dddpaul.kafka.rewind.consumers.SimpleConsumer;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.lang.invoke.MethodHandles;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka simpleConsumer offset rewind tool
 * If timestamp is in future - do nothing
 * See https://jeqo.github.io/post/2017-01-31-kafka-rewind-consumers-offset/
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static CountDownLatch latch;
    private static SimpleConsumer simpleConsumer;

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


        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props)) {
            new Seeker(consumer, topic, offsets, timeout).start();
        }

        if (consume) {
            try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props)) {
                simpleConsumer = new SimpleConsumer(consumer, topic, timeout);
                simpleConsumer.start();
            }
        }

        if (latch != null) {
            latch.countDown();
        }
    }

    public static void main(String[] args) {
        Application app = CommandLine.populateCommand(new Application(), args);
        if (app.help) {
            CommandLine.usage(app, System.err);
            return;
        }

        latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (simpleConsumer != null) {
                simpleConsumer.stop();
            }
            Try.run(latch::await).orElseRun(Throwable::printStackTrace);
        }));
        app.start();
    }
}
