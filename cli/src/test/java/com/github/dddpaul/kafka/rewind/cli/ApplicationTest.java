package com.github.dddpaul.kafka.rewind.cli;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.apache.commons.configuration2.ConfigurationConverter.getProperties;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
class ApplicationTest {

    private static final String GROUP_ID = "id1";
    private static final String TOPIC = "topic";
    private static final Properties PRODUCER_PROPS = getProperties(new MapConfiguration(Map.of(
            "compression.type", "snappy"
    )));
    private static final Properties CONSUMER_PROPS = getProperties(new MapConfiguration(Map.of(
            "group.id", GROUP_ID,
            "enable.auto.commit", "false"
    )));
    private static final String TODAY = ISO_DATE.format(LocalDate.now());
    private static final String TOMORROW = ISO_DATE.format(LocalDate.now().plusDays(1));

    @Test
    void test(KafkaHelper kafkaHelper) throws Exception {
        // given
        KafkaProducer<String, String> producer = kafkaHelper.createStringProducer(PRODUCER_PROPS);
        Map<String, String> data = Stream.of("a", "b", "c", "d", "e")
                .collect(Collectors.toMap(k -> String.valueOf(k.hashCode()), Function.identity()));
        kafkaHelper.produce(TOPIC, producer, data);

        // and consume all
        assertThat(consume(kafkaHelper, 5)).hasSize(5);

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(CONSUMER_PROPS), TOPIC, 10).call()).isEmpty();

        // and rewind to the start of the day
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=" + TODAY).start();

        // then consume all
        assertThat(consume(kafkaHelper, 5)).hasSize(5);

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(CONSUMER_PROPS), TOPIC, 10).call()).isEmpty();

        // and rewind to the next day
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=" + TOMORROW).start();

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(CONSUMER_PROPS), TOPIC, 10).call()).isEmpty();
    }

    private List<ConsumerRecord<String, String>> consume(KafkaHelper kafkaHelper, int amount) throws Exception {
        return kafkaHelper.consume(TOPIC, kafkaHelper.createStringConsumer(CONSUMER_PROPS), amount).get();
    }

}
