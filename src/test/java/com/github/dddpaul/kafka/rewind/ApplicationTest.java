package com.github.dddpaul.kafka.rewind;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.time.LocalDate;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.apache.commons.configuration2.ConfigurationConverter.getProperties;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
class ApplicationTest {

    private static final String GROUP_ID = "id1";
    private static final String TOPIC = "topic";
    private static final Properties PROPS = getProperties(new MapConfiguration(Map.of(
            "group.id", GROUP_ID,
            "enable.auto.commit", "false"
    )));
    private static final String TODAY = ISO_DATE.format(LocalDate.now());
    private static final String TOMORROW = ISO_DATE.format(LocalDate.now().plusDays(1));

    @Test
    void test(KafkaHelper kafkaHelper) throws InterruptedException, ExecutionException {
        // given
        kafkaHelper.produceStrings(TOPIC, "a", "b", "c", "d", "e");

        // and consume all
        assertThat(kafkaHelper.consume(TOPIC, kafkaHelper.createStringConsumer(PROPS), 5).get())
                .hasSize(5);

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(PROPS), TOPIC, 10).call())
                .isEmpty();

        // and rewind to the start of the day
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=" + TODAY).start();

        // then consume all
        assertThat(kafkaHelper.consume(TOPIC, kafkaHelper.createStringConsumer(PROPS), 5).get())
                .hasSize(5);

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(PROPS), TOPIC, 10).call())
                .isEmpty();

        // and rewind to the next day
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=" + TOMORROW).start();

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(PROPS), TOPIC, 10).call())
                .isEmpty();
    }
}
