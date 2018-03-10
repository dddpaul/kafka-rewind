package com.github.dddpaul.kafka.rewind;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.configuration2.ConfigurationConverter.getProperties;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
class ApplicationTest {

    private static final String GROUP_ID = "id1";
    private static final String TOPIC = "topic";
    private static final Properties PROPS = getProperties(new MapConfiguration(Map.of(
            "group.id", GROUP_ID
    )));
    private static final String TODAY = ISO_DATE.format(LocalDate.now());
    private static final String TOMORROW = ISO_DATE.format(LocalDate.now().plusDays(1));

    @Test
    void test(KafkaHelper kafkaHelper) throws Exception {
        // given
        kafkaHelper.produceStrings(TOPIC, "a", "b", "c", "d", "e");

        // and consume all
        assertThat(consume(kafkaHelper, 5)).hasSize(5);

        // and ensure there is nothing to consume
        assertThat(consume(kafkaHelper, 1)).isEmpty();

        // and rewind to the start of the day
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=" + TODAY).start();

        // then consume all
        assertThat(consume(kafkaHelper, 5)).hasSize(5);

        // and ensure there is nothing to consume
        assertThat(consume(kafkaHelper, 1)).isEmpty();

        // and rewind to the next day
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=" + TOMORROW).start();

        // then consume nothing
        assertThat(consume(kafkaHelper, 1)).isEmpty();
    }

    private List<ConsumerRecord<String, String>> consume(KafkaHelper kafkaHelper, int amount) {
        try (KafkaConsumer<String, String> consumer = kafkaHelper.createStringConsumer(PROPS)) {
            return kafkaHelper.consume(TOPIC, consumer, amount).get(5, SECONDS);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
}
