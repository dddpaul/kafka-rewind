package com.github.dddpaul.kafka.rewind;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.util.List;
import java.util.Map;
import java.util.Properties;

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

    @Test
    void test(KafkaHelper kafkaHelper) throws Exception {
        // given
        kafkaHelper.produceStrings(TOPIC, "a", "b", "c", "d", "e");

        // and
        List<ConsumerRecord<String, String>> records =
                kafkaHelper.consume(TOPIC, kafkaHelper.createStringConsumer(PROPS), 5).get();
        assertThat(records).hasSize(5);

        // and
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=2017-12-01").start();

        // then with the same group id
        records = kafkaHelper.consume(TOPIC, kafkaHelper.createStringConsumer(PROPS), 5).get();
        assertThat(records).hasSize(5);

        // and
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=2017-12-30").start();

        // then with the same group id
        records = kafkaHelper.consume(TOPIC, kafkaHelper.createStringConsumer(PROPS), 5).get();
        assertThat(records).isEmpty();
    }
}
