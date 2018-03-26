package com.github.dddpaul.kafka.rewind;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitRule;
import com.github.charithe.kafka.StartupMode;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Rule;
import org.junit.Test;
import picocli.CommandLine;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.apache.commons.configuration2.ConfigurationConverter.getProperties;
import static org.assertj.core.api.Assertions.assertThat;

public class ApplicationTest {

    private static final String GROUP_ID = "id1";
    private static final String TOPIC = "topic";
    private static final Properties PROPS = getProperties(new MapConfiguration(Map.of(
            "group.id", GROUP_ID,
            "enable.auto.commit", "false"
    )));
    private static final String TODAY = ISO_DATE.format(LocalDate.now());
    private static final String TOMORROW = ISO_DATE.format(LocalDate.now().plusDays(1));

    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create(), StartupMode.WAIT_FOR_STARTUP);

    @Test
    public void test() throws InterruptedException, ExecutionException {
        KafkaHelper kafkaHelper = kafkaRule.helper();

        // given
        kafkaHelper.produceStrings(TOPIC, "a", "b", "c", "d", "e");

        // and consume all
        assertThat(consume(kafkaHelper, 5)).hasSize(5);

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(PROPS), TOPIC, 10).call()).isEmpty();

        // and rewind to the start of the day
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=" + TODAY).start();

        // then consume all
        assertThat(consume(kafkaHelper, 5)).hasSize(5);

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(PROPS), TOPIC, 10).call()).isEmpty();

        // and rewind to the next day
        CommandLine.populateCommand(new Application(),
                "--servers=localhost:" + kafkaHelper.kafkaPort(),
                "--group-id=" + GROUP_ID,
                "--topic=" + TOPIC,
                "--offset=0=" + TOMORROW).start();

        // and ensure there is nothing to consume
        assertThat(new TestConsumer<>(kafkaHelper.createStringConsumer(PROPS), TOPIC, 10).call()).isEmpty();
    }

    private List<ConsumerRecord<String, String>> consume(KafkaHelper kafkaHelper, int amount) throws ExecutionException, InterruptedException {
        KafkaConsumer<String, String> consumer = kafkaHelper.createStringConsumer(PROPS);
        return kafkaHelper.consume(TOPIC, consumer, amount).get();
    }
}
