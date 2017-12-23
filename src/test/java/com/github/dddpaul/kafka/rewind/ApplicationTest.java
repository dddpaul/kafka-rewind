package com.github.dddpaul.kafka.rewind;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
class ApplicationTest {

    @Test
    void testSomething(KafkaHelper kafkaHelper) throws ExecutionException, InterruptedException {
        // Convenience methods to produce and consume messages
        kafkaHelper.produceStrings("my-test-topic", "a", "b", "c", "d", "e");
        List<String> result = kafkaHelper.consumeStrings("my-test-topic", 5).get();
        assertThat(result).containsExactlyInAnyOrder("a", "b", "c", "d", "e");

        // or use the built-in producers and consumers
        KafkaProducer<String, String> producer = kafkaHelper.createStringProducer();

        KafkaConsumer<String, String> consumer = kafkaHelper.createStringConsumer();

        // Alternatively, the Zookeeper connection String and the broker port can be retrieved to generate your own config
        String zkConnStr = kafkaHelper.zookeeperConnectionString();
        int brokerPort = kafkaHelper.kafkaPort();
    }
}