package com.kafka.kafkademo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.kafka.support.KafkaHeaders.TOPIC;


@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka
public class KafkademoApplicationTests {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Test
	public void testProducer() {
		Consumer consumer = runConsumer();

		Producer producer = publishData();

		ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, TOPIC);
		Assert.assertNotNull(singleRecord);
		Assert.assertEquals("my-aggregate-id", singleRecord.key());
		Assert.assertEquals("my-test-value", singleRecord.value());

	}

	public Consumer runConsumer(){
		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("consumer", "false", embeddedKafkaBroker));
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new StringDeserializer()).createConsumer();
		consumer.subscribe(Collections.singleton(TOPIC));
		return consumer;
	}

	public Producer publishData(){
		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
		Producer<String, String> producer = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer()).createProducer();
		producer.send(new ProducerRecord<>(TOPIC, "my-aggregate-id", "my-test-value"));
		return producer;
	}
}
