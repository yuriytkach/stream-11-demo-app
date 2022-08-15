package com.yuriytkach.demo.stream11;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = OrderKafkaPublisher.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(properties = {
  "app.kafka.orders-topic=" + AbstractKafkaIntegrationTest.TOPIC,
})
class KafkaPublisherIntegrationTest extends AbstractKafkaIntegrationTest {

  private Consumer<String, String> ordersConsumer;

  @Autowired
  private OrderKafkaPublisher tested;

  /**
   * Kafka consumer that will consume messages that are published to topic
   */
  @BeforeEach
  void setupKafkaOrdersConsumer() {
    final Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(brokers, "testConsumer", "true");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    final ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

    ordersConsumer = consumerFactory.createConsumer();
    ordersConsumer.subscribe(List.of(TOPIC));
  }

  @AfterEach
  void closeConsumer() {
    if (ordersConsumer != null) {
      ordersConsumer.close();
    }
  }

  @Test
  void shouldSendKafkaMessage() {
    final Order order = new Order(new UUID(1, 1), "name", 42);
    tested.publish(order);

    WAIT.untilAsserted(() -> {
      final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(ordersConsumer);
      assertThat(records.count()).isEqualTo(1);

      final ConsumerRecord<String, String> singleRecord = records.iterator().next();

      assertThat(singleRecord).isNotNull();

      assertThat(singleRecord.key()).isEqualTo(new UUID(1, 1).toString());

      JSONAssert.assertEquals(objectMapper.writeValueAsString(order), singleRecord.value(), JSONCompareMode.STRICT);
    });
  }

}
