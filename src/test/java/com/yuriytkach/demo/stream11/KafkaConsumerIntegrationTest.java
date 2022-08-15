package com.yuriytkach.demo.stream11;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest(classes = KafkaConsumer.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(properties = {
  "app.kafka.orders-topic=" + AbstractKafkaIntegrationTest.TOPIC,

  // Needed, because in test we can publish before consumers in KafkaConsumer are ready to read messages.
  // With this setting, they will read from the beginning in the topic
  "spring.kafka.consumer.auto-offset-reset=earliest"
})
class KafkaConsumerIntegrationTest extends AbstractKafkaIntegrationTest {

  static final String TOPIC_DLT = TOPIC + ".DLT";

  @MockBean
  private OrderConsumptionService consumptionService;

  private KafkaTemplate<String, Object> producerTemplate;

  private Consumer<String, String> dltConsumer;

  /**
   * Create Kafka producer that will publish to topic
   */
  @BeforeEach
  void setupKafkaProducerTemplate() {
    final Map<String, Object> producerProperties = KafkaTestUtils.producerProps(brokers);

    producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-for-" + TOPIC);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    final ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(
      producerProperties,
      new StringSerializer(),
      new JsonSerializer<>()
    );

    producerTemplate = new KafkaTemplate<>(producerFactory);
    producerTemplate.setDefaultTopic(TOPIC);
  }

  /**
   * Create Kafka consumer for {@link #TOPIC_DLT} and make it consumer messages from it with auto acknowledge
   */
  @BeforeEach
  void setupKafkaDltConsumer() {
    final Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(brokers, "dltConsumer", "true");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    final ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

    dltConsumer = consumerFactory.createConsumer();
    dltConsumer.subscribe(List.of(TOPIC_DLT));
  }

  /**
   * Closing Kafka consumer, so the one that will be created in for next test will be able to read new messages.
   */
  @AfterEach
  void closeConsumer() {
    if (dltConsumer != null) {
      dltConsumer.close();
    }
  }

  @Test
  @SneakyThrows
  void shouldConsumeSingleOrder() {
    final Order order = new Order(new UUID(1, 1), "item", 42);

    final var future = producerTemplate.send(TOPIC, new UUID(1, 1).toString(), order);
    future.addCallback(
      record -> log.info("Sent to {}: {}", record.getProducerRecord().topic(), record.getProducerRecord().value()),
      ex -> log.error("Failed to send", ex)
    );

    WAIT.untilAsserted(() ->  {
      assertThat(future.isDone()).isTrue();

      verify(consumptionService).processSingle(order);
      verify(consumptionService).processMultiple(List.of(order));
    });
  }

  @Test
  @SneakyThrows
  void shouldConsumeMultipleOrdersInBatch() {
    IntStream.range(0, 7)
      .mapToObj(i -> new Order(UUID.randomUUID(), "item", i))
      .forEach(order -> producerTemplate.send(TOPIC, order.id().toString(), order));

    // need to wait a bit longer, as batch consumer has 5sec delay
    WAIT.atMost(Duration.ofSeconds(12)).untilAsserted(() ->  {
      verify(consumptionService, times(7)).processSingle(any());
      verify(consumptionService, times(2)).processMultiple(any());
    });
  }

  @Test
  @SneakyThrows
  void shouldRetryBatchIfException() {
    doThrow(IllegalStateException.class)  // first throw exception
      .doNothing()                        // then do nothing
      .when(consumptionService).processMultiple(any());

    IntStream.range(0, 2)
      .mapToObj(i -> new Order(UUID.randomUUID(), "item", i))
      .forEach(order -> producerTemplate.send(TOPIC, order.id().toString(), order));

    // need to wait a bit longer, as batch consumer has 5sec delay
    WAIT.untilAsserted(() ->
      // verify that method is called 2 times - one with full batch, another one with full batch during retry
      verify(consumptionService, times(2)).processMultiple(argThat(list -> {
        assertThat(list).hasSize(2);
        return true;
      })));
  }

  @Test
  @SneakyThrows
  void shouldNotRetryBatchIfIgnoredException() {
    doThrow(IllegalArgumentException.class).when(consumptionService).processMultiple(any());

    IntStream.range(0, 2)
      .mapToObj(i -> new Order(UUID.randomUUID(), "item", i))
      .forEach(order -> producerTemplate.send(TOPIC, order.id().toString(), order));

    // need to wait a bit longer, as batch consumer has 5sec delay
    WAIT.untilAsserted(() ->
      verify(consumptionService, atMostOnce()).processMultiple(argThat(list -> {
        assertThat(list).hasSize(2);
        return true;
      })));
  }

  @Test
  @SneakyThrows
  void shouldRetryBatchIfExceptionAndSendToDltIfAllAttemptsFail() {
    doThrow(IllegalStateException.class).when(consumptionService).processMultiple(any());

    final List<Order> orders = IntStream.range(0, 2)
      .mapToObj(i -> new Order(UUID.randomUUID(), "item", i))
      .toList();

    orders.forEach(order -> producerTemplate.send(TOPIC, order.id().toString(), order));

    // need to wait a bit longer, as batch consumer has 5sec delay
    WAIT.atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
      // verify method is called 4 times - one when records were received, and three times during retries
      verify(consumptionService, times(4)).processMultiple(argThat(list -> {
        assertThat(list).hasSize(2);
        return true;
      }));

      final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(dltConsumer);
      assertThat(records.count()).isEqualTo(2);

      assertThat(StreamSupport.stream(records.spliterator(), false).map(ConsumerRecord::key))
        .containsExactlyInAnyOrderElementsOf(orders.stream().map(Order::id).map(UUID::toString).toList());
    });
  }
}
