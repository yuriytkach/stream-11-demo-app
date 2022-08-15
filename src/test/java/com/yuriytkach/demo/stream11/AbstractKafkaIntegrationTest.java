package com.yuriytkach.demo.stream11;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJson;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Testcontainers
@AutoConfigureJson
@Import({ KafkaConfiguration.class, KafkaAutoConfiguration.class })
@ExtendWith(SpringExtension.class)
abstract class AbstractKafkaIntegrationTest {

  static final String TOPIC = "orders-test-topic";

  @Container
  static final KafkaContainer KAFKA = new KafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka").withTag("7.2.1")
  ).withEnv("KAFKA_NUM_PARTITIONS", "2");

  @DynamicPropertySource
  public static void updateConfig(final DynamicPropertyRegistry registry) {
    log.info("Kafka brokers: {}", KAFKA.getBootstrapServers());
    registry.add(
      "spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers
    );
  }

  protected static final ConditionFactory WAIT = await()
    .atMost(Duration.ofSeconds(8))
    .pollInterval(Duration.ofSeconds(1))
    .pollDelay(Duration.ofSeconds(1));

  @Value("${spring.kafka.bootstrap-servers}")
  protected String brokers;

  @Autowired
  protected ObjectMapper objectMapper;

}
