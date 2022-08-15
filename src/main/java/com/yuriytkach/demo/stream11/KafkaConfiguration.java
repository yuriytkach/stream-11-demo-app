package com.yuriytkach.demo.stream11;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@EnableKafka
@Configuration
public class KafkaConfiguration {

  @Bean
  KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Order>> batchConsumerContainerFactory(
    final ConsumerFactory<String, Order> consumerFactory,
    final KafkaOperations<?, ?> kafkaOperations
  ) {
    final ConcurrentKafkaListenerContainerFactory<String, Order> factory =
      new ConcurrentKafkaListenerContainerFactory<>();

    factory.setConsumerFactory(consumerFactory);

    factory.getContainerProperties().setIdleBetweenPolls(5000);
    factory.getContainerProperties().setPollTimeout(10000);

    final DefaultErrorHandler commonErrorHandler = new DefaultErrorHandler(
      new DeadLetterPublishingRecoverer(kafkaOperations),
      new FixedBackOff(1000, 3)
    );
    commonErrorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
    factory.setCommonErrorHandler(commonErrorHandler);

    return factory;
  }

}
