package com.yuriytkach.demo.stream11;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderKafkaPublisher {

  @Value("${app.kafka.orders-topic}")
  private String topic;

  private final KafkaTemplate<String, Order> orderKafkaTemplate;

  public void publish(final Order order) {

    final ListenableFuture<SendResult<String, Order>> send = orderKafkaTemplate.send(
      topic,
      order.id().toString(),
      order
    );

    send.addCallback(
      result -> {
        final ProducerRecord<String, Order> producerRecord = result.getProducerRecord();
        final RecordMetadata recordMetadata = result.getRecordMetadata();
        log.info("SEND to {}={}", recordMetadata.partition(), recordMetadata.offset());
      },
      ex -> {
        log.error("Error in sending: {}", ex.getMessage(), ex);
      }
    );

    final CompletableFuture<SendResult<String, Order>> completable = send.completable();
    completable.join();

    // do othre thinkgs
  }

}
