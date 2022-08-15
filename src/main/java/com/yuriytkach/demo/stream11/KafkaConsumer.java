package com.yuriytkach.demo.stream11;

import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumer {

  private final OrderConsumptionService service;

  @KafkaListener(
    id = "orders-listener-1",
    idIsGroup = false,
    groupId = "orders-listener-group",
    topics = "${app.kafka.orders-topic}"
  )
  public void consumeRecord(final ConsumerRecord<String, Order> record) {
    log.info("[1] Received record on {}-{}: {}={}", record.partition(), record.offset(), record.key(), record.value());
    service.processSingle(record.value());
  }

  @KafkaListener(
    id = "orders-listener-2",
    idIsGroup = false,
    groupId = "orders-listener-group",
    topics = "${app.kafka.orders-topic}"
  )
  public void consumeOrder(
    @Payload final Order order,
    @Header(value = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) final String id,
    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
    @Header(KafkaHeaders.OFFSET) final int offset,
    @Header(value = "my-header", required = false) final Integer myHeader
  ) {
    log.info("[2] Received in {}-{} {}={}", partition, offset, id, order);
    service.processSingle(order);
  }

  @KafkaListener(
    id = "orders-listener-batch",
    topics = "${app.kafka.orders-topic}",
    batch = "true",
    containerFactory = "batchConsumerContainerFactory",
    properties = {
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG + "=5"
    }
  )
  public void consumeOrders(
    final ConsumerRecords<String, Order> records
  ) {
    log.info("[BATCH] Received orders: {}", records.count());

    records.forEach(record ->
      log.info(
        "[BATCH] Received in batch on {}-{} {}={}",
        record.partition(),
        record.offset(),
        record.key(),
        record.value()
      )
    );

    final var orders = StreamSupport.stream(records.spliterator(), false)
      .map(ConsumerRecord::value)
      .toList();

    service.processMultiple(orders);
  }

}
