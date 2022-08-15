package com.yuriytkach.demo.stream11;

import java.util.UUID;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class OrderController {

  private final OrderKafkaPublisher publisher;

  @PostMapping("/orders")
  public String send(
    @RequestBody  final OrderRequest requests
  ) {
    final Order order = new Order(UUID.randomUUID(), requests.item(), requests.userId());

    publisher.publish(order);

    return "Sent " + order.id();
  }

  public record OrderRequest(String item, Integer userId) {}
}
