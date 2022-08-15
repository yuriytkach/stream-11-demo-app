package com.yuriytkach.demo.stream11;

import java.util.Collection;

import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class OrderConsumptionService {

  public void processSingle(final Order order) {
    log.info("Processing single {}", order);
  }

  public void processMultiple(final Collection<Order> orders) {
    log.info("Processing multiple orders: {} ({})", orders.size(), orders);
  }

}
