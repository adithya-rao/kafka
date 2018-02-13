package com.bnsf.spm.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

public class Receiver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(Receiver.class);

  private CountDownLatch latch = new CountDownLatch(1);

  public CountDownLatch getLatch() {
    return latch;
  }

  @KafkaListener(topics = "${wb2.topic.event.shipmentrequest}")
  public void txReceive(String payload) {
    LOGGER.info("received Tx payload='{}'", payload);
    latch.countDown();
  }
}
