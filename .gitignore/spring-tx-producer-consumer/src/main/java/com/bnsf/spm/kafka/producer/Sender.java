package com.bnsf.spm.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

public class Sender {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(Sender.class);

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Transactional
  public void send(String topic, String payload) {
	kafkaTemplate.executeInTransaction(t -> {
		LOGGER.info("sending payload='{}' to topic='{}'", payload, topic);
		t.send(topic, payload);
		return null;
	});
  }
}
