package com.bnsf.spm.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.bnsf.spm.kafka.producer.Sender;

@SpringBootApplication
public class SpringKafkaApplication {

  public static void main(String[] args) throws InterruptedException {
	  ConfigurableApplicationContext ctx = SpringApplication.run(SpringKafkaApplication.class, args);
	  
	  Sender sender = ctx.getBean(Sender.class);
	  
	  sender.send("com.bnsf.wb2.event.shipmentrequest", "test");
	  
	  //Receiver receiver = ctx.getBean(Receiver.class);
  }
}
