package com.bnsf.spm.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
@EnableKafka
public class ReceiverConfig {

	@Value("${bootstrap.servers}")
	private String bootstrapServers;

	@Value("${wb2.topic.event.shipmentrequest}")
	private String defaultTopic;

	@Value("${security.protocol}")
	private String securityProtocol;

	@Value("${ssl.truststore.location}")
	private String sslTruststoreLocation;

	@Value("${ssl.truststore.password}")
	private String sslTruststorePassword;

	@Value("${ssl.keystore.location}")
	private String sslKeystoreLocation;

	@Value("${ssl.keystore.password}")
	private String sslKeystorePassword;

	@Value("${ssl.key.password}")
	private String sslKeyPassword;

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		// list of host:port pairs used for establishing the initial connections to the
		// Kafka cluster
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		// allows a pool of processes to divide the work of consuming and processing
		// records
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "tester");
		// automatically reset the offset to the earliest offset
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
		props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
		props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
		props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
		props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
		props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);

		return props;
	}

	@Bean
	public ConsumerFactory<String, String> kafkaConsumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(kafkaConsumerFactory());
		return factory ;
	}


	@Bean
	public Receiver receiver() {
		return new Receiver();
	}
}
