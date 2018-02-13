package com.bnsf.spm.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

@Configuration
public class SenderConfig {

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
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();

		setupBootstrapAndSerializers(props);
		setupBatchingAndCompression(props);
		setupRetriesInFlightTimeout(props);
		setupSecurity(props);
		// Set number of acknowledgments - acks - default is all
		props.put(ProducerConfig.ACKS_CONFIG, "all");

		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my.tx");

		return props;
	}

	private void setupSecurity(Map<String, Object> props) {
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
		props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
		props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
		props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
		props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
		props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);		
	}

	private void setupBootstrapAndSerializers(Map<String, Object> props) {
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "SPM_SPRING_TX");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	}

	private void setupBatchingAndCompression(Map<String, Object> props) {
		// disable batchingfor test
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);

		// Linger up to 5 ms before sending batch if size not met
		// props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
		// props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384);

		// We won't use compression, but if needed can use Snappy compression for batch
		// compression.
		// props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

	}

	private void setupRetriesInFlightTimeout(Map<String, Object> props) {
		// Only two in-flight messages per Kafka broker connection
		// - max.in.flight.requests.per.connection (default 5)
		// props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
		// Set the number of retries - retries
		// props.put(ProducerConfig.RETRIES_CONFIG, 3);

		// Request timeout - request.timeout.ms
		// props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);

		// Only retry after one second.
		// props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerConfigs());
		pf.setTransactionIdPrefix("my.transaction");
		return pf;
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory(), true);
		kafkaTemplate.setDefaultTopic(defaultTopic);
		return kafkaTemplate;
	}
	
	@Bean
	public KafkaTransactionManager<String, String> kafkaTransactionManager() {
		return new KafkaTransactionManager<String, String>(producerFactory());
	}

	@Bean
	public Sender sender() {
		return new Sender();
	}
}
