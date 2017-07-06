package de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka;

import java.util.Properties;

public class KafkaProducer {

private Properties props = new Properties();
	
	public KafkaProducer() {
		  props.put(KafkaConfig.BOOTSTRAP_SERVER, KafkaConfig.BOOTSTRAP_SERVER_VALUE);      
	      props.put(KafkaConfig.ACK, KafkaConfig.ACK_VALUE);	      
	      props.put(KafkaConfig.RETRIES, KafkaConfig.RETRIES_VALUE);
	      props.put(KafkaConfig.BATCH_SIZE, KafkaConfig.BATCH_SIZE_VALUE);
	      props.put(KafkaConfig.LINGER, KafkaConfig.LINGER_VALUE);	      
	      props.put(KafkaConfig.BUFFER, KafkaConfig.BUFFER_VALUE);	      
	      props.put(KafkaConfig.KEY_SERIALIZER, KafkaConfig.KEY_SERIALIZER_VALUE);	         
	      props.put(KafkaConfig.VALUE_SERIALIZER, KafkaConfig.VALUE_SERIALIZER_VALUE);
	}

	public Properties getProps() {
		return props;
	}

}
