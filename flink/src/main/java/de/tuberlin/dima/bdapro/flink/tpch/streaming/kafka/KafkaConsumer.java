package de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka;

import java.util.Properties;

public class KafkaConsumer {

private Properties props = new Properties();
	
	public KafkaConsumer() {
		props.put(KafkaConfig.TOPIC_NAME, KafkaConfig.TOPIC_NAME_VALUE);   
		props.put(KafkaConfig.BOOTSTRAP_SERVER, KafkaConfig.BOOTSTRAP_SERVER_VALUE);   
		props.put(KafkaConfig.ZOOKEEPER, KafkaConfig.ZOOKEPER_VALUE);  
		props.put(KafkaConfig.GROUP_ID, KafkaConfig.GROUP_ID_VALUE);  
	}

	public Properties getProps() {
		return props;
	}

}
