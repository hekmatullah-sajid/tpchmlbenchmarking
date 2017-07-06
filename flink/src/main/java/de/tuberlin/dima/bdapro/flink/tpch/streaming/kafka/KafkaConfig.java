package de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka;

public class KafkaConfig {

	public static final String TOPIC_NAME = "topic";
	public static final String TOPIC_NAME_VALUE = "tpch";
	public static final String BOOTSTRAP_SERVER = "bootstrap.servers";
	public static final String BOOTSTRAP_SERVER_VALUE = "localhost:9092";
	public static final String ZOOKEEPER = "zookeeper.connect";
	public static final String ZOOKEPER_VALUE = "localhost:2181";
	public static final String GROUP_ID = "group.id";
	public static final String GROUP_ID_VALUE = "group1";
	public static final String ACK = "acks";
	public static final String ACK_VALUE = "all";
	public static final String RETRIES = "retries";
	public static final String RETRIES_VALUE = "0";
	public static final String BATCH_SIZE = "batch.size";
	public static final String BATCH_SIZE_VALUE = "16384";
	public static final String LINGER = "linger.ms";
	public static final String LINGER_VALUE = "1";
	public static final String BUFFER = "buffer.memory";
	public static final String BUFFER_VALUE = "33554432";
	public static final String KEY_SERIALIZER = "key.serializer";
	public static final String KEY_SERIALIZER_VALUE = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String VALUE_SERIALIZER = "value.serializer";
	public static final String VALUE_SERIALIZER_VALUE = "org.apache.kafka.common.serialization.StringSerializer";

}
