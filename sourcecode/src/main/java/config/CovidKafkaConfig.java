package config;

public class CovidKafkaConfig {
	public static String KAFKA_BROKERS = "quickstart.cloudera:9092";
    public static Integer MESSAGE_COUNT=1000;
    public static String CLIENT_ID="client1";
    public static String TOPIC_NAME="covid_us_counties";
    public static String GROUP_ID_CONFIG="consumerGroup1";
    public static Integer MESSAGE_SIZE=100000000;
    public static String KEY_SERIALIZER_KEY = "key.serializer";
    public static String KEY_SERIALIZER_VALUE = "org.apache.kafka.common.serialization.StringSerializer";
    public static String VALUE_SERIALIZER_KEY = "value.serializer";
    public static String VALUE_SERIALIZER_VALUE = "org.apache.kafka.common.serialization.StringSerializer";
}
