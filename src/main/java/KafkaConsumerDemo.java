import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
	public static void main(String[] args) {
		Properties properties = new Properties();

		// kafka bootstrap server
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

		// ACKS
		properties.setProperty("group.id", "group1");
		properties.setProperty("enable.auto.commit", "true");
		properties.setProperty("auto.commit.intervals.ms", "1000");
		properties.setProperty("auto.offset.rest", "earliest");


		// launch kakfa consumer
		KafkaConsumer<String, String> kafkaComsumer = new KafkaConsumer<String, String>(properties);
		kafkaComsumer.subscribe(Arrays.asList("second_topic"));
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords = kafkaComsumer.poll(100);
			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				/*consumerRecord.value();
				consumerRecord.key();
				consumerRecord.offset();
				consumerRecord.partition();
				consumerRecord.topic();
				consumerRecord.timestamp();*/
				System.out.println("consumerRecord.partition() = " + consumerRecord.partition());
				System.out.println("consumerRecord.offset() = " + consumerRecord.offset());
				System.out.println("consumerRecord.key() = " + consumerRecord.key());
				System.out.println("consumerRecord.value() = " + consumerRecord.value());
			}
		}
	}
}
