import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {
	public static void main(String[] args) {
		Properties properties = new Properties();

		// kafka bootstrap server
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		// ACKS
		properties.setProperty("acks", "1");
		properties.setProperty("retries", "3");
		properties.setProperty("linger.ms", "1");


		// launch kakfa producer
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int key = 0; key < 10; key++) {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("second_topic",
				Integer.toString(key), "message test" + Integer.toBinaryString(key));
			producer.send(producerRecord);
		}

		producer.flush();
		producer.close();
	}
}
