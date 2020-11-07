package kafkademo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumoKafka {
	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		/*
		 * LEMBRETE: Com earliest ou sem earlist mensagens do mesmo grupo N�O ser�o
		 * lidas novamente. Para testar o conceito do earliest crie um novo grupo e
		 * ent�o execute o c�digo. O esperado � que ent�o todas as mensagens do t�pico
		 * apare�am.
		 */
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);) {

			consumer.subscribe(Arrays.asList("testeJava4"));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.value());
				}

			}
		}

	}
}
