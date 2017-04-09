/**
 * Created by Pai on 08-03-2017.
 */

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
class SimpleProducer {

    public static void main(String[] args) throws Exception{

        String topicName = "test";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        producer.send(new ProducerRecord<String, String>(topicName,
                "1", "Hello"));
        producer.send(new ProducerRecord<String, String>(topicName,
                "2", "World"));

        System.out.println("Message sent successfully");
        producer.close();
    }
}
