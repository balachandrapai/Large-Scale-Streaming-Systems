import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Properties;


/**
 * Created by Pai on 09-04-2017.
 */
public class KafkaJSonProducer {

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
                "KafkaJsonSerializer.class.getName()");


        Producer<String, JSONObject> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, JSONObject>(props);
            try {
                JSONObject record = new JSONObject();
                record.put("name", "Harry Potter");
                record.put("age", "25");

                producer.send(new ProducerRecord<>(topicName, record));

//                JSONObject record2 = new JSONObject();
//                record.put("name", "Likith");
//                record.put("age", "25");
//
//                producer.send(new ProducerRecord<>(topicName, record2.toString()));
            }catch (JSONException e){
                e.printStackTrace();
            }
        System.out.println("Message sent successfully");
        producer.close();
    }
}
