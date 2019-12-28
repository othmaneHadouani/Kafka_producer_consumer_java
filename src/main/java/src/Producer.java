package src;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Producer {

    private int counter;
    public static void main(String[] args) {
        new Producer();
    }
    public Producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        Random random = new Random();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()-> {
            String key = String.valueOf(++counter);
            String value = String.valueOf(random.nextDouble()*1234);
            kafkaProducer.send(new ProducerRecord<>("testTopic",key,value), (recordMetadata, e) -> {
                System.out.println("value sent :" + value + " Partition :"+recordMetadata.partition());
            });
        },1000,1000, TimeUnit.MILLISECONDS);

    }
}
