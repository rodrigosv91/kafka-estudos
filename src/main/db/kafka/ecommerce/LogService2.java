package db.kafka.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService2 {
    public static void main(String[] args) {
        var logService = new LogService2();
        try(var consumer = new KafkaService2(LogService2.class.getSimpleName(),
                Pattern.compile("ECCOMERCE.*"),
                logService::parse,
                String.class, Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))){
            consumer.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record){
        System.out.println("------------------------------------------");
        System.out.println("LOG : " + record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }

}
