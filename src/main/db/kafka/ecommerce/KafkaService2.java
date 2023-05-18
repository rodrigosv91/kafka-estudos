package db.kafka.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaService2<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction2<T> parse;

    public KafkaService2(String groupId, String topico, ConsumerFunction2<T> parse, Class<T> type) {
        this(groupId, parse, type);
        consumer.subscribe(Collections.singletonList(topico));
    }

    public KafkaService2(String groupId, Pattern topico, ConsumerFunction2<T> parse, Class<T> type) {
        this(groupId, parse, type);
        consumer.subscribe(topico);
    }
    private KafkaService2(String groupId, ConsumerFunction2<T> parse, Class<T> type) {
        this.consumer = new KafkaConsumer<>(properties(groupId, type));
        this.parse = parse;
    }




    void run(){
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");

                for (var record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    private  Properties properties(String groupId, Class<T> type){
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());

        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
