package db.kafka.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction2 {
    void consume(ConsumerRecord<String, String> record);
}
