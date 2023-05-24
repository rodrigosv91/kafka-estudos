package db.kafka.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction2<T> {
    void consume(ConsumerRecord<String, T> record);
}
