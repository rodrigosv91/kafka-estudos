package db.kafka.ecommerce;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher2<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher2<Email>()) {
                for (int i = 0; i < 10; i++) {
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                    var order = new Order(userId, orderId, amount);
                    orderDispatcher.send("ECCOMERCE_NEW_ORDER", userId, order);

                    var email = new Email("Order confirmation", "Welcome! Processando  pedido!");
                    emailDispatcher.send("ECCOMERCE_SEND_EMAIL", userId, email);
                }
            }
        }
    }

}
