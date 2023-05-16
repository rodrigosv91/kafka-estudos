package db.kafka.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService2 {
    public static void main(String[] args) {
        var fraudService = new FraudDetectorService2();
        var service = new kafkaService2(FraudDetectorService.class.getSimpleName(),"ECCOMERCE_NEW_ORDER", fraudService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }

}
