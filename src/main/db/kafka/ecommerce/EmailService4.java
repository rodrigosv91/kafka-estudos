package db.kafka.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService4 {
    public static void main(String[] args) {
        var emailService = new EmailService4();
        try(var service = new KafkaService2<Email>(EmailService4.class.getSimpleName(),
                "ECCOMERCE_SEND_EMAIL", emailService::parse,
                Email.class)){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Email> record){
        System.out.println("------------------------------------------");
        System.out.println("Sending Email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email Enviado");

    }


}
