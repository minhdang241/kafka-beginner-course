package org.example.demos.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

  private static final Logger log = LoggerFactory.getLogger(
      ProducerDemoWithCallback.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am a producer with callback");

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for (int i = 0; i < 10; i++) {
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java",
          "hello world " + i);
      producer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          log.info("callback is executed");
          if (exception == null) {
            log.info("Received new metadata/ \n" +
                "Topic: " + metadata.topic() + "\n" +
                "Partition: " + metadata.partition() + "\n" +
                "Offset: " + metadata.offset() + "\n" +
                "Timestamp: " + metadata.timestamp());
          } else {
            log.error("Error while producing", exception);
          }
        }
      });
      try {
        Thread.sleep(5000);
      } catch (Exception exception) {
        exception.printStackTrace();
      }
    }

    producer.flush();

    producer.close();

  }

}
