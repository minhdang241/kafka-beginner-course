package org.example.demos.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

  private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am a kafka consumer");

    String boostrapServer = "127.0.0.1:9092";
    String groupId = "my-third-application";
    String topic = "demo_java";
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(List.of(topic));

    // get a reference to the current thread
    final Thread mainThread = Thread.currentThread();

    // adding the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
        consumer.wakeup();

//         join the main thread to allow the execution of the code in the main thread
        try {
          mainThread.join();
          log.info("This thread: " + Thread.currentThread().getName() + " is running");
        } catch (Exception exception) {
          exception.printStackTrace();
        }
      }
    });

    try {
      while (true) {
        log.info("Polling");
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {
          log.info("Key: " + record.key() + ", Value: " + record.value());
          log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        }
      }
    } catch (WakeupException exception) {
      log.info("Wake up exception!");
    } catch (Exception exception) {
      log.error("Unexpected exception");
    } finally {
      consumer.close(); // this will also commit the offets if need be
      log.info("The consumer is now gracefully closed");
    }

  }
}
