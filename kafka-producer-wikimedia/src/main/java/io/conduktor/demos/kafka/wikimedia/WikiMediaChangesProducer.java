package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventSource.Builder;
import com.launchdarkly.eventsource.MessageEvent;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class WikiMediaChangesProducer {

  public static void main(String[] args) throws InterruptedException {
    String boostrapServer = "127.0.0.1:9092";

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    String topic = "wikimedia.recentchange";

    EventHandler eventHandler = new WikiMediaChangeHandler(producer, topic);
    String url = "https://stream.wikimedia.org/v2/stream/recentchange";
    EventSource.Builder builder = new Builder(eventHandler, URI.create(url));
    EventSource eventSource = builder.build();

    // start the producer in another thread
    eventSource.start();

    TimeUnit.MINUTES.sleep(10);
  }
}
