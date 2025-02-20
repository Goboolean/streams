package io.goboolean.streams.kafka;

import io.goboolean.streams.serde.Model;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;


public class KafkaAggregateConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAggregateConsumerService.class);

    private final Properties props;
    private final Consumer<Integer, Model.Aggregate> consumer;
    private final KafkaConsumerListener listener;

    private String[] topics;
    private Thread pollingThread;

    public KafkaAggregateConsumerService(Properties props, KafkaConsumerListener listener) {
        this.props = props;
        this.listener = listener;

        this.consumer = new KafkaConsumer<>(props);
    }

    public void run(String[] topics) {
        this.topics = topics;
        consumer.subscribe(Arrays.asList(topics));

        pollingThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    consumer.poll(100).forEach(record -> {
                        logger.debug("Received message - Topic: {}, Partition: {}, Offset: {}",
                            record.topic(), record.partition(), record.offset());  
                        listener.onMessage(record.value());
                    });
                } catch (Exception e) {
                    logger.error("Error polling Kafka", e);
                }
            }
        });
        pollingThread.start();
    }

    public void close() {
        pollingThread.interrupt();
        consumer.close();
    }
}
