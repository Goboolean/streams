package io.goboolean.streams.kafka;

import io.goboolean.streams.serde.Model;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Properties;

public class KafkaAggregateConsumerService {

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
                consumer.poll(100).forEach(record -> {
                    listener.onMessage(record.value());
                });
            }
        });
        pollingThread.start();
    }

    public void close() {
        pollingThread.interrupt();
        consumer.close();
    }
}
