package io.goboolean.streams.streams;

import io.goboolean.streams.serde.Model;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class AggregateTransformer implements Transformer<Integer, Model.Aggregate, KeyValue<Integer, Model.Aggregate>> {
    private ProcessorContext context;

    private List<Model.Aggregate> aggregates = new ArrayList<>();
    private final String storeName;

    private TimeTruncationer.Truncationer truncationer;
    private ZonedDateTime roundedTime;
    private Duration duration;
    private LongCounter counter;

    public AggregateTransformer(String storeName, TimeTruncationer.Truncationer truncationer, Duration duration, LongCounter counter) {
        this.storeName = storeName;
        this.truncationer = truncationer;
        this.duration = duration;
        this.counter = counter;
    }

    public static class OneSec extends AggregateTransformer {
        public OneSec(String storeName) {
            super(
                    storeName,
                    new TimeTruncationer.FiveSecTruncationer(),
                    Duration.ofSeconds(1),
                    null,
                    /*
                    GlobalOpenTelemetry.getMeterProvider()
                            .get("fetch-system.streams")
                            .counterBuilder("fetch-system.streams.received.aggregate")
                            .build()
                    */
            );
        }
    }

    public static class FiveSec extends AggregateTransformer {
        public FiveSec(String storeName) {
            super(
                    storeName,
                    new TimeTruncationer.OneMinTruncationer(),
                    Duration.ofSeconds(5),
                    null,
                    /*
                    GlobalOpenTelemetry.getMeterProvider()
                            .get("fetch-system.streams")
                            .counterBuilder("fetch-system.streams.received.aggregate")
                            .build()
                    */
            );
        }
    }

    public static class OneMin extends AggregateTransformer {
        public OneMin(String storeName) {
            super(
                    storeName,
                    new TimeTruncationer.FiveMinTruncationer(),
                    Duration.ofSeconds(10),
                    null,
                    /*
                    GlobalOpenTelemetry.getMeterProvider()
                            .get("fetch-system.streams")
                            .counterBuilder("fetch-system.streams.received.aggregate")
                            .build()
                    */
            );
        }
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.aggregates = new StoreList<>(context.getStateStore(storeName));
    }

    @Override
    public KeyValue<Integer, Model.Aggregate> transform(Integer key, Model.Aggregate value) {
        counter.add(1);

        /*
        GlobalOpenTelemetry.getMeterProvider()
                .get("fetch-system.streams")
                .counterBuilder("fetch-system.streams.received.aggregate")
                .build();
        */

        if (roundedTime == null) { // Case when the first record is received
            roundedTime = truncationer.truncate(value.timestamp());
            aggregates.add(value);
            return null;
        }

        if (!roundedTime.equals(truncationer.truncate(value.timestamp()))) { // Case when the time has been changed

            long volume = aggregates.stream()
                    .mapToLong(Model.Aggregate::volume)
                    .sum();
            float average = (float) aggregates.stream()
                    .mapToDouble(a -> a.average() * a.volume())
                    .sum() / volume;

            Model.Aggregate merged;
            if (aggregates.size() == 1) {
                merged = new Model.Aggregate(
                        aggregates.get(0).symbol(),
                        aggregates.get(0).open(),
                        aggregates.get(0).close(),
                        aggregates.get(0).high(),
                        aggregates.get(0).low(),
                        average,
                        volume,
                        roundedTime
                );
            } else {
                merged = aggregates.stream()
                        .reduce((a1, a2) -> new Model.Aggregate(
                                a1.symbol(),
                                a1.open(),
                                a2.close(),
                                Math.max(a1.high(), a2.high()),
                                Math.min(a1.low(), a2.low()),
                                average,
                                volume,
                                roundedTime
                        ))
                        .get();
            }

            aggregates.clear();
            aggregates.add(value);
            roundedTime = truncationer.truncate(value.timestamp());

            return KeyValue.pair(key, merged);
        }

        if (!truncationer.truncate(value.timestamp().plus(duration)).equals(truncationer.truncate(value.timestamp()))) {
            // Case when the time has been changed now
            aggregates.add(value);

            long volume = aggregates.stream()
                    .mapToLong(Model.Aggregate::volume)
                    .sum();
            float average = (float) aggregates.stream()
                    .mapToDouble(a -> a.average() * a.volume())
                    .sum() / volume;

            Model.Aggregate merged;
            if (aggregates.size() == 1) {
                merged = new Model.Aggregate(
                        aggregates.get(0).symbol(),
                        aggregates.get(0).open(),
                        aggregates.get(0).close(),
                        aggregates.get(0).high(),
                        aggregates.get(0).low(),
                        average,
                        volume,
                        roundedTime
                );
            } else {
                merged = aggregates.stream()
                        .reduce((a1, a2) -> new Model.Aggregate(
                                a1.symbol(),
                                a1.open(),
                                a2.close(),
                                Math.max(a1.high(), a2.high()),
                                Math.min(a1.low(), a2.low()),
                                average,
                                volume,
                                roundedTime
                        ))
                        .get();
            }

            aggregates.clear();
            roundedTime = truncationer.truncate(value.timestamp().plus(duration));

            return KeyValue.pair(key, merged);
        }

        if (roundedTime.equals(truncationer.truncate(value.timestamp()))) { // Case when the time has not changed
            aggregates.add(value);
            return null;
        }

        return null;
    }

    @Override
    public void close() {}
}
