package io.goboolean.streams.config;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.instrumentation.log4j.appender.v2_17.OpenTelemetryAppender;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ResourceAttributes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class OpenTelemetryConfig {

    @Value("${open-telemetry.endpoint}")
    private String endPoint;

    @Value("${open-telemetry.service-name}")
    private String serviceName;

    @Value("${open-telemetry.service-version}")
    private String serviceVersion;

    @Bean
    public OpenTelemetry openTelemetry() {

        Resource resource = Resource.getDefault().toBuilder()
                .put(ResourceAttributes.SERVICE_NAME, serviceName)
                .put(ResourceAttributes.SERVICE_VERSION, serviceVersion)
                .build();

        MetricExporter metricExporter = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endPoint)
                .setTimeout(30, TimeUnit.SECONDS)
                .build();

        LogRecordExporter logRecordExporter = OtlpGrpcLogRecordExporter.builder()
                .setEndpoint(endPoint)
                .setTimeout(30, TimeUnit.SECONDS)
                .build();

        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
                .registerMetricReader(
                        PeriodicMetricReader.builder(metricExporter)
                                .build())
                .setResource(resource)
                .build();

        SdkLoggerProvider sdkLoggerProvider = SdkLoggerProvider.builder()
                .addLogRecordProcessor(
                        BatchLogRecordProcessor.builder(logRecordExporter)
                                .build())
                .setResource(resource)
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(sdkMeterProvider)
                .setLoggerProvider(sdkLoggerProvider)
                .buildAndRegisterGlobal(); 

        //if (GlobalOpenTelemetry.get() == OpenTelemetry.noop()) {
        //    GlobalOpenTelemetry.set(openTelemetry);
        //}

        OpenTelemetryAppender.install(openTelemetry);

        return openTelemetry;
    }

    @Bean(name = "fetch-system.streams.received.trade")
    public LongCounter receivedTradeCounter(OpenTelemetry openTelemetry) {
        return openTelemetry
                .getMeter("fetch-system.streams")
                .counterBuilder("fetch-system.streams.received.trade")
                .setDescription("")
                .build();
    }

    @Bean(name = "fetch-system.streams.exported.trade")
    public LongCounter exportedTradeCounter(OpenTelemetry openTelemetry) {
        return openTelemetry
                .getMeter("fetch-system.streams")
                .counterBuilder("fetch-system.streams.received.trade")
                .setDescription("")
                .build();
    }

    @Bean(name = "fetch-system.streams.received.1s")
    public LongCounter received1sAggregateCounter(OpenTelemetry openTelemetry) {
        return openTelemetry
                .getMeter("fetch-system.streams")
                .counterBuilder("fetch-system.streams.received.1s-aggregate")
                .setDescription("")
                .build();
    }

    @Bean(name = "fetch-system.streams.received.5s")
    public LongCounter received5sAggregateCounter(OpenTelemetry openTelemetry) {
        return openTelemetry
                .getMeter("fetch-system.streams")
                .counterBuilder("fetch-system.streams.received.5s-aggregate")
                .setDescription("")
                .build();
    }

    @Bean(name = "fetch-system.streams.received.1m")
    public LongCounter exported1mAggregateCounter(OpenTelemetry openTelemetry) {
        return openTelemetry
                .getMeter("fetch-system.streams")
                .counterBuilder("fetch-system.streams.exported.1m-aggregate")
                .setDescription("")
                .build();
    }
}
