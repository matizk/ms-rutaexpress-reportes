package com.rutaexpress.report.kafka;

import com.rutaexpress.report.domain.ShipmentStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Profile("kafka")
class ReportKafkaConfiguration {
    @Bean
    NewTopic shipmentReportTopic(@Value("${report.kafka.topic}") String name,
                                 @Value("${report.kafka.replicas}") int replicas) {
        return TopicBuilder.name(name).partitions(3).replicas(replicas).build();
    }

    @Bean
    NewTopic invalidReportEventsTopic(@Value("${report.kafka.topic}") String name,
                                      @Value("${report.kafka.replicas}") int replicas) {
        return TopicBuilder.name(name + ".dlt").partitions(3).replicas(replicas).build();
    }

    @Bean
    DefaultErrorHandler reportKafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(record.topic() + ".dlt", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    @Bean
    MeterBinder shipmentMetrics(ShipmentProjectionStore projection) {
        return registry -> {
            for (ShipmentStatus status : ShipmentStatus.values()) {
                Gauge.builder("rutaexpress.report.shipments", projection, store -> store.count(status))
                        .description("Envíos en la proyección de Reportes por estado")
                        .tag("status", status.name())
                        .register(registry);
            }
        };
    }
}
