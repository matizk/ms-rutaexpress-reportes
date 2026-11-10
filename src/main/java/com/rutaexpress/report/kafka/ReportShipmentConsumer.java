package com.rutaexpress.report.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("kafka")
class ReportShipmentConsumer {
    private static final Logger log = LoggerFactory.getLogger(ReportShipmentConsumer.class);
    private final ObjectMapper json;
    private final ShipmentProjectionStore projection;
    private final Counter applied;
    private final Counter ignored;

    ReportShipmentConsumer(ObjectMapper json, ShipmentProjectionStore projection, MeterRegistry registry) {
        this.json = json;
        this.projection = projection;
        this.applied = registry.counter("rutaexpress.report.events", "result", "applied");
        this.ignored = registry.counter("rutaexpress.report.events", "result", "duplicate_or_stale");
    }

    @KafkaListener(topics = "${report.kafka.topic}")
    void consume(ConsumerRecord<String, String> record) {
        ReportShipmentEvent event;
        try {
            event = json.readValue(record.value(), ReportShipmentEvent.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("JSON de evento inválido", ex);
        }
        if (event == null) {
            throw new IllegalArgumentException("Evento de envío vacío");
        }
        event.validate(record.key());
        // El registro se confirma en PostgreSQL antes de que el listener devuelva el control a Kafka.
        if (projection.apply(event)) {
            applied.increment();
            log.info("REPORT_KAFKA_APPLIED eventId={} status={}", event.eventId(), event.status());
        } else {
            ignored.increment();
        }
    }
}
