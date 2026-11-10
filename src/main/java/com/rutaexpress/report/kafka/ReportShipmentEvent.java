package com.rutaexpress.report.kafka;

import com.rutaexpress.report.domain.ShipmentStatus;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.UUID;

record ReportShipmentEvent(int schemaVersion, UUID eventId, String eventType, Long shipmentId,
                           String trackingCode, String destination, ShipmentStatus status,
                           LocalDateTime createdAt, LocalDateTime updatedAt) {
    private static final Set<String> EVENT_TYPES =
            Set.of("shipment.created", "shipment.status_changed", "shipment.snapshot");

    void validate(String key) {
        if (schemaVersion != 1 || eventId == null || eventType == null || !EVENT_TYPES.contains(eventType)
                || shipmentId == null || shipmentId <= 0 || trackingCode == null || trackingCode.isBlank()
                || trackingCode.length() > 50 || !trackingCode.equals(key) || destination == null
                || destination.isBlank() || destination.length() > 250 || status == null
                || createdAt == null || updatedAt == null || updatedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("Evento de envío inválido");
        }
    }
}
