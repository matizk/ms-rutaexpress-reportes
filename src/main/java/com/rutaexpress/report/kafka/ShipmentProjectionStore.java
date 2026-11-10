package com.rutaexpress.report.kafka;

import com.rutaexpress.report.client.ShipmentReader;
import com.rutaexpress.report.domain.ShipmentSnapshot;
import com.rutaexpress.report.domain.ShipmentStatus;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Profile("kafka")
class ShipmentProjectionStore implements ShipmentReader {
    private final JdbcTemplate jdbc;

    ShipmentProjectionStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    boolean apply(ReportShipmentEvent event) {
        // Un reintento o un evento anterior no vuelve a contar ni hace retroceder el estado.
        return jdbc.update("""
                INSERT INTO report_shipments
                    (tracking_code, shipment_id, destination, status, created_at, updated_at, last_event_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (tracking_code) DO UPDATE SET
                    shipment_id = EXCLUDED.shipment_id,
                    destination = EXCLUDED.destination,
                    status = EXCLUDED.status,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    last_event_id = EXCLUDED.last_event_id
                WHERE report_shipments.updated_at < EXCLUDED.updated_at
                """, event.trackingCode(), event.shipmentId(), event.destination(), event.status().name(),
                event.createdAt(), event.updatedAt(), event.eventId()) > 0;
    }

    @Override
    public List<ShipmentSnapshot> findCreatedBetween(LocalDate from, LocalDate to) {
        return jdbc.query("""
                SELECT shipment_id, tracking_code, destination, status, created_at, updated_at
                FROM report_shipments
                WHERE created_at >= ? AND created_at < ?
                """, (row, index) -> new ShipmentSnapshot(row.getLong("shipment_id"), row.getString("tracking_code"),
                        row.getString("destination"), ShipmentStatus.valueOf(row.getString("status")),
                        row.getObject("created_at", LocalDateTime.class), row.getObject("updated_at", LocalDateTime.class)),
                from.atStartOfDay(), to.plusDays(1).atStartOfDay());
    }

    long count(ShipmentStatus status) {
        return jdbc.queryForObject("SELECT count(*) FROM report_shipments WHERE status = ?", Long.class, status.name());
    }
}
