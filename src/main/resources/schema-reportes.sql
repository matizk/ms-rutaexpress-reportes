CREATE TABLE IF NOT EXISTS report_shipments (
    tracking_code varchar(50) PRIMARY KEY,
    shipment_id bigint NOT NULL,
    destination varchar(250) NOT NULL,
    status varchar(20) NOT NULL,
    created_at timestamp(6) NOT NULL,
    updated_at timestamp(6) NOT NULL,
    last_event_id uuid NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_report_shipments_created ON report_shipments (created_at);
