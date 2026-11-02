CREATE SCHEMA analytics;
GO

CREATE TABLE analytics.route_service_daily (
    service_date DATE NOT NULL,
    route_id VARCHAR(100) NOT NULL,
    trip_count BIGINT NOT NULL,
    average_arrival_delay_minutes DECIMAL(10, 2) NOT NULL,
    on_time_percentage DECIMAL(5, 2) NOT NULL
);
GO
