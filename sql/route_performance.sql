SELECT
    date.year,
    date.month,
    flight.route,
    flight.flight_number,
    COUNT(*) AS active_booking_count,
    SUM(booking.passenger_count) AS passenger_count,
    ROUND(SUM(booking.amount), 2) AS booking_revenue_eur,
    ROUND(AVG(booking.amount), 2) AS average_booking_value_eur
FROM workspace.airline_booking_lakehouse.fact_booking AS booking
INNER JOIN workspace.airline_booking_lakehouse.dim_date AS date
    ON booking.date_key = date.date_key
INNER JOIN workspace.airline_booking_lakehouse.dim_flight AS flight
    ON booking.flight_key = flight.flight_key
WHERE booking.booking_status = 'ACTIVE'
GROUP BY
    date.year,
    date.month,
    flight.route,
    flight.flight_number
ORDER BY
    date.year,
    date.month,
    booking_revenue_eur DESC;