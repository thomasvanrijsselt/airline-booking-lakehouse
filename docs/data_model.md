# Gold data model

## `dim_date`

**Grain:** one row per departure date present in the current booking data.

The deterministic `date_key` uses the `yyyyMMdd` format.

## `dim_flight`

**Grain:** one row per unique combination of flight number, departure airport and arrival airport.

`flight_key` is a SHA-256 hash of the natural key:

- `flight_number`
- `departure_airport`
- `arrival_airport`

## `fact_booking`

**Grain:** one row per booking, representing its latest valid event by business event time.

The fact table retains active and cancelled bookings. Analytical queries must explicitly include or exclude cancellations depending on the required metric.

Measures include:

- `passenger_count`
- `amount`

Dimension references include:

- `date_key`
- `flight_key`