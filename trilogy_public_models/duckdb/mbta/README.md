# Boston Transit Pulse (MBTA)

## Overview

Live **Boston subway reliability** data: current vehicle positions, predicted
arrivals, timetabled service, active alerts — and gold-level reliability
metrics comparing what riders are *getting* against what the schedule
*promises*. Covers every subway line (Red, Mattapan, Orange, Blue, Green
B/C/D/E).

**Source:** [MBTA V3 API](https://api-v3.mbta.com) (GTFS/GTFS-realtime),
snapshotted hourly (except ~2–4am Boston, when the T is closed) by the
trilogy-cloud `boston-transit-pulse` pipeline. Every asset carries
`mbta_data_updated_through` — the snapshot watermark. Assets are point-in-time
snapshots, not history: each refresh replaces the previous one.

All facts share conformed `route_id` / `stop_id` / `trip_id` / `direction_id`
keys (see `common.preql`), so cross-fact questions resolve without explicit
joins.

## Assets

| Datasource | Grain | What it holds |
|---|---|---|
| `routes` | route | Line names, colors, direction names/destinations |
| `stops` | stop | Platforms + parent stations, denormalized, with coordinates |
| `vehicle_observations` | vehicle | Where every train is right now |
| `prediction_observations` | predicted stop event | Predicted arrivals/departures with uncertainty |
| `scheduled_stop_events` | scheduled stop event | The timetable, −1h..+3h around the snapshot |
| `active_alerts` | (alert, informed entity) | Active disruptions and what they touch |
| `route_headways` | route × stop × direction | Predicted vs scheduled headway, `headway_ratio` |
| `headway_events` | predicted stop event | Each arrival classified bunching / service_gap / normal |
| `alert_service_impact` | (alert, informed entity) | Alerts joined to live route headway pressure |

## Example questions

```trilogy
# Which routes are under headway pressure right now?
where headway_ratio > 1.2
select route_id, route_name, count(stop_id) as pressured_stops, avg(headway_ratio) as avg_ratio
order by avg_ratio desc;

# Where is the biggest gap between consecutive trains?
where headway_event_type = 'service_gap'
select route_id, stop_name, direction_id, predicted_gap_minutes, predicted_event_time
order by predicted_gap_minutes desc limit 10;

# Which active alerts sit on genuinely degraded service?
where route_headway_pressure > 1.3
select alert_id, alert_effect, alert_severity, alert_service_effect, route_id, route_headway_pressure
order by route_headway_pressure desc;

# Live vehicle map data
select vehicle_id, route_id, vehicle_latitude, vehicle_longitude, vehicle_bearing, vehicle_status;
```

## License / attribution

Data from the Massachusetts Bay Transportation Authority (MBTA), via the
public V3 API. See the
[MBTA developer terms](https://www.mbta.com/developers/v3-api) for usage
conditions.
