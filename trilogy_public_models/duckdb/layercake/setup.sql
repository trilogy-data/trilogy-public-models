INSTALL httpfs;

LOAD httpfs;

-- LayerCake (https://openstreetmap.us/our-work/layercake/) publishes thematic
-- GeoParquet extracts of the full OpenStreetMap planet at
-- https://data.openstreetmap.us/layercake/. These are views, not tables: the
-- large layers are far too big to materialize (buildings is ~700M rows), and
-- DuckDB pushes column selection and bbox filters down to HTTP range reads
-- against the remote parquet. The views also flatten the bbox struct, rename
-- namespaced OSM tag columns (building:levels -> building_levels), and take
-- the primary entry of multilingual name arrays. The raw WKB geometry column
-- is intentionally excluded; use bbox / centroid for filtering and mapping.

CREATE OR REPLACE VIEW buildings AS
SELECT
    type,
    id,
    building,
    "building:levels" AS building_levels,
    "building:flats" AS building_flats,
    "building:material" AS building_material,
    "building:colour" AS building_colour,
    "building:part" AS building_part,
    "building:use" AS building_use,
    name,
    "addr:housenumber" AS addr_housenumber,
    "addr:street" AS addr_street,
    "addr:city" AS addr_city,
    "addr:postcode" AS addr_postcode,
    website,
    wikipedia,
    wikidata,
    height,
    "roof:shape" AS roof_shape,
    "roof:levels" AS roof_levels,
    "roof:colour" AS roof_colour,
    "roof:material" AS roof_material,
    "roof:orientation" AS roof_orientation,
    "roof:height" AS roof_height,
    start_date,
    access,
    wheelchair,
    bbox.xmin AS bbox_xmin,
    bbox.ymin AS bbox_ymin,
    bbox.xmax AS bbox_xmax,
    bbox.ymax AS bbox_ymax,
    (bbox.xmin + bbox.xmax) / 2 AS longitude,
    (bbox.ymin + bbox.ymax) / 2 AS latitude
FROM read_parquet('https://data.openstreetmap.us/layercake/buildings.parquet');

CREATE OR REPLACE VIEW highways AS
SELECT
    type,
    id,
    highway,
    service,
    crossing,
    cycleway,
    "cycleway:left" AS cycleway_left,
    "cycleway:right" AS cycleway_right,
    footway,
    construction,
    name,
    ref,
    bridge,
    covered,
    lanes,
    layer,
    lit,
    sidewalk,
    smoothness,
    surface,
    tracktype,
    tunnel,
    wheelchair,
    width,
    access,
    bicycle,
    bus,
    foot,
    hgv,
    maxspeed,
    motor_vehicle,
    motorcycle,
    oneway,
    toll,
    bbox.xmin AS bbox_xmin,
    bbox.ymin AS bbox_ymin,
    bbox.xmax AS bbox_xmax,
    bbox.ymax AS bbox_ymax,
    (bbox.xmin + bbox.xmax) / 2 AS longitude,
    (bbox.ymin + bbox.ymax) / 2 AS latitude
FROM read_parquet('https://data.openstreetmap.us/layercake/highways.parquet');

CREATE OR REPLACE VIEW boundaries AS
SELECT
    type,
    id,
    boundary,
    admin_level,
    name[1] AS name,
    official_name[1] AS official_name,
    int_name[1] AS int_name,
    alt_name[1] AS alt_name,
    place,
    border_type,
    "ISO3166-2" AS iso_3166_2,
    "ISO3166-1:alpha2" AS iso_3166_1_alpha2,
    "ISO3166-1:alpha3" AS iso_3166_1_alpha3,
    wikidata,
    wikipedia,
    bbox.xmin AS bbox_xmin,
    bbox.ymin AS bbox_ymin,
    bbox.xmax AS bbox_xmax,
    bbox.ymax AS bbox_ymax,
    (bbox.xmin + bbox.xmax) / 2 AS longitude,
    (bbox.ymin + bbox.ymax) / 2 AS latitude
FROM read_parquet('https://data.openstreetmap.us/layercake/boundaries.parquet');

CREATE OR REPLACE VIEW settlements AS
SELECT
    type,
    id,
    place,
    name,
    alt_name,
    official_name,
    wikidata,
    wikipedia,
    population,
    bbox.xmin AS bbox_xmin,
    bbox.ymin AS bbox_ymin,
    bbox.xmax AS bbox_xmax,
    bbox.ymax AS bbox_ymax,
    (bbox.xmin + bbox.xmax) / 2 AS longitude,
    (bbox.ymin + bbox.ymax) / 2 AS latitude
FROM read_parquet('https://data.openstreetmap.us/layercake/settlements.parquet');

CREATE OR REPLACE VIEW parks AS
SELECT
    type,
    id,
    boundary,
    protected_area,
    leisure,
    name[1] AS name,
    short_name[1] AS short_name,
    official_name[1] AS official_name,
    protect_class,
    protection_title,
    protected,
    iucn_level,
    access,
    operator,
    "operator:type" AS operator_type,
    owner,
    ownership,
    start_date,
    related_law,
    website,
    wikidata,
    wikipedia,
    bbox.xmin AS bbox_xmin,
    bbox.ymin AS bbox_ymin,
    bbox.xmax AS bbox_xmax,
    bbox.ymax AS bbox_ymax,
    (bbox.xmin + bbox.xmax) / 2 AS longitude,
    (bbox.ymin + bbox.ymax) / 2 AS latitude
FROM read_parquet('https://data.openstreetmap.us/layercake/parks.parquet');
