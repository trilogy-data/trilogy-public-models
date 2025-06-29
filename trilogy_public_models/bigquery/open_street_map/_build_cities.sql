CREATE OR REPLACE TABLE `preqldata.public_geo.osm_cities` AS
WITH cities_raw AS (
    SELECT 
        (SELECT value FROM UNNEST(all_tags) WHERE key = 'name') AS city_name,
        ST_CENTROID(geometry) AS city_point,
        feature_type,
        ST_GEOMETRYTYPE(geometry) AS geometry_type
    FROM 
        `bigquery-public-data.geo_openstreetmap.planet_features`
    WHERE 
        -- Filter for cities
        EXISTS (
            SELECT 1 FROM UNNEST(all_tags) AS tag 
            WHERE tag.key = 'place' AND tag.value = 'city'
        )
        -- Ensure we have a name
        AND EXISTS (
            SELECT 1 FROM UNNEST(all_tags) AS tag 
            WHERE tag.key = 'name' AND tag.value IS NOT NULL
        )
        -- Ensure we have valid geometry
        AND geometry IS NOT NULL
)
SELECT 
    c.city_name,
    co.country_name,
    co.country_code_alpha2,
    co.country_code_alpha3,
    adm.admin_name AS state_province_name,
    adm.iso_code AS state_iso_code,
    ST_Y(c.city_point) AS latitude,
    ST_X(c.city_point) AS longitude,
    c.feature_type,
    c.geometry_type
FROM cities_raw c
INNER JOIN `preqldata.public_geo.osm_countries` co 
    ON ST_CONTAINS(co.country_geometry, c.city_point)
LEFT JOIN `preqldata.public_geo.osm_state_province` adm
    ON ST_CONTAINS(adm.admin_geometry, c.city_point)
WHERE 
    -- Only include cities that successfully matched to a country
    co.country_name IS NOT NULL;