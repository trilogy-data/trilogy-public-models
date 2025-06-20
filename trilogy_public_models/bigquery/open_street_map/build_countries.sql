CREATE OR REPLACE TABLE `preqldata.public_geo.osm_countries` AS

SELECT 
    COALESCE(
        (SELECT value FROM UNNEST(all_tags) WHERE key = 'name:en'),
        (SELECT value FROM UNNEST(all_tags) WHERE key = 'name')
    ) AS country_name,
    (SELECT value FROM UNNEST(all_tags) WHERE key = 'ISO3166-1:alpha2') AS country_code_alpha2,
    (SELECT value FROM UNNEST(all_tags) WHERE key = 'ISO3166-1:alpha3') AS country_code_alpha3,
    (SELECT value FROM UNNEST(all_tags) WHERE key = 'ISO3166-1') AS country_code_numeric,
    geometry AS country_geometry,
    ST_AREA(geometry) AS country_area_sqm,
    -- all_tags
FROM 
    `bigquery-public-data.geo_openstreetmap.planet_features`
WHERE 
    -- Country-level administrative boundaries
    EXISTS (
        SELECT 1 FROM UNNEST(all_tags) AS tag 
        WHERE tag.key = 'admin_level' AND tag.value = '2'
    )
    AND EXISTS (
        SELECT 1 FROM UNNEST(all_tags) AS tag 
        WHERE tag.key = 'boundary' AND tag.value = 'administrative'
    )
AND EXISTS (
        SELECT 1 FROM UNNEST(all_tags) AS tag 
        WHERE tag.key = 'ISO3166-1:alpha2' AND tag.value IS NOT NULL
    )
    -- Ensure we have a name
    AND (
        EXISTS (SELECT 1 FROM UNNEST(all_tags) WHERE key = 'name:en' AND value is not null)
        OR EXISTS (SELECT 1 FROM UNNEST(all_tags) WHERE key = 'name' AND value is not null)
    )
    -- Valid geometry
    AND geometry IS NOT NULL;