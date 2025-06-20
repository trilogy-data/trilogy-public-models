CREATE OR REPLACE TABLE `preqldata.public_geo.osm_state_province` AS
WITH admin_raw AS (
    SELECT 
        COALESCE(
            (SELECT value FROM UNNEST(all_tags) WHERE key = 'name:en'),
            (SELECT value FROM UNNEST(all_tags) WHERE key = 'name')
        ) AS admin_name,
        (SELECT value FROM UNNEST(all_tags) WHERE key = 'admin_level') AS admin_level,
        -- General ISO codes if available
        (SELECT value FROM UNNEST(all_tags) WHERE key = 'ISO3166-2') AS iso_code,
        geometry AS admin_geometry,
        ST_AREA(geometry) AS admin_area_sqm,
        ST_CENTROID(geometry) AS admin_centroid
    FROM 
        `bigquery-public-data.geo_openstreetmap.planet_features`
    WHERE 
        -- Second-level administrative boundaries (states/provinces)
        EXISTS (
            SELECT 1 FROM UNNEST(all_tags) AS tag 
            WHERE tag.key = 'admin_level' AND tag.value = '4'
        )
        AND EXISTS (
            SELECT 1 FROM UNNEST(all_tags) AS tag 
            WHERE tag.key = 'boundary' AND tag.value = 'administrative'
        )
        AND EXISTS (
            SELECT 1 FROM UNNEST(all_tags) AS tag 
            WHERE tag.key = 'ISO3166-2' AND tag.value IS NOT NULL
        )
        -- Ensure we have a proper name
        AND (
            EXISTS (SELECT 1 FROM UNNEST(all_tags) WHERE key = 'name:en' AND value IS NOT NULL)
            OR EXISTS (SELECT 1 FROM UNNEST(all_tags) WHERE key = 'name' AND value IS NOT NULL)
        )
        -- Valid geometry
        AND geometry IS NOT NULL
        -- Exclude very small areas (likely errors)
        AND ST_AREA(geometry) > 100000  -- roughly 0.1 sq km
)
SELECT 
    a.admin_name,
    a.admin_level,
    a.iso_code, 
    -- Country information from spatial join
    c.country_name,
    c.country_code_alpha2,
    c.country_code_alpha3,

    -- Geometry and metrics
    a.admin_geometry,
    a.admin_area_sqm,
    a.admin_centroid
FROM admin_raw a
INNER JOIN `preqldata.public_geo.osm_countries` c
    ON ST_CONTAINS(c.country_geometry, a.admin_centroid);