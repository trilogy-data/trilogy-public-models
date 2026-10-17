INSTALL vgi FROM community;
LOAD vgi;
ATTACH IF NOT EXISTS 'https://vgi-earthquakes.rusty-bb6.workers.dev' AS usgs_earthquakes (TYPE vgi);
