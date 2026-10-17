INSTALL vgi FROM community;
LOAD vgi;
ATTACH IF NOT EXISTS 'https://vgi-open-meteo.rusty-bb6.workers.dev' AS open_meteo (TYPE vgi);
