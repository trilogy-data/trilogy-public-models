INSTALL vgi FROM community;
LOAD vgi;
ATTACH IF NOT EXISTS 'https://vgi-overture.rusty-bb6.workers.dev' AS overture (TYPE vgi);
