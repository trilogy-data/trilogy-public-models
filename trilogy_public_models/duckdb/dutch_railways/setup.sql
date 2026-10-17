INSTALL vgi FROM community;
LOAD vgi;
ATTACH IF NOT EXISTS 'https://vgi-trains-ts.rusty-bb6.workers.dev' AS dutch_railways (TYPE vgi);
