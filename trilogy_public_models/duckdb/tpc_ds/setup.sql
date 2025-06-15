INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=.5);