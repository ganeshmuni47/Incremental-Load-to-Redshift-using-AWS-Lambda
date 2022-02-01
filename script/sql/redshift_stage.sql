SET query_group TO 'ingest';

CREATE TABLE taxischema.nyc_greentaxi_tmp (LIKE taxischema.nyc_greentaxi);

COPY taxischema.nyc_greentaxi_tmp FROM '{0}' IAM_ROLE '{1}' csv ignoreheader 1 region '{2}' gzip;

DELETE FROM taxischema.nyc_greentaxi USING taxischema.nyc_greentaxi_tmp WHERE taxischema.nyc_greentaxi.vendorid = taxischema.nyc_greentaxi_tmp.vendorid AND taxischema.nyc_greentaxi.lpep_pickup_datetime = nyc_greentaxi_tmp.lpep_pickup_datetime;

INSERT INTO taxischema.nyc_greentaxi SELECT * FROM taxischema.nyc_greentaxi_tmp;

DROP TABLE taxischema.nyc_greentaxi_tmp;