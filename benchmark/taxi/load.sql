CREATE TABLE trips (
    trip_id                 BIGINT,
    vendor_id               VARCHAR,
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    store_and_fwd_flag      VARCHAR,
    rate_code_id            BIGINT,
    pickup_longitude        DOUBLE,
    pickup_latitude         DOUBLE,
    dropoff_longitude       DOUBLE,
    dropoff_latitude        DOUBLE,
    passenger_count         BIGINT,
    trip_distance           DOUBLE,
    fare_amount             DOUBLE,
    extra                   DOUBLE,
    mta_tax                 DOUBLE,
    tip_amount              DOUBLE,
    tolls_amount            DOUBLE,
    ehail_fee               DOUBLE,
    improvement_surcharge   DOUBLE,
    total_amount            DOUBLE,
    payment_type            VARCHAR,
    trip_type               VARCHAR,
    pickup                  VARCHAR,
    dropoff                 VARCHAR,
    cab_type                VARCHAR,
    precipitation           BIGINT,
    snow_depth              BIGINT,
    snowfall                BIGINT,
    max_temperature         BIGINT,
    min_temperature         BIGINT,
    average_wind_speed      BIGINT,
    pickup_nyct2010_gid     BIGINT,
    pickup_ctlabel          VARCHAR,
    pickup_borocode         BIGINT,
    pickup_boroname         VARCHAR,
    pickup_ct2010           VARCHAR,
    pickup_boroct2010       BIGINT,
    pickup_cdeligibil       VARCHAR,
    pickup_ntacode          VARCHAR,
    pickup_ntaname          VARCHAR,
    pickup_puma             VARCHAR,
    dropoff_nyct2010_gid    BIGINT,
    dropoff_ctlabel         VARCHAR,
    dropoff_borocode        BIGINT,
    dropoff_boroname        VARCHAR,
    dropoff_ct2010          VARCHAR,
    dropoff_boroct2010      VARCHAR,
    dropoff_cdeligibil      VARCHAR,
    dropoff_ntacode         VARCHAR,
    dropoff_ntaname         VARCHAR,
    dropoff_puma            VARCHAR);

INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_aa.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ai.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_aq.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ay.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bg.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bo.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bw.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ce.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cm.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cu.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dc.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dk.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ab.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_aj.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ar.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_az.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bh.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bp.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bx.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cf.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cn.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cv.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dd.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dl.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ac.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ak.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_as.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ba.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bi.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bq.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_by.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cg.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_co.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cw.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_de.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dm.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ad.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_al.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_at.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bb.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bj.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_br.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bz.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ch.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cp.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cx.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_df.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ae.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_am.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_au.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bc.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bk.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bs.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ca.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ci.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cq.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cy.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dg.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_af.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_an.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_av.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bd.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bl.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bt.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cb.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cj.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cr.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cz.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dh.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ag.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ao.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_aw.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_be.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bm.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bu.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cc.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ck.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cs.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_da.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_di.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ah.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ap.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ax.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bf.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bn.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_bv.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cd.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_cl.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_ct.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_db.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dj.csv.gz');
INSERT INTO trips FROM read_csv('https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_dn.csv.gz');
