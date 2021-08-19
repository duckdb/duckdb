create table household_demographics(
	hd_demo_sk integer not null,
	hd_income_band_sk integer,
	hd_buy_potential varchar(15),
	hd_dep_count integer,
	hd_vehicle_count integer
);
