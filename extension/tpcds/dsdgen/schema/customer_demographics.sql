create table customer_demographics(
	cd_demo_sk integer not null,
	cd_gender varchar(1),
	cd_marital_status varchar(1),
	cd_education_status varchar(20),
	cd_purchase_estimate integer,
	cd_credit_rating varchar(10),
	cd_dep_count integer,
	cd_dep_employed_count integer,
	cd_dep_college_count integer
);
