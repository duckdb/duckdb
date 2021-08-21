create table customer_address(
	ca_address_sk integer not null,
	ca_address_id varchar(16) not null,
	ca_street_number varchar(10),
	ca_street_name varchar(60),
	ca_street_type varchar(15),
	ca_suite_number varchar(10),
	ca_city varchar(60),
	ca_county varchar(30),
	ca_state varchar(2),
	ca_zip varchar(10),
	ca_country varchar(20),
	ca_gmt_offset decimal(5,2),
	ca_location_type varchar(20)
);
