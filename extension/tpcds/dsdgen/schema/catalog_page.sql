create table catalog_page(
	cp_catalog_page_sk integer not null,
	cp_catalog_page_id varchar(16) not null,
	cp_start_date_sk integer,
	cp_end_date_sk integer,
	cp_department varchar(50),
	cp_catalog_number integer,
	cp_catalog_page_number integer,
	cp_description varchar(100),
	cp_type varchar(100)
);
