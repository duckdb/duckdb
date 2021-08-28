create table web_page(
	wp_web_page_sk integer not null,
	wp_web_page_id varchar(16) not null,
	wp_rec_start_date date , wp_rec_end_date date , wp_creation_date_sk integer,
	wp_access_date_sk integer,
	wp_autogen_flag varchar(1),
	wp_customer_sk integer,
	wp_url varchar(100),
	wp_type varchar(50),
	wp_char_count integer,
	wp_link_count integer,
	wp_image_count integer,
	wp_max_ad_count integer
);
