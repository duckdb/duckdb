create table time_dim(
	t_time_sk integer not null,
	t_time_id varchar(16) not null,
	t_time integer,
	t_hour integer,
	t_minute integer,
	t_second integer,
	t_am_pm varchar(2),
	t_shift varchar(20),
	t_sub_shift varchar(20),
	t_meal_time varchar(20)
);
