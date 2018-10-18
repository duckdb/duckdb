insert into main.partsupp values ( 32, case when ((select c_name from main.customer limit 1 offset 5) is not NULL) or (71 is not NULL) then 83 else 83 end , 52, cast(null as DECIMAL), default) 
