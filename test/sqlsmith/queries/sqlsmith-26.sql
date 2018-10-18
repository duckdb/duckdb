select ref_0.r_regionkey as c0, ref_0.r_name as c1, cast(nullif(ref_0.r_comment, ref_0.r_name) as VARCHAR) as c2 from main.region as ref_0 where ref_0.r_name is NULL limit 53 
