select  
  subq_0.c4 as c0, 
  subq_0.c0 as c1, 
  subq_0.c3 as c2, 
  subq_0.c2 as c3, 
  subq_0.c5 as c4, 
  subq_0.c1 as c5
from 
  (select  
        ref_0.s_nationkey as c0, 
        cast(nullif(ref_0.s_address,
          ref_1.n_name) as VARCHAR) as c1, 
        ref_1.n_name as c2, 
        (select o_custkey from main.orders limit 1 offset 5)
           as c3, 
        ref_0.s_nationkey as c4, 
        ref_1.n_name as c5
      from 
        main.supplier as ref_0
          right join main.nation as ref_1
          on ((0) 
              and (ref_1.n_comment is not NULL))
      where ref_1.n_name is not NULL
      limit 35) as subq_0
where 1
