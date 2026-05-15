with recursive aoc11_input(i) as (select '0 89741 316108 7641 756 9 7832357 91'),
parts(i,r,rp) as (
   select '', i, position(' ' in i) from aoc11_input
   union all
   select i, r, position(' ' in r)
   from (select case when rp>0 then substr(r, 0, rp) else r end as i, case when rp>0 then substr(r, rp+1) else '' end as r
         from parts where r != '') s
),
startvalues(i) as (select i::bigint from parts where i!=''),
rounds(r,i,f) as (
   select 0, i, 1::bigint from startvalues
   union all
   select r+1, i, sum(f)::bigint from
      (with tmp as (select * from rounds where r<75)
       select r, case when i=0 then 1 when length(i::text)%2=0 then substr(i::text,1,length(i::text)>>1)::bigint else i*2024 end as i, f
       from tmp
       union all
       select r, substr(i::text,(length(i::text)>>1)+1)::bigint as i, f from tmp where length(i::text)%2=0) s
   group by r, i)
select (select sum(f) from rounds where r=25) as part1,
       (select sum(f) from rounds where r=75) as part2
;
