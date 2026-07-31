with recursive aoc22_input(i) as (select '
1
10
100
2024
'),
lines(y,line) as (
   select 0, substr(i,1,position(E'\n' in i)-1), substr(i,position(E'\n' in i)+1)
   from aoc22_input
   union all
   select y+1,substr(r,1,position(E'\n' in r)-1), substr(r,position(E'\n' in r)+1)
   from lines l(y,l,r) where position(E'\n' in r)>0
),
secrets(y,v) as (select y, line::bigint from lines where line<>''),
sequences(y,step,v) as (
   select y,0,v from secrets
   union all
   select y,step+1, xor(n * 2048, n) % 16777216 from (
      select y, step, xor(n // 32, n) % 16777216 as n from (
      select y, step, xor(v * 64, v) % 16777216 as n
      from sequences where step<2000) s) s),
rawgains(y,step,enc, v) as (
   select y, step, (v1+10-v0)+((v2+10-v1)<<5)+((v3+10-v2)<<10)+((v4+10-v3)<<15), v4 from (
   select y, step, v%10 as v0, lead(v%10, 1) over w as v1, lead(v%10, 2) over w as v2, lead(v%10, 3) over w as v3, lead(v%10, 4) over w as v4
   from sequences where step<2000-4 window w as (partition by y order by step)) s
),
gains(y,enc,v) as (select y, enc, v from rawgains g1 where not exists (select * from rawgains g2 where g1.y=g2.y and g1.enc=g2.enc and g2.step<g1.step)),
gainsperenc(enc,v) as (select enc, sum(v) from gains group by enc)
select (select sum(v) from sequences where step = 2000) as part1,
       (select max(v) from gainsperenc) as part2
