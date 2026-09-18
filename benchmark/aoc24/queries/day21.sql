with recursive aoc21_input(i) as (select '
973A
836A
780A
985A
413A
'),
lines(y,line) as (
   select 0, substr(i,1,position(E'\n' in i)-1), substr(i,position(E'\n' in i)+1)
   from aoc21_input
   union all
   select y+1,substr(r,1,position(E'\n' in r)-1), substr(r,position(E'\n' in r)+1)
   from lines l(y,l,r) where position(E'\n' in r)>0
),
numvalues(y,v) as (select y, substr(line,1,position('A' in line)-1)::bigint from lines where line like '%A'),
charvalues(c,v) as (select i::text, i from generate_series(0,9) i(i) union all select 'A', 10),
charpairs(y,d1,d2) as (
   select y, case when i>1 then substr(line, i::integer-1, 1) else 'A' end, substr(line, i::integer, 1) from (select * from lines where line like '%A') s, lateral generate_series(1,length(line)) i(i)
),
pairs(y,d1,d2) as (
   select y, c1.v, c2.v from charpairs, charvalues c1, charvalues c2
   where d1=c1.c and d2=c2.c
),
neighbors_movement(o,d,n) as (values(0,2,1), (0,3,4), (1,0,0), (1,2,2), (1,3,3), (2,0,1), (3,0,4), (3,1,1), (4,1,0), (4,2,3)),
neighbors_digits(o,d,n) as (values(0,0,10), (0,3,2), (1,0,2), (1,3,4), (2,0,3), (2,1,0), (2,2,1), (2,3,5), (3,1,10), (3,2,2), (3,3,6), (4,0,5), (4,1,1), (4,3,7), (5,0,6), (5,1,2), (5,2,4), (5,3,8), (6,1,3), (6,2,5), (6,3,9), (7,0,8), (7,1,4), (8,0,9), (8,1,5), (8,2,7), (9,1,6), (9,2,8), (10,2,0), (10,3,3)),
costs(step,d,last,c) as (
   select 0, d, l, 1::bigint from generate_series(0,4) d(d), generate_series(0,4) l(l)
   union all
   (with prev as (select * from costs)
      select (select min(step) from prev)+1, "at", start, c
         from (
         with recursive flood(r, start, "at", last, costs) as (
            select 0, i, i, 4, 0::bigint from generate_series(0,4) i(i)
            union all
            (with tmp as (select * from flood)
             select r+1, start, "at", last, min(costs)
             from ((select * from tmp) union all
             (select r, start, n.n, n.d, t.costs+p.c
             from tmp t, neighbors_movement n, prev p
             where p.last=t.last and n.d=p.d and n.o=t.at))
             group by r, start, "at", last having r<6))
         select start, "at", min(c) as c
         from (select start, "at", f.costs+p.c as c
         from flood f, prev p where f.last=p.last and p.d=4) s
         group by start, "at") s where (select min(step) from prev)<26)
),
rawdistances(rd, step, start, "at", last, c) as (
   select 0, rl, i, i, 4, 0::bigint from (values(2),(25)) rl(rl), generate_series(0,10) i(i)
   union
   (with tmp as (select * from rawdistances)
    select rd+1, step, start, "at", last, min(c) from
    (select rd, step, start, "at", last, c from tmp
    union all
    select rd, t.step, start, n.n, n.d, t.c+c.c
    from tmp t, neighbors_digits n, costs c
    where c.step=t.step and c.last=t.last and n.o=t.at and n.d=c.d) s
    group by rd, step, start, "at", last having rd < 20)
),
distances(step, d1, d2, c) as (
   select step, start, "at", min(c)
   from (select r.step, start, "at", r.c+c.c as c
         from rawdistances r, costs c
         where r.step=c.step and r.last=c.last and c.d=4) s
   group by step, start, "at"
),
minlen(y,step,len) as (
   select y, step, sum(c)
   from pairs p, distances d
   where p.d1=d.d1 and p.d2=d.d2
   group by y, step
),
answers(step, a) as (
   select step, sum(n.v*m.len) from numvalues n, minlen m where n.y=m.y group by step
)
select (select a from answers where step=2) as part1,
       (select a from answers where step=25) as part2
