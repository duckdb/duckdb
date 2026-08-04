with recursive aoc24_input(i) as (select '
x00: 1
x01: 1
x02: 1
y00: 0
y01: 1
y02: 0

x00 AND y00 -> z00
x01 XOR y01 -> z01
x02 OR y02 -> z02
'),
lines(y,line) as (
   select 0, substr(i,1,position(E'\n' in i)-1), substr(i,position(E'\n' in i)+1)
   from aoc24_input
   union all
   select y+1,substr(r,1,position(E'\n' in r)-1), substr(r,position(E'\n' in r)+1)
   from lines l(y,l,r) where position(E'\n' in r)>0
),
assignments(r,value) as (
   select substr(line, 1, position(': ' in line)-1) as r, substr(line, position(': ' in line)+2)::bigint as value from lines where line like '%: %'),
gates(in1,op,in2,out) as (
   select ll, substr(lr, 1, position(' ' in lr)-1), substr(lr, position(' ' in lr)+1), sr from (
   select substr(sl, 1, position(' ' in sl)-1) as ll, substr(sl, position(' ' in sl)+1) as lr, sr from (
   select substr(line, 1, position(' -> ' in line)-1) as sl, substr(line, position(' -> ' in line)+4) as sr from lines where line like '% -> %') s) s
),
part1raw(step, r,v) as (
   select 0, * from assignments
   union all
   (with tmp as (select * from part1raw),
    prop(step, r, v) as (
    select r1.step, g.out, case g.op when 'AND' then r1.v & r2.v when 'OR' then r1.v | r2.v when 'XOR' then xor(r1.v, r2.v) end
    from gates g, tmp r1, tmp r2
    where g.in1=r1.r and g.in2=r2.r),
    next(step,r ,v) as (
    select * from prop
    union all
    select * from tmp t where t.r not in (select r from prop))
    select step+1, r, v
    from next
    where exists(select * from next n left join tmp t on n.r=t.r where t.v is distinct from n.v))),
part1values(r,v) as (select r,v from part1raw where step=(select max(step) from part1raw)),
part1(v) as (select sum(case when r like 'z%' then v<<substr(r,2)::integer else 0 end) as v from part1values where r like 'z%'),
part2(step, digit, v1, v2) as (
   select 0, 1, g.out, '' from gates g where g.op = 'AND' and ((g.in1='x00' and g.in2='y00') or (g.in2='x00' and g.in1='y00'))
   union all (
   with tmp as (select * from part2),
      state(step, digit, x, y, z, last_carry) as (select step, digit, case when digit < 10 then 'x0'||digit else 'x'||digit end, case when digit < 10 then 'y0'||digit else 'y'||digit end, case when digit < 10 then 'z0'||digit else 'z'||digit end, v1 from tmp where v2='' limit 1),
      swaps(v1,v2) as (select v1, v2 from tmp where v2<>''),
      swaps2(v1,v2) as (select v1, v2 from swaps union all select v2, v1 from swaps),
      swappedgates(in1,op,in2,out) as (select in1, op, in2, coalesce(s.v2, g.out) from gates g left join swaps2 s on g.out=s.v1),
      and_gate(ag) as (select out from swappedgates g, state s where g.op='AND' and ((g.in1=s.x and g.in2=s.y) or (g.in2=s.x and g.in1=s.y))),
      xor_gate(xg) as (select out from swappedgates g, state s where g.op='XOR' and ((g.in1=s.x and g.in2=s.y) or (g.in2=s.x and g.in1=s.y))),
      setz_gate(r1,r2) as (select in1, in2 from swappedgates g, state s where g.out=s.z),
      swapforsetz(s1,s2) as (select r2, xg from setz_gate, xor_gate, state where r1=last_carry and r2<>xg union all select r1, xg from setz_gate, xor_gate, state where r2=last_carry and r1<>xg),
      z_gate(zg) as (select out from swappedgates g, state s, xor_gate where g.op='XOR' and ((g.in1=xg and g.in2=last_carry) or (g.in2=xg and g.in1=last_carry))),
      swapforz(s1,s2) as (select zg, z from z_gate, state where zg<>z),
      tg(tg) as (select out from swappedgates g, xor_gate, state where g.op='AND' and ((g.in1=xg and g.in2=last_carry) or (g.in2=xg and g.in1=last_carry))),
      next_carry(ng) as (select out from swappedgates g, tg, and_gate where g.op='OR' and ((g.in1=tg and g.in2=ag) or (g.in2=tg and g.in1=ag))),
      scenario as (select case when exists (select * from swapforsetz) then 0 when exists (select * from swapforz) then 1 else 2 end as sc),
      next(step, digit, v1, v2) as (
      select step+1, digit+case when sc=2 then 1 else 0 end, case when sc=2 then ng else last_carry end, '' from state left join next_carry on true, scenario
      union all
      select step+1, digit+case when sc=2 then 1 else 0 end, v1, v2 from swaps, state, scenario
      union all
      select step+1, digit, s1, s2 from swapforsetz, state, scenario where sc=0
      union all
      select step+1, digit, s1, s2 from swapforz, state, scenario where sc=1)
      select step, digit, v1, v2 from next where digit<(select count(*) from assignments where r like 'x%')
   )
),
swappedpart2(s) as (
   select v1 from part2 where v2<>'' and step = (select max(step) from part2)
   union all
   select v2 from part2 where v2<>'' and step = (select max(step) from part2)
)
select (select v from part1) as part1,
       (select string_agg(s,',' order by s) from swappedpart2) as part2
