with recursive aoc17_input(i) as (select '
Register A: 2024
Register B: 0
Register C: 0

Program: 0,3,5,4,3,0
'),
lines(y,line) as (
   select 0, substr(i,1,position(E'\n' in i)-1), substr(i,position(E'\n' in i)+1)
   from aoc17_input
   union all
   select y+1,substr(r,1,position(E'\n' in r)-1), substr(r,position(E'\n' in r)+1)
   from lines l(y,l,r) where position(E'\n' in r)>0
),
splitlines(y,head, value) as (select y, substr(line, 1, position(': ' in line)-1) as head, substr(line, position(': ' in line)+2) as value from lines where line like '%: %'),
state(a,b,c) as (
   select a.value::bigint, b.value::bigint, c.value::bigint
   from splitlines a, splitlines b, splitlines c
   where a.head = 'Register A' and b.head = 'Register B' and c.head = 'Register C'
   limit 1),
rawprog(step, value) as (
   select 0 as step, substr(value, 1, position(',' in value)-1)::bigint as value, substr(value, position(',' in value)+1) as tail
   from splitlines where head = 'Program'
   union all
   select step+1, case when tail like '%,%' then substr(tail, 1, position(',' in tail)-1)::bigint else tail::bigint end, case when tail like '%,%' then substr(tail, position(',' in tail)+1) else '' end
   from rawprog where tail <> ''),
program(pos, op, arg) as (
   select p1.step, p1.value, p2.value
   from rawprog p1 left join rawprog p2 on p1.step+1=p2.step),
simulation(step,ip,a,b,c,out) as (
   select 0 as step,0::bigint as ip,a,b,c,'' as out from state
   union all
   select step+1,
          case when op=3 and a<>0 then arg else ip end,
          case op when 0 then case when combo not between 0 and 63 then 0 else a>>combo::integer end else a end,
          case op when 1 then xor(b, arg) when 2 then combo & 7 when 4 then xor(b, c) when 6 then case when combo not between 0 and 63 then 0 else a>>combo::integer end else b end,
          case op when 7 then case when combo not between 0 and 63 then 0 else a>>combo::integer end else c end,
          case op when 5 then case when out<>'' then out||','||(combo&7) else (combo&7)::text end else out end
   from (select step, ip+2 as ip, a, b, c, out, op, arg, case arg when 0 then 0 when 1 then 1 when 2 then 2 when 3 then 3 when 4 then a when 5 then b when 6 then c else 0 end::bigint as combo
         from simulation join program on ip=pos)
),
part1(out) as (select out from simulation where step=(select max(step) from simulation)),
part2(v, remaining) as (
   select 0::bigint as v, (select count(*) from program) as remaining
   union all
   (with recursive tmp as (select * from part2),
   rem(remaining) as (select min(remaining) from tmp),
   simulation(step,seed,ip,a,b,c,out) as (
   select 0 as step,8*v+t as v,0::bigint as ip,8*v+t as a,b,c,'' as out from state,tmp, generate_series(0,7) g(t) where remaining>0
   union all
   select step+1, seed,
          case when op=3 and a<>0 then arg else ip end,
          case op when 0 then case when combo not between 0 and 63 then 0 else a>>combo::integer end else a end,
          case op when 1 then xor(b, arg) when 2 then combo & 7 when 4 then xor(b, c) when 6 then case when combo not between 0 and 63 then 0 else a>>combo::integer end else b end,
          case op when 7 then case when combo not between 0 and 63 then 0 else a>>combo::integer end else c end,
          case op when 5 then case when out<>'' then out||','||(combo&7) else (combo&7)::text end else out end
   from (select step, seed,ip+2 as ip, a, b, c, out, op, arg, case arg when 0 then 0 when 1 then 1 when 2 then 2 when 3 then 3 when 4 then a when 5 then b when 6 then c else 0 end::bigint as combo
         from simulation join program on ip=pos)),
   simulationend(seed,out) as (select seed, out from simulation s1 where step=(select max(s2.step) from simulation s2 where s2.seed=s1.seed)),
   expected(out) as (select string_agg(op::text, ',' order by pos) as out from program, rem where pos>=remaining-1)
   select seed, remaining-1 from simulationend s, expected e, rem where remaining>0 and s.out=e.out
   )
)
select (select out from part1) as part1,
       (select min(v) from part2 where remaining=0) as part2
