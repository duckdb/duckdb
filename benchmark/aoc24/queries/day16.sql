with recursive aoc16_input(i) as (select '
###############
#.......#....E#
#.#.###.#.###.#
#.....#.#...#.#
#.###.#####.#.#
#.#.#.......#.#
#.#.#####.###.#
#...........#.#
###.#.#####.#.#
#...#.....#.#.#
#.#.#.###.#.#.#
#.....#...#.#.#
#.###.#.#.#.#.#
#S..#.....#...#
###############
'),
lines(y,line) as (
   select 0, substr(i,1,position(E'\n' in i)-1), substr(i,position(E'\n' in i)+1)
   from aoc16_input
   union all
   select y+1,substr(r,1,position(E'\n' in r)-1), substr(r,position(E'\n' in r)+1)
   from lines l(y,l,r) where position(E'\n' in r)>0
),
field(x,y,v) as (
   select x::smallint,y::smallint,substr(line,x::integer,1)
   from (select * from lines l where line<>'') s, lateral generate_series(1,length(line)) g(x)
),
walls(x,y) as (select x,y from field where v='#'),
startpos(x,y) as (select x,y from field where v='S' limit 1),
endpos(x,y) as (select x,y from field where v='E' limit 1),
directions(d,dx,dy) as (values(0,1::smallint,0::smallint),(1,0::smallint,1::smallint),(2,-1::smallint,0::smallint),(3,0,-1::smallint)),
turns(td,tc) as (values(0,0),(1,1000),(2,2000),(3,1000)),
part1steps(r,x,y,d,c) as (
   select 0,x,y,0,0 from startpos
   union all (
   with tmp(r,x,y,d,c) as (select * from part1steps),
      agg(r,x,y,d,c) as (select r,x,y,d,min(c) from (
         select r,x,y,(d+td)%4 as d,c+tc as c from tmp cross join turns
         ) s group by r,x,y,d)
      select r+1,(x+dx)::smallint,(y+dy)::smallint,d,c+1
      from agg a natural join directions where not exists(select * from walls w where a.x+dx=w.x and a.y+dy=w.y)
      and r<(select count(*) from field)
   )),
part1(x,y,d,c) as (select x,y,d,min(c) from (select x,y,(d+td)%4 as d, (c+tc) as c from part1steps cross join turns) group by x,y,d),
best1(c) as (select min(c) from part1 natural join endpos),
part2steps(x,y,d,c) as (
   select * from part1 natural join endpos where c=(select c from best1)
   union all
   (with tmp as (select distinct * from part2steps)
    select p.x, p.y, p.d, p.c from tmp t cross join turns, part1 p
    where p.x=t.x and p.y=t.y and p.d=(t.d+4-td)%4 and p.c=t.c-tc and td>0
    union all
    select p.x, p.y, p.d, p.c from tmp t natural join directions, part1 p
    where p.x=t.x-dx and p.y=t.y-dy and p.d=t.d and p.c=t.c-1)),
part2(x,y) as (select distinct x,y from part2steps)
select (select * from best1) as part1, (select count(*) from part2) as part2;
