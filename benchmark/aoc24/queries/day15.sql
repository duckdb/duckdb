with recursive aoc15_input(i) as (select '
##########
#..O..O.O#
#......O.#
#.OO..O.O#
#..O@..O.#
#O#..O...#
#O..O..O.#
#.OO.O.OO#
#....O...#
##########

<vv>^<v^>v>^vv^v>v<>v^v<v<^vv<<<^><<><>>v<vvv<>^v^>^<<<><<v<<<v^vv^v>^
vvv<<^>^v^^><<>>><>^<<><^vv^^<>vvv<>><^^v>^>vv<>v<<<<v<^v>^<^^>>>^<v<v
><>vv>v^v^<>><>>>><^^>vv>v<^^^>>v^v^<^^>v^^>v^<^v>v<>>v^v^<v>v^^<^^vv<
<<v<^>>^^^^>>>v^<>vvv^><v<<<>^^^vv^<vvv>^>v<^^^^v<>^>vvvv><>>v^<<^^^^^
^><^><>>><>^^<<^^v>>><^<v>^<vv>>v>>>^v><>^v><<<<v>>v<v<v>vvv>^<><<>^><
^>><>^v<><^vvv<^^<><v<<<<<><^v<<<><<<^^<v<^^^><^>>^<v^><<<^>>^v<v^v<v^
>^>>^v>vv>^<<^v<>><<><<v<<v><>v<^vv<<<>^^v^>^^>>><<^v>>v^v><^^>>^<>vv^
<><^^>^^^<><vvvvv^v<v<<>^v<v>v<<^><<><<><<<^^<<<^<<>><<><^^^>^^<>^>v<>
^^>vv<^v^v<vv>^<><v<^v>^^^>>>^^vvv^>vvv<>>>^<^>>>>>^<<^v>^vvv<>^<><<v>
v^^>>><<^^<>>^v^<v^vv<>v^<<>^<^v^v><^<<<><<^<v><v<>vv>>v><v^<vv<>v^<<^
'),
lines(y,line,r,u) as (
   select 0, substr(i,1,position(E'\n' in i)-1), substr(i,position(E'\n' in i)+1),true
   from aoc15_input
   union all
   select y+1,substr(r,1,position(E'\n' in r)-1), substr(r,position(E'\n' in r)+1),u and position(E'\n' in r)>1
   from lines l(y,l,r,u) where position(E'\n' in r)>0
),
field(x,y,v) as (
   select x::smallint-1,y::smallint-1,substr(line,x::integer,1)
   from (select * from lines l where u and line<>''), lateral generate_series(1,length(line)) g(x)
),
walls(x,y) as (select x,y from field where v = '#'),
robots(x,y) as (select x,y from field where v = 'O'),
startpos(x,y) as (select x,y from field where v='@' limit 1),
rawmovement(y,s,dx,dy) as (
   select l.y, x, case substr(line,x::integer,1) when '>' then 1 when '<' then -1 else 0 end, case substr(line,x::integer,1) when 'v' then 1 when '^' then -1 else 0 end
   from (select * from lines l where not u and line<>'') l, lateral generate_series(1,length(line)) g(x)
   where substr(line,x::integer,1) in ('>','v','<','^')),
movement(s,dx,dy) as (select rank() over (order by y,s), dx, dy from rawmovement),
simulation(r,rx,ry,px,py) as (
   select 0,r.x,r.y,p.x,p.y from robots r, startpos p
   union all
   (with recursive tmp as (select * from simulation),
       state(px,py,dx,dy) as (select px,py,dx,dy from (select * from tmp limit 1) join movement on r+1=s limit 1),
       shift(x,y) as (
          select px+dx,py+dy from state
          union all
          select x+dx,y+dy from shift cross join state where exists (select * from tmp r where x=r.rx and y=r.ry)),
       canmove(f) as (select not exists(select * from walls f, shift s where f.x=s.x and f.y=s.y))
       select r+1,case when sf then rx+dx else rx end, case when sf then ry+dy else ry end, case when f then px+dx else px end,case when f then py+dy else py end
       from (select t.*, dx, dy, f, f and exists(select * from shift s where t.rx=s.x and t.ry=s.y) as sf from tmp t cross join state cross join canmove) s
   )),
walls2(x,y) as (select 2*x,y from walls union all select 2*x+1,y from walls),
robots2(x,y) as (select 2*x,y from robots),
simulation2(r,rx,ry,px,py) as (
   select 0,r.x,r.y,2*p.x,p.y from robots2 r, startpos p
   union all
   (with recursive tmp as (select * from simulation2),
       state(px,py,dx,dy) as (select px,py,dx,dy from (select * from tmp limit 1) join movement on r+1=s limit 1),
       shift(x,y) as (
          select px+dx,py+dy from state
          union
          (with tmp2 as (select * from shift)
          select x+2,y from tmp2 cross join (select * from state where dx=1 and dy=0) where exists (select * from tmp r where x=r.rx and y=r.ry)
          union all
          select x-2,y from tmp2 cross join (select * from state where dx=-1 and dy=0) where exists (select * from tmp r where x=r.rx+1 and y=r.ry)
          union all
          select x,y+1 from tmp2 cross join (select * from state where dx=0 and dy=1) where exists (select * from tmp r where x=r.rx and y=r.ry)
          union all
          select x+1,y+1 from tmp2 cross join (select * from state where dx=0 and dy=1) where exists (select * from tmp r where x=r.rx and y=r.ry)
          union all
          select x-1,y+1 from tmp2 cross join (select * from state where dx=0 and dy=1) where exists (select * from tmp r where x=r.rx+1 and y=r.ry)
          union all
          select x,y+1 from tmp2 cross join (select * from state where dx=0 and dy=1) where exists (select * from tmp r where x=r.rx+1 and y=r.ry)
          union all
          select x,y-1 from tmp2 cross join (select * from state where dx=0 and dy=-1) where exists (select * from tmp r where x=r.rx and y=r.ry)
          union all
          select x+1,y-1 from tmp2 cross join (select * from state where dx=0 and dy=-1) where exists (select * from tmp r where x=r.rx and y=r.ry)
          union all
          select x-1,y-1 from tmp2 cross join (select * from state where dx=0 and dy=-1) where exists (select * from tmp r where x=r.rx+1 and y=r.ry)
          union all
          select x,y-1 from tmp2 cross join (select * from state where dx=0 and dy=-1) where exists (select * from tmp r where x=r.rx+1 and y=r.ry)
          )),
       canmove(f) as (select not exists(select * from walls2 f, shift s where f.x=s.x and f.y=s.y))
       select r+1,case when sf then rx+dx else rx end, case when sf then ry+dy else ry end, case when f then px+dx else px end,case when f then py+dy else py end
       from (select t.*, dx, dy, f, f and exists(select * from shift s where (t.rx=s.x or t.rx+1=s.x) and t.ry=s.y) as sf from tmp t cross join state cross join canmove) s
   ))
select (select sum(100*ry+rx) from simulation where r=(select max(s) from movement)) as part1,
       (select sum(100*ry+rx) from simulation2 where r=(select max(s) from movement)) as part2
