with recursive aoc9_input(i) as (select '
2333133121414131402
'),
lines(y,line) as (
   select 0, substr(i,1,position(E'\n' in i)-1), substr(i,position(E'\n' in i)+1)
   from aoc9_input
   union all
   select y+1,substr(r,1,position(E'\n' in r)-1), substr(r,position(E'\n' in r)+1)
   from lines l(y,l,r) where position(E'\n' in r)>0
),
rawfield(x,y,v) as (
   select x::smallint,y::smallint,substr(line,x::integer,1)
   from (select * from lines l where line<>'') s, lateral generate_series(1,length(line)) g(x)
),
rawentries(id,v) as (
   select row_number() over (order by y,x) as id, v::bigint from rawfield
),
files(id, ofs, len) as (
   select id, ofs, len from (
   select id>>1 as id, sum(v) over (order by id) - v as ofs,v as len, id%2=1 as isfile from rawentries
   ) s where isfile and len>0
),
free(ofs) as (select f1ofs+f1len+x from (select f1.ofs as f1ofs, f1.len as f1len, f2.ofs as f2ofs from files f1, files f2 where f1.id+1=f2.id and f2.ofs>f1.ofs+f1.len) s, lateral generate_series(0,(f2ofs-(f1ofs+f1len)-1)::integer) x(x)),
part1 as (
    select id, (select min(ofs) from free) as ofs, len, ofs as oofs from files where id=(select max(id) from files)
    union all
    (with tmp as (select * from part1),
          nextfree as (select min(f.ofs) as nf from free f, tmp t where f.ofs>t.ofs),
          placenext as (select case when f.len=1 then f.id-1 else f.id end as id, case when f.len=1 then f2.len else f.len-1 end as len, case when f.len=1 then f2.ofs else oofs end as oofs from tmp f left join files f2 on f2.id=f.id-1 where f.id>1 or f.len>1)
     select id, case when nf is null or nf>np then np else nf end, len, oofs
     from (select *, oofs+len-1 as np from placenext) p, nextfree n)
),
part2placement as (
    select (select max(id) from files) as toplace, id, ofs, len from files
    union all
    (with tmp as (select * from part2placement),
       toplace as (select min(toplace) as cid from tmp),
       state as (select id as cid, ofs as cofs, len as clen from tmp, toplace where id=cid limit 1),
       gaps as (select ofs, len from (select prevend as ofs, ofs-prevend len, clen from (select coalesce(lag(ofs+len) over (order by ofs),0) as prevend, id, ofs, len, cid, clen from tmp, state where ofs<=cofs) s) s where len>=clen)
     select toplace-1, id, case when id<>toplace then ofs else coalesce((select min(ofs) from gaps),ofs) end, len from tmp where toplace>0
   )
),
part2(id, ofs) as (
   select id, ofs+x-1 from
   (select * from part2placement where toplace=0) s, lateral generate_series(1,len) x(x)
)
select (select sum(id*ofs) from part1) as part1,
       (select sum(id*ofs) from part2) as part2
