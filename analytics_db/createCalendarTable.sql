create table calendar_table as
select x.dt, 
      EXTRACT(YEAR FROM x.dt) as year,
      EXTRACT(quarter FROM x.dt) as quarter,
      EXTRACT(month FROM x.dt) as month,
      EXTRACT(week FROM x.dt) as week,  /* WARNING! This follows odd ISO convention so may not be as expected */
      EXTRACT(day FROM x.dt) as day,
      EXTRACT(dow FROM x.dt) as dow,
      EXTRACT(doy FROM x.dt) as doy,
      (x.dt - DATE('1970-01-01') + 1) as odn,
      ((x.dt - DATE('1970-01-01') + 4) / 7)+1 as own
FROM (
    select (DATE('1970-01-01') + v) as dt
    from (
        select e.i * 10000 + d.i * 1000 + c.i * 100 + b.i * 10 + a.i as v
        from ints a, ints b, ints c, ints d, ints e
        order by 1
        )
    where v < 25567
) x