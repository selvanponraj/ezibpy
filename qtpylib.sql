select * from symbols;
select * from bars where symbol_id=1;
select * from ticks where symbol_id=1;
select * from bars where symbol_id=1;
select * from trades;


drop table trades;
drop table greeks;
drop table bars;
drop table ticks;
drop table symbols;

SELECT t.id,t.datetime,s.symbol,t.last,t.ask,t.bid
FROM ticks t, symbols s
WHERE
t.id IN (SELECT MAX(t1.id) FROM ticks t1 GROUP BY t1.symbol_id)
and t.symbol_id in (10,11,13,14)
and s.id = symbol_id
order by symbol_id