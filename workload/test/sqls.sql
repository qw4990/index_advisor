select * from test_t1 where a in (1, 2, 3);
select * from test_t1 where a=1 and b=2 and c=3;
select * from test_t1 where a=1 and d<10;
select * from test_t2 where a>10;
select * from test_t2 where a>1 and b=2 and c=3;
select * from test_t2 where a>1 and d<10;
select * from test_t1, test_t2 where t1.a=t2.a and t1.b<10 and t2.b<10;
select * from test_t1, test_t2 where t1.a=t2.a and t1.d in (1, 2, 3) and t2.c=10;