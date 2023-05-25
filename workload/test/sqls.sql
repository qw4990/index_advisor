select * from test.test_t1 where a in (1, 2, 3);
select * from test.test_t1 where a=1 and b=2 and c=3;
select * from test.test_t1 where a=1 and d<10;
select * from test.test_t2 where a>10;
select * from test.test_t2 where a>1 and b=2 and c=3;
select * from test.test_t2 where a>1 and d<10;
select * from test.test_t1, test.test_t2 where test_t1.a=test_t2.a and test_t1.b<10 and test_t2.b<10;
select * from test.test_t1, test.test_t2 where test_t1.a=test_t2.a and test_t1.d in (1, 2, 3) and test_t2.c=10;