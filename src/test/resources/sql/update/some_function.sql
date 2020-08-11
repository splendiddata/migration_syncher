create or replace function test.someFunction() returns table of test.a language sql as $$
select * from test.a where pk > 2;
$$;