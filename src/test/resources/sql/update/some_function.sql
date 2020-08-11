create or replace function test.someFunction() returns table test.a language sql as $$
select * from test.a where pk > 2;
$$;