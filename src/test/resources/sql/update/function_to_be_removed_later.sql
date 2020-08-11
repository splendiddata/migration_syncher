create function test.function_to_be_removed_later() returns text language sql as $$
select 'this function is supposed to be removed later on';
$$;