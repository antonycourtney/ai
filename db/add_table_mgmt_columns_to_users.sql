alter table users
	add column created_base_tables timestamp with time zone,
	add column updated_derived_tables timestamp with time zone;
