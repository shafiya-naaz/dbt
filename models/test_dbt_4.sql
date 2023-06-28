with parsed_data as (
    select 
    json_parse(json_serialize(_airbyte_data)) as new_data 
    from "basic-profile".basic_profile."_airbyte_raw_userprofile"
),
extracted_data as (
	select json_extract_path_text(new_data, 'profileList', true) as profiles from parsed_data
)
select * from extracted_data