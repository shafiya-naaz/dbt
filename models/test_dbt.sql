

create table basic_profile."userProfileFinal" compound sortkey(_airbyte_emitted_at)

as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "basic-profile".basic_profile._airbyte_raw_userprofile
select
        case when _airbyte_data."createdAt" != '' then _airbyte_data."createdAt" end as createdAt,
        case when  _airbyte_data."profileList.id" != '' then _airbyte_data."profileId" end as profileId,
        case when  _airbyte_data."profileList.phone" != '' then _airbyte_data."phone" end as phone,
        case when  _airbyte_data."profileList.name" != '' then _airbyte_data."name" end as profileName,
        case when  _airbyte_data."profileList.email" !='' then _airbyte_data."email" end as email,
        case when  _airbyte_data."profileList.patientProfile.preferredCurrency" !='' then _airbyte_data."preferredCurrency" end as preferredCurrency,
        -- case when  _airbyte_data."profileList".roles != '' then _airbyte_data."roles" end as roles,
        case when _airbyte_data."id" != '' then _airbyte_data."id" end as id,
        case when _airbyte_data."_id" != '' then _airbyte_data."_id" end as _id,
        case when _airbyte_data."_class" != '' then _airbyte_data."_class" end as _class,
        case when _airbyte_data."userId" != '' then _airbyte_data."userId" end as userid,
        case when _airbyte_data."status" != '' then _airbyte_data."status" end as status,
        case when _airbyte_data."updatedAt" != '' then _airbyte_data."updatedAt" end as updatedAt,
        _airbyte_ab_id,
    	_airbyte_emitted_at,
    	getdate() as _airbyte_normalized_at
from basic_profile."_airbyte_raw_userprofile"
  );