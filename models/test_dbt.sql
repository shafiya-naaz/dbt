with "basic-profile".basic_profile."userProfile" as (
    select
        case when _airbyte_data."createdAt" != '' then _airbyte_data."createdAt" end as createdat,
        case when  _airbyte_data."profileList.phone" != '' then _airbyte_data."phone" end as phone,
        case when _airbyte_data."id" != '' then _airbyte_data."id" end as id,
        case when _airbyte_data."_id" != '' then _airbyte_data."_id" end as _id,
        case when _airbyte_data."_class" != '' then _airbyte_data."_class" end as _class,
        case when _airbyte_data."userId" != '' then _airbyte_data."userId" end as userid,
        case when _airbyte_data."status" != '' then _airbyte_data."status" end as status,
        case when _airbyte_data."updatedAt" != '' then _airbyte_data."updatedAt" end as updatedat
    from "basic-profile".basic_profile._airbyte_raw_userprofile
    
)