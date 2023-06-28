WITH unnested_data AS (
  SELECT
    id,
    unnest(profileList) AS array_item
  FROM
    "basic-profile".basic_profile."_airbyte_raw_userprofile"
),

extracted_data AS (
  SELECT
    id,
    array_item ->> 'id' AS profileId,
    array_item ->> 'name' AS name
  FROM
    unnested_data
)
SELECT *
FROM extracted_data
