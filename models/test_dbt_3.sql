-- process_array.sql

-- Step 1: Unnest the array into individual rows
WITH unnested_data AS (
  SELECT
    id,
    unnest(profileList) AS array_item
  FROM
    "basic-profile".basic_profile."_airbyte_raw_userprofile"
    -- {{ source('my_source', 'my_table') }}
),

-- Step 2: Extract properties from the objects in the array
extracted_data AS (
  SELECT
    id,
    array_item ->> 'id' AS profileId,
    array_item ->> 'name' AS name
  FROM
    unnested_data
),

-- -- Step 3: Apply further transformations or aggregations as needed
-- transformed_data AS (
--   SELECT
--     id,
--     object_id,
--     UPPER(object_name) AS capitalized_name,
--     CASE
--       WHEN object_age > 18 THEN 'Adult'
--       ELSE 'Minor'
--     END AS age_group
--   FROM
--     extracted_data
-- )

-- Step 4: Output the final transformed data
SELECT *
FROM extracted_data
