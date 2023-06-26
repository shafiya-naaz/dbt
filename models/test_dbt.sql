-- select
--     a.name,
--     b.base_stat status_value,
--     c.name status_name
-- from {{ ref('poke_pokemon') }} a
-- left join {{ ref('poke_pokemon_stats') }} b
--   on a._airbyte_poke_pokemon_hashid = b._airbyte_poke_pokemon_hashid
-- left join {{ ref('poke_pokemon_stats_stat') }} c
--    on b._airbyte_stats_hashid = c._airbyte_stats_hashid
-- where c.name = 'attack'
-- order by status_value desc

-- create table "basic_profile".
-- select case when 

-- {{ config(materialized='table') }}

-- SELECT e.emp_no,
--        e.first_name,
--        e.last_name,
--        s.salary
-- FROM dbt_database.employees e
--          JOIN dbt_database.salaries s ON e.emp_no = s.emp_no
-- select
--     case when _airbyte_data."createdAt" != '' then _airbyte_data."createdAt" end as createdat,

-- create table sales(
-- salesid integer not null,
-- listid integer not null,
-- sellerid integer not null,
-- buyerid integer not null,
-- eventid integer not null encode mostly16,
-- dateid smallint not null,
-- qtysold smallint not null encode mostly8,
-- pricepaid decimal(8,2) encode delta32k,
-- commission decimal(8,2) encode delta32k,
-- saletime timestamp,
-- primary key(salesid),
-- foreign key(listid) references listing(listid),
-- foreign key(sellerid) references users(userid),
-- foreign key(buyerid) references users(userid),
-- foreign key(dateid) references date(dateid))
-- distkey(listid)
-- compound sortkey(listid,sellerid);

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

-- with customers as (

--     select
--         id as customer_id,
--         first_name,
--         last_name

--     from jaffle_shop.customers

-- ),

-- orders as (

--     select
--         id as order_id,
--         user_id as customer_id,
--         order_date,
--         status

--     from jaffle_shop.orders

-- ),

-- customer_orders as (

--     select
--         customer_id,

--         min(order_date) as first_order_date,
--         max(order_date) as most_recent_order_date,
--         count(order_id) as number_of_orders

--     from orders

--     group by 1

-- ),

-- final as (

--     select
--         customers.customer_id,
--         customers.first_name,
--         customers.last_name,
--         customer_orders.first_order_date,
--         customer_orders.most_recent_order_date,
--         coalesce(customer_orders.number_of_orders, 0) as number_of_orders

--     from customers

--     left join customer_orders using (customer_id)

-- )

-- select * from final