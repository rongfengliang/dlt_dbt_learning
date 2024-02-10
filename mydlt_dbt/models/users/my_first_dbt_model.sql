{{ config(materialized='table') }}

with users as (
    select * from postgres_data.users
)

select *
from users
