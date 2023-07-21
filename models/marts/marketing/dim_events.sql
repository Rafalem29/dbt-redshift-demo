{{ config(materialized='table', sort='event_id', dist='event_id') }}

WITH categories AS (

    select * from {{ ref('stg_tickit__categories') }}

),

events AS (

    select * from {{ ref('stg_tickit__events') }}

),

venues AS (

    select * from {{ ref('stg_tickit__venues') }}

),

dates AS (

    select * from {{ ref('stg_tickit__dates') }}

),

final 
    AS (
    SELECT 
        e.event_id,
        e.event_name,
        e.start_time,
        v.venue_name,
        v.venue_city,
        v.venue_state,
        v.venue_seats,
        c.cat_group,
        c.cat_name,
        c.cat_desc,
        d.week,
        d.qtr,
        d.holiday

    FROM 
        events e
    INNER JOIN 
        categories c 
            ON e.cat_id = c.cat_id
    INNER JOIN 
        venues v 
            ON e.venue_id = v.venue_id
    INNER JOIN 
        dates d 
            ON e.date_id = d.date_id

    WHERE 
        1=1 

)

SELECT 
    * 
    
FROM 
    final

WHERE   
    1=1 