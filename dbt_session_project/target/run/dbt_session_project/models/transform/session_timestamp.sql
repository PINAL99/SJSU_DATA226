
  create or replace   view USER_DB_DRAGON.PUBLIC.session_timestamp
  
  
  
  
  as (
    with source_data as (

    select
        sessionId,
        session_start,
        session_end,
        datediff('minute', session_start, session_end) as session_length_minutes
    from raw.user_sessions

)

select *
from source_data
  );

