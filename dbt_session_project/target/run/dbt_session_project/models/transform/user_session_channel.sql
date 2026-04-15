
  create or replace   view USER_DB_DRAGON.PUBLIC.user_session_channel
  
  
  
  
  as (
    with source_data as (

    select
        sessionId,
        userId,
        channel
    from raw.user_sessions

)

select *
from source_data
  );

