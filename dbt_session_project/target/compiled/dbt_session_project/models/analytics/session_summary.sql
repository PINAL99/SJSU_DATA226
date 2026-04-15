with __dbt__cte__user_session_channel as (
select
  userId,
  sessionId,
  channel
from USER_DB_DRAGON.RAW.USER_SESSION_CHANNEL
where sessionId is not null
),  __dbt__cte__session_timestamp as (
select
  sessionId,
  ts
from USER_DB_DRAGON.RAW.SESSION_TIMESTAMP
where sessionId is not null
) select
  u.userId,
  u.sessionId,
  u.channel,
  st.ts
from __dbt__cte__user_session_channel u
join __dbt__cte__session_timestamp st
  on u.sessionId = st.sessionId