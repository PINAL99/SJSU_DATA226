select
  sessionId,
  ts
from USER_DB_DRAGON.RAW.SESSION_TIMESTAMP
where sessionId is not null