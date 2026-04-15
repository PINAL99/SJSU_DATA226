select
  userId,
  sessionId,
  channel
from USER_DB_DRAGON.RAW.USER_SESSION_CHANNEL
where sessionId is not null