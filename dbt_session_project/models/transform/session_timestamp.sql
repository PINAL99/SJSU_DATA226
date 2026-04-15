select
  sessionId,
  ts
from {{ source('raw', 'session_timestamp') }}
where sessionId is not null
