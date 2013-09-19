CREATE TABLE tweet WITH DESCRIPTION 'Tweets sampled via Streaming API'
ROW KEY FORMAT (userId LONG)
WITH
LOCALITY GROUP default
  WITH DESCRIPTION 'default locality group' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'Tweets' (
    tweet CLASS com.adamkunicki.avro.Tweet WITH DESCRIPTION 'Tweet'
  )
);
