CREATE EXTERNAL TABLE IF NOT EXISTS TABLE NAME (
  order_id int,
  item string,
  price double,
  timestamp double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://your-bucket-name/output/';
