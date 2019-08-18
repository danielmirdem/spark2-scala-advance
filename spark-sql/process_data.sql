CREATE EXTERNAL TABLE orders(
 order_id INT,
 order_date STRING,
 order_customer_id INT,
 order_status STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://user2emr/retail_db/orders';

CREATE EXTERNAL TABLE orders_count_by_status
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://user2emr/retail_db/orders_count_by_status'
AS
SELECT order_status, count(*) count_by_status FROM orders
GROUP BY order_status;

DROP TABLE orders;


