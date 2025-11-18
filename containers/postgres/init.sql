CREATE SCHEMA retail;

CREATE TABLE retail.user_purchase (
    invoice_number varchar(10),
    stock_code varchar(20),
    detail varchar(1000),
    quantity int,
    invoice_date timestamp,
    unit_price Numeric(8,3),
    customer_id int,
    country varchar(20)
);

COPY retail.user_purchase(invoice_number,stock_code,detail,quantity,invoice_date,unit_price,customer_id,country) 
FROM '/input_data/OnlineRetail.csv' DELIMITER ','  CSV HEADER;

CREATE TABLE retail.movie_review(
	cid int,
	review_str varchar(10000)	
);

COPY retail.movie_review(cid, review_str) FROM '/input_data/movie_review.csv' DELIMITER ',' CSV HEADER;
