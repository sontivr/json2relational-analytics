CREATE TABLE MYAPP.LINE_ITEMS 
(
  id             BIGINT NOT NULL,
  variant_id     BIGINT,
  quantity       INTEGER NOT NULL,
  product_id     BIGINT,
  order_id       BIGINT NOT NULL,
  order_number   BIGINT NOT NULL,
  user_id        BIGINT NOT NULL,
  email          VARCHAR(60),
  phone          VARCHAR(20),
  load_ts        timestamptz NOT NULL,
  PRIMARY KEY (id)
);
