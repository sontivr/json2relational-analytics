CREATE TABLE myapp.user_summary 
(
  user_id                  BIGINT NOT NULL,
  total_quantity           BIGINT NOT NULL,
  total_price              BIGINT NOT NULL,
  total_discounts          BIGINT NOT NULL,
  total_line_items_price   BIGINT NOT NULL,
  total_price_usd          BIGINT NOT NULL,
  PRIMARY KEY (user_id)
);
