-- Create geolocation table
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_zip_code_prefix INTEGER PRIMARY KEY,
    geolocation_lat NUMERIC(10,8) NOT NULL,
    geolocation_lng NUMERIC(11,8) NOT NULL,
    geolocation_city VARCHAR(100) NOT NULL,
    geolocation_state CHAR(2) NOT NULL
);

-- Create customers table with foreign key relationship to geolocation
CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY,
    customer_unique_id UUID NOT NULL,
    customer_zip_code_prefix INTEGER NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state CHAR(2) NOT NULL,
    FOREIGN KEY (customer_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix)
);

CREATE INDEX idx_customers_zip_code ON customers(customer_zip_code_prefix);

-- Create orders table with a foreign key relationship to customers
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    order_status VARCHAR(30) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_purchase_date ON orders(order_purchase_timestamp);

-- Create order_reviews table with a foreign key relationship to orders
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    review_score SMALLINT NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE INDEX idx_order_reviews_order ON order_reviews(order_id);

-- Create order_payments table with a foreign key relationship to orders
CREATE TABLE IF NOT EXISTS order_payments (
    order_id UUID,
    payment_sequential SMALLINT,
    payment_type VARCHAR(30) NOT NULL,
    payment_installments SMALLINT NOT NULL,
    payment_value DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Create product_categories table
CREATE TABLE IF NOT EXISTS product_categories (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100) NOT NULL
);

-- Create products table with foreign key relationship to product_categories
CREATE TABLE IF NOT EXISTS products (
    product_id UUID PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length SMALLINT,
    product_description_length INTEGER,
    product_photos_qty SMALLINT,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    FOREIGN KEY (product_category_name) REFERENCES product_categories(product_category_name)
);

CREATE INDEX idx_products_category ON products(product_category_name);

-- Create sellers table with foreign key relationship to geolocation
CREATE TABLE IF NOT EXISTS sellers (
    seller_id UUID PRIMARY KEY,
    seller_zip_code_prefix INTEGER NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state CHAR(2) NOT NULL,
    FOREIGN KEY (seller_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix)
);

CREATE INDEX idx_sellers_zip_code ON sellers(seller_zip_code_prefix);

-- Create order_items table with foreign key relationships to orders, products, and sellers
CREATE TABLE IF NOT EXISTS order_items (
    order_id UUID,
    order_item_id SMALLINT,
    product_id UUID NOT NULL,
    seller_id UUID NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    freight_value DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_order_items_seller ON order_items(seller_id);