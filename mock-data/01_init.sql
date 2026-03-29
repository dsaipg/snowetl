-- Mock source database with realistic tables for demo

-- Orders table
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    order_date DATE NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Customers table
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    country VARCHAR(100),
    tier VARCHAR(50) DEFAULT 'standard',
    updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    stock_count INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Transactions table
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    payment_method VARCHAR(50),
    status VARCHAR(50) DEFAULT 'pending',
    processed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert mock data - customers
INSERT INTO customers (name, email, country, tier, updated_at) VALUES
('Alice Johnson', 'alice@example.com', 'USA', 'premium', NOW() - INTERVAL '1 day'),
('Bob Smith', 'bob@example.com', 'UK', 'standard', NOW() - INTERVAL '2 days'),
('Carol White', 'carol@example.com', 'Canada', 'premium', NOW() - INTERVAL '3 days'),
('David Brown', 'david@example.com', 'Australia', 'standard', NOW() - INTERVAL '1 day'),
('Eva Martinez', 'eva@example.com', 'Spain', 'enterprise', NOW() - INTERVAL '4 days'),
('Frank Lee', 'frank@example.com', 'Singapore', 'standard', NOW() - INTERVAL '2 days'),
('Grace Kim', 'grace@example.com', 'South Korea', 'premium', NOW() - INTERVAL '1 day'),
('Henry Wilson', 'henry@example.com', 'USA', 'enterprise', NOW() - INTERVAL '5 days'),
('Iris Chen', 'iris@example.com', 'China', 'standard', NOW() - INTERVAL '3 days'),
('Jack Taylor', 'jack@example.com', 'USA', 'premium', NOW() - INTERVAL '1 day');

-- Insert mock data - products
INSERT INTO products (name, category, price, stock_count, updated_at) VALUES
('Laptop Pro 15', 'Electronics', 1299.99, 150, NOW() - INTERVAL '2 days'),
('Wireless Mouse', 'Electronics', 49.99, 500, NOW() - INTERVAL '1 day'),
('Standing Desk', 'Furniture', 599.99, 75, NOW() - INTERVAL '3 days'),
('Office Chair', 'Furniture', 399.99, 100, NOW() - INTERVAL '2 days'),
('Monitor 27"', 'Electronics', 449.99, 200, NOW() - INTERVAL '1 day'),
('Keyboard Mech', 'Electronics', 129.99, 300, NOW() - INTERVAL '4 days'),
('Webcam HD', 'Electronics', 89.99, 250, NOW() - INTERVAL '2 days'),
('Desk Lamp', 'Furniture', 79.99, 400, NOW() - INTERVAL '1 day'),
('Headphones', 'Electronics', 199.99, 175, NOW() - INTERVAL '3 days'),
('USB Hub', 'Electronics', 39.99, 600, NOW() - INTERVAL '1 day');

-- Insert mock data - orders
INSERT INTO orders (customer_id, product_id, amount, status, order_date, updated_at) VALUES
(1, 1, 1299.99, 'completed', NOW() - INTERVAL '5 days', NOW() - INTERVAL '1 day'),
(2, 3, 599.99, 'completed', NOW() - INTERVAL '4 days', NOW() - INTERVAL '2 days'),
(3, 5, 449.99, 'processing', NOW() - INTERVAL '3 days', NOW() - INTERVAL '1 day'),
(4, 2, 49.99, 'completed', NOW() - INTERVAL '6 days', NOW() - INTERVAL '3 days'),
(5, 9, 199.99, 'completed', NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day'),
(6, 6, 129.99, 'pending', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
(7, 4, 399.99, 'completed', NOW() - INTERVAL '7 days', NOW() - INTERVAL '4 days'),
(8, 1, 1299.99, 'processing', NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day'),
(9, 10, 39.99, 'completed', NOW() - INTERVAL '3 days', NOW() - INTERVAL '2 days'),
(10, 7, 89.99, 'completed', NOW() - INTERVAL '4 days', NOW() - INTERVAL '1 day'),
(1, 5, 449.99, 'completed', NOW() - INTERVAL '8 days', NOW() - INTERVAL '5 days'),
(3, 2, 49.99, 'completed', NOW() - INTERVAL '9 days', NOW() - INTERVAL '6 days'),
(5, 8, 79.99, 'pending', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
(7, 6, 129.99, 'completed', NOW() - INTERVAL '5 days', NOW() - INTERVAL '3 days'),
(9, 3, 599.99, 'processing', NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day');

-- Insert mock data - transactions
INSERT INTO transactions (order_id, amount, payment_method, status, processed_at, updated_at) VALUES
(1, 1299.99, 'credit_card', 'completed', NOW() - INTERVAL '5 days', NOW() - INTERVAL '1 day'),
(2, 599.99, 'paypal', 'completed', NOW() - INTERVAL '4 days', NOW() - INTERVAL '2 days'),
(4, 49.99, 'credit_card', 'completed', NOW() - INTERVAL '6 days', NOW() - INTERVAL '3 days'),
(5, 199.99, 'bank_transfer', 'completed', NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day'),
(7, 399.99, 'credit_card', 'completed', NOW() - INTERVAL '7 days', NOW() - INTERVAL '4 days'),
(9, 39.99, 'paypal', 'completed', NOW() - INTERVAL '3 days', NOW() - INTERVAL '2 days'),
(10, 89.99, 'credit_card', 'completed', NOW() - INTERVAL '4 days', NOW() - INTERVAL '1 day'),
(11, 449.99, 'credit_card', 'completed', NOW() - INTERVAL '8 days', NOW() - INTERVAL '5 days'),
(12, 49.99, 'paypal', 'completed', NOW() - INTERVAL '9 days', NOW() - INTERVAL '6 days'),
(14, 129.99, 'bank_transfer', 'completed', NOW() - INTERVAL '5 days', NOW() - INTERVAL '3 days');
