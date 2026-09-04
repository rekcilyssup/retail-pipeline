CREATE TABLE customers (
    customer_id     INTEGER PRIMARY KEY,
    name            VARCHAR(100),
    city            VARCHAR(50),
    segment         VARCHAR(20),
    signup_date     DATE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

INSERT INTO customers (customer_id, name, city, segment, signup_date) VALUES
(1, 'Aarav Sharma', 'Hyderabad', 'Premium', '2023-01-15'),
(2, 'Priya Nair', 'Chennai', 'Standard', '2023-03-22'),
(3, 'Rohan Mehta', 'Bangalore', 'Premium', '2022-11-05'),
(4, 'Sneha Reddy', 'Hyderabad', 'Standard', '2024-02-10'),
(5, 'Karthik Iyer', 'Chennai', 'Premium', '2023-07-19'),
(6, 'Ananya Gupta', 'Bangalore', 'Standard', '2024-05-01'),
(7, 'Vikram Rao', 'Hyderabad', 'Premium', '2022-09-14'),
(8, 'Divya Menon', 'Chennai', 'Standard', '2023-12-30'),
(9, 'Arjun Kapoor', 'Bangalore', 'Premium', '2023-04-08'),
(10, 'Meera Pillai', 'Hyderabad', 'Standard', '2024-01-20');
