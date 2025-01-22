-- Create two sample tables
CREATE TABLE users AS
SELECT *
FROM (VALUES
    (1, 'Alice', 25),
    (2, 'Bob', 30),
    (3, 'Charlie', 35)
) AS t(id, name, age);

CREATE TABLE orders AS
SELECT *
FROM (VALUES
    (101, 1, '2023-01-01', 100.50),
    (102, 2, '2023-01-02', 200.75),
    (103, 1, '2023-01-03', 150.00)
) AS t(order_id, user_id, order_date, amount);

-- Write JSON to a file using json_group_object
COPY (
    SELECT json_group_object(
        'users', (SELECT json_group_array(json_object(*)) FROM users),
        'orders', (SELECT json_group_array(json_object(*)) FROM orders)
    )
) TO 'output.json';

-- Read the JSON file as text
SELECT content
FROM read_text('output.json');
