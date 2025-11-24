create DATABASE DW_Shopzada;
Use DW_Shopzada;

CREATE TABLE dim_location (
  location_id int PRIMARY KEY,
  street VARCHAR(40),
  city VARCHAR(25),
  state VARCHAR(25),
  country VARCHAR(25)
);

CREATE TABLE dim_user (
  user_id VARCHAR(9) PRIMARY KEY,
  name VARCHAR(100),
  birthdate DATETIME,
  gender VARCHAR(10),
  device_address VARCHAR(100),
  creation_date DATETIME,
  user_type VARCHAR(25),
  job_title VARCHAR(25),
  job_level VARCHAR(25),
  location_id INTEGER
);

CREATE TABLE dim_issuing_bank (
  user_id VARCHAR(9),
  issuing_bank VARCHAR(40)
);

CREATE TABLE dim_product (
  product_id VARCHAR(12) PRIMARY KEY,
  product_name VARCHAR(25),
  product_type VARCHAR(25),
  price DECIMAL(5,2)
);

CREATE TABLE dim_merchant (
  merchant_id VARCHAR(13) PRIMARY KEY,
  name VARCHAR(40),
  contact_number VARCHAR(20),
  creation_date DATETIME,
  location_id INTEGER
);

CREATE TABLE dim_staff (
  staff_id VARCHAR(12) PRIMARY KEY,
  name VARCHAR(40),
  job_level VARCHAR(25),
  contact_number VARCHAR(20),
  creation_date DATETIME,
  location_id INTEGER
);

CREATE TABLE dim_campaign (
  campaign_id VARCHAR(13) PRIMARY KEY,
  campaign_name VARCHAR(25),
  campaign_description VARCHAR(50),
  discount DECIMAL(5,2)
);

CREATE TABLE dim_date (
  date_id DATE PRIMARY KEY,
  day INTEGER,
  month INTEGER,
  year INTEGER,
  quarter INTEGER,
  day_of_week VARCHAR(10),
  month_name VARCHAR(10),
  is_weekend BOOLEAN,
  is_holiday BOOLEAN
);

-- ////////////////////////////////////////
-- // 3. FACT TABLES
-- ////////////////////////////////////////

CREATE TABLE fact_orders (
  order_id VARCHAR(25) PRIMARY KEY,
  user_id VARCHAR(9),
  merchant_id VARCHAR(13),
  staff_id VARCHAR(12),
  campaign_id VARCHAR(13),
  transaction_date DATE,
  estimated_arrival INTEGER,
  delay_in_days INTEGER,
  campaign_availed BOOLEAN
);

CREATE TABLE fact_line_items (
  line_item_id SERIAL PRIMARY KEY,
  order_id VARCHAR(25),
  product_id VARCHAR(12),
  merchant_id VARCHAR(13),
  user_id VARCHAR(9),
  transaction_date DATE,
  price DECIMAL(5,2),
  quantity INTEGER
);

-- ////////////////////////////////////////
-- // 4. FOREIGN KEY CONSTRAINTS
-- ////////////////////////////////////////

-- Dimension Relationships
ALTER TABLE dim_user ADD FOREIGN KEY (location_id) REFERENCES dim_location (location_id);
ALTER TABLE dim_issuing_bank ADD FOREIGN KEY (user_id) REFERENCES dim_user (user_id);
ALTER TABLE dim_merchant ADD FOREIGN KEY (location_id) REFERENCES dim_location (location_id);
ALTER TABLE dim_staff ADD FOREIGN KEY (location_id) REFERENCES dim_location (location_id);

-- Fact Orders Relationships
ALTER TABLE fact_orders ADD FOREIGN KEY (user_id) REFERENCES dim_user (user_id);
ALTER TABLE fact_orders ADD FOREIGN KEY (merchant_id) REFERENCES dim_merchant (merchant_id);
ALTER TABLE fact_orders ADD FOREIGN KEY (staff_id) REFERENCES dim_staff (staff_id);
ALTER TABLE fact_orders ADD FOREIGN KEY (campaign_id) REFERENCES dim_campaign (campaign_id);
ALTER TABLE fact_orders ADD FOREIGN KEY (transaction_date) REFERENCES dim_date (date_id);

-- Fact Line Items Relationships
ALTER TABLE fact_line_items ADD FOREIGN KEY (order_id) REFERENCES fact_orders (order_id);
ALTER TABLE fact_line_items ADD FOREIGN KEY (product_id) REFERENCES dim_product (product_id);
ALTER TABLE fact_line_items ADD FOREIGN KEY (merchant_id) REFERENCES dim_merchant (merchant_id);
ALTER TABLE fact_line_items ADD FOREIGN KEY (user_id) REFERENCES dim_user (user_id);
ALTER TABLE fact_line_items ADD FOREIGN KEY (transaction_date) REFERENCES dim_date (date_id);


 INSERT INTO dim_location (location_id, street, city, state, country) VALUES
(1, '123 Maple Ave', 'New York', 'NY', 'USA'),
(2, '456 Oak St', 'Los Angeles', 'CA', 'USA'),
(3, '789 Pine Rd', 'Chicago', 'IL', 'USA'),
(4, '101 Cedar Ln', 'Houston', 'TX', 'USA'),
(5, '202 Birch Blvd', 'Phoenix', 'AZ', 'USA'),
(6, '303 Elm Dr', 'Philadelphia', 'PA', 'USA'),
(7, '404 Spruce Way', 'San Antonio', 'TX', 'USA'),
(8, '505 Willow Ct', 'San Diego', 'CA', 'USA'),
(9, '606 Aspen Pl', 'Dallas', 'TX', 'USA'),
(10, '707 Magnolia St', 'San Jose', 'CA', 'USA'),
(11, '808 Redwood Dr', 'Austin', 'TX', 'USA'),
(12, '909 Cypress Ln', 'Jacksonville', 'FL', 'USA'),
(13, '111 Poplar Cir', 'San Francisco', 'CA', 'USA'),
(14, '222 Cherry Ave', 'Columbus', 'OH', 'USA'),
(15, '333 Sycamore Rd', 'Indianapolis', 'IN', 'USA'),
(16, '444 Dogwood St', 'Fort Worth', 'TX', 'USA'),
(17, '555 Hawthorn Blvd', 'Charlotte', 'NC', 'USA'),
(18, '666 Juniper Way', 'Seattle', 'WA', 'USA'),
(19, '777 Laurel Dr', 'Denver', 'CO', 'USA'),
(20, '888 Palm Pl', 'Washington', 'DC', 'USA');

INSERT INTO dim_user (user_id, name, birthdate, gender, device_address, creation_date, user_type, job_title, job_level, location_id) VALUES
('USR000001', 'John Doe', '1985-04-12 00:00:00', 'Male', '192.168.1.1', '2023-01-10 09:00:00', 'Premium', 'Software Engineer', 'Senior', 1),
('USR000002', 'Jane Smith', '1990-07-23 00:00:00', 'Female', '192.168.1.2', '2023-01-11 10:30:00', 'Standard', 'Data Analyst', 'Mid', 2),
('USR000003', 'Michael Brown', '1982-11-05 00:00:00', 'Male', '192.168.1.3', '2023-01-12 11:15:00', 'Premium', 'Product Manager', 'Senior', 3),
('USR000004', 'Emily Davis', '1995-02-18 00:00:00', 'Female', '192.168.1.4', '2023-01-13 14:20:00', 'Standard', 'UX Designer', 'Junior', 4),
('USR000005', 'Chris Wilson', '1988-09-30 00:00:00', 'Male', '192.168.1.5', '2023-01-14 16:45:00', 'Premium', 'DevOps Engineer', 'Mid', 5),
('USR000006', 'Sarah Johnson', '1992-05-14 00:00:00', 'Female', '192.168.1.6', '2023-01-15 08:10:00', 'Standard', 'Marketing Specialist', 'Mid', 6),
('USR000007', 'David Lee', '1980-12-22 00:00:00', 'Male', '192.168.1.7', '2023-01-16 13:00:00', 'Premium', 'CTO', 'Executive', 7),
('USR000008', 'Laura Martinez', '1998-03-08 00:00:00', 'Female', '192.168.1.8', '2023-01-17 15:30:00', 'Standard', 'Intern', 'Entry', 8),
('USR000009', 'James Taylor', '1987-06-19 00:00:00', 'Male', '192.168.1.9', '2023-01-18 09:45:00', 'Standard', 'Accountant', 'Senior', 9),
('USR000010', 'Linda Anderson', '1993-08-25 00:00:00', 'Female', '192.168.1.10', '2023-01-19 11:00:00', 'Premium', 'HR Manager', 'Manager', 10),
('USR000011', 'Robert Thomas', '1984-01-15 00:00:00', 'Male', '192.168.1.11', '2023-01-20 12:15:00', 'Standard', 'Sales Rep', 'Mid', 11),
('USR000012', 'Patricia Jackson', '1991-10-03 00:00:00', 'Female', '192.168.1.12', '2023-01-21 14:40:00', 'Premium', 'Consultant', 'Senior', 12),
('USR000013', 'Charles White', '1989-04-27 00:00:00', 'Male', '192.168.1.13', '2023-01-22 10:00:00', 'Standard', 'Admin Assistant', 'Junior', 13),
('USR000014', 'Barbara Harris', '1996-09-12 00:00:00', 'Female', '192.168.1.14', '2023-01-23 16:20:00', 'Standard', 'Content Writer', 'Mid', 14),
('USR000015', 'Daniel Martin', '1983-11-29 00:00:00', 'Male', '192.168.1.15', '2023-01-24 09:30:00', 'Premium', 'Operations Mgr', 'Manager', 15),
('USR000016', 'Jennifer Thompson', '1994-02-05 00:00:00', 'Female', '192.168.1.16', '2023-01-25 11:50:00', 'Standard', 'QA Tester', 'Mid', 16),
('USR000017', 'Matthew Garcia', '1986-07-17 00:00:00', 'Male', '192.168.1.17', '2023-01-26 13:10:00', 'Premium', 'Systems Architect', 'Senior', 17),
('USR000018', 'Susan Robinson', '1997-12-09 00:00:00', 'Female', '192.168.1.18', '2023-01-27 15:55:00', 'Standard', 'Graphic Designer', 'Junior', 18),
('USR000019', 'Anthony Clark', '1981-05-21 00:00:00', 'Male', '192.168.1.19', '2023-01-28 08:25:00', 'Premium', 'Director', 'Executive', 19),
('USR000020', 'Karen Rodriguez', '1999-03-14 00:00:00', 'Female', '192.168.1.20', '2023-01-29 10:45:00', 'Standard', 'Receptionist', 'Entry', 20);

INSERT INTO dim_issuing_bank (user_id, issuing_bank) VALUES
('USR000001', 'Chase'),
('USR000002', 'Bank of America'),
('USR000003', 'Wells Fargo'),
('USR000004', 'Citibank'),
('USR000005', 'US Bank'),
('USR000006', 'PNC Bank'),
('USR000007', 'Capital One'),
('USR000008', 'TD Bank'),
('USR000009', 'Truist'),
('USR000010', 'Chase'),
('USR000011', 'Bank of America'),
('USR000012', 'Wells Fargo'),
('USR000013', 'Citibank'),
('USR000014', 'US Bank'),
('USR000015', 'PNC Bank'),
('USR000016', 'Capital One'),
('USR000017', 'TD Bank'),
('USR000018', 'Truist'),
('USR000019', 'Chase'),
('USR000020', 'Bank of America');

INSERT INTO dim_product (product_id, product_name, product_type, price) VALUES
('PROD00000001', 'Wireless Mouse', 'Electronics', 25.50),
('PROD00000002', 'Mechanical Keyboard', 'Electronics', 89.99),
('PROD00000003', '27-inch Monitor', 'Electronics', 299.00),
('PROD00000004', 'USB-C Hub', 'Accessories', 45.00),
('PROD00000005', 'Noise Cancelling Headphones', 'Audio', 150.00),
('PROD00000006', 'Bluetooth Speaker', 'Audio', 59.99),
('PROD00000007', 'Smartphone Stand', 'Accessories', 12.99),
('PROD00000008', 'Laptop Sleeve', 'Accessories', 19.99),
('PROD00000009', 'External SSD 1TB', 'Storage', 120.00),
('PROD00000010', 'Webcam 1080p', 'Electronics', 65.00),
('PROD00000011', 'Ergonomic Chair', 'Furniture', 350.00),
('PROD00000012', 'Standing Desk', 'Furniture', 450.00),
('PROD00000013', 'Desk Lamp', 'Lighting', 35.00),
('PROD00000014', 'Mouse Pad', 'Accessories', 9.99),
('PROD00000015', 'Gaming Laptop', 'Computers', 100.00),
('PROD00000016', 'Business Laptop', 'Computers', 950.00),
('PROD00000017', 'Tablet 10-inch', 'Tablets', 300.00),
('PROD00000018', 'Smart Watch', 'Wearables', 199.00),
('PROD00000019', 'Fitness Tracker', 'Wearables', 99.00),
('PROD00000020', 'HDMI Cable', 'Cables', 15.00);

INSERT INTO dim_merchant (merchant_id, name, contact_number, creation_date, location_id) VALUES
('MERCH00000001', 'Tech Haven', '555-0101', '2022-05-01 09:00:00', 1),
('MERCH00000002', 'Gadget Galaxy', '555-0102', '2022-05-02 10:00:00', 2),
('MERCH00000003', 'Audio World', '555-0103', '2022-05-03 11:00:00', 3),
('MERCH00000004', 'Office Depot', '555-0104', '2022-05-04 12:00:00', 4),
('MERCH00000005', 'Home Comforts', '555-0105', '2022-05-05 13:00:00', 5),
('MERCH00000006', 'Digital Dreams', '555-0106', '2022-05-06 14:00:00', 6),
('MERCH00000007', 'PC Builders', '555-0107', '2022-05-07 15:00:00', 7),
('MERCH00000008', 'Mobile Store', '555-0108', '2022-05-08 16:00:00', 8),
('MERCH00000009', 'Sound Systems', '555-0109', '2022-05-09 17:00:00', 9),
('MERCH00000010', 'Vision Tech', '555-0110', '2022-05-10 09:30:00', 10),
('MERCH00000011', 'Electro Mart', '555-0111', '2022-05-11 10:30:00', 11),
('MERCH00000012', 'Accessory Hub', '555-0112', '2022-05-12 11:30:00', 12),
('MERCH00000013', 'Gaming Zone', '555-0113', '2022-05-13 12:30:00', 13),
('MERCH00000014', 'Cable Connect', '555-0114', '2022-05-14 13:30:00', 14),
('MERCH00000015', 'Storage Solutions', '555-0115', '2022-05-15 14:30:00', 15),
('MERCH00000016', 'Smart Life', '555-0116', '2022-05-16 15:30:00', 16),
('MERCH00000017', 'Fit Gear', '555-0117', '2022-05-17 16:30:00', 17),
('MERCH00000018', 'Tablet Town', '555-0118', '2022-05-18 17:30:00', 18),
('MERCH00000019', 'Lighting Pro', '555-0119', '2022-05-19 09:45:00', 19),
('MERCH00000020', 'Desk Direct', '555-0120', '2022-05-20 10:45:00', 20);

INSERT INTO dim_staff (staff_id, name, job_level, contact_number, creation_date, location_id) VALUES
('STF000000001', 'Alice Walker', 'Manager', '555-1111', '2022-01-01 09:00:00', 1),
('STF000000002', 'Bob Miller', 'Associate', '555-1112', '2022-01-02 10:00:00', 2),
('STF000000003', 'Charlie Hall', 'Senior', '555-1113', '2022-01-03 11:00:00', 3),
('STF000000004', 'Diana Young', 'Associate', '555-1114', '2022-01-04 12:00:00', 4),
('STF000000005', 'Evan King', 'Manager', '555-1115', '2022-01-05 13:00:00', 5),
('STF000000006', 'Fiona Scott', 'Senior', '555-1116', '2022-01-06 14:00:00', 6),
('STF000000007', 'George Green', 'Associate', '555-1117', '2022-01-07 15:00:00', 7),
('STF000000008', 'Hannah Baker', 'Manager', '555-1118', '2022-01-08 16:00:00', 8),
('STF000000009', 'Ian Adams', 'Associate', '555-1119', '2022-01-09 17:00:00', 9),
('STF000000010', 'Julia Nelson', 'Senior', '555-1120', '2022-01-10 09:30:00', 10),
('STF000000011', 'Kevin Carter', 'Associate', '555-1121', '2022-01-11 10:30:00', 11),
('STF000000012', 'Laura Mitchell', 'Manager', '555-1122', '2022-01-12 11:30:00', 12),
('STF000000013', 'Mike Perez', 'Senior', '555-1123', '2022-01-13 12:30:00', 13),
('STF000000014', 'Nina Roberts', 'Associate', '555-1124', '2022-01-14 13:30:00', 14),
('STF000000015', 'Oscar Turner', 'Manager', '555-1125', '2022-01-15 14:30:00', 15),
('STF000000016', 'Paula Phillips', 'Associate', '555-1126', '2022-01-16 15:30:00', 16),
('STF000000017', 'Quinn Campbell', 'Senior', '555-1127', '2022-01-17 16:30:00', 17),
('STF000000018', 'Rachel Parker', 'Associate', '555-1128', '2022-01-18 17:30:00', 18),
('STF000000019', 'Steve Evans', 'Manager', '555-1129', '2022-01-19 09:45:00', 19),
('STF000000020', 'Tina Edwards', 'Senior', '555-1130', '2022-01-20 10:45:00', 20);

INSERT INTO dim_campaign (campaign_id, campaign_name, campaign_description, discount) VALUES
('CAMP000000001', 'New Year Sale', 'Start the year with savings', 10.00),
('CAMP000000002', 'Spring Fling', 'Fresh deals for spring', 15.00),
('CAMP000000003', 'Summer Blowout', 'Hot summer discounts', 20.00),
('CAMP000000004', 'Back to School', 'Essentials for students', 12.50),
('CAMP000000005', 'Black Friday', 'Biggest sale of the year', 50.00),
('CAMP000000006', 'Cyber Monday', 'Online tech deals', 40.00),
('CAMP000000007', 'Winter Wonderland', 'Cozy winter savings', 15.00),
('CAMP000000008', 'Valentine Special', 'Gifts for loved ones', 10.00),
('CAMP000000009', 'Halloween Spooktacular', 'Scary good deals', 13.00),
('CAMP000000010', 'Flash Sale', '24 hour discounts', 25.00),
('CAMP000000011', 'Member Exclusive', 'Deals for members only', 5.00),
('CAMP000000012', 'Clearance', 'Last chance items', 60.00),
('CAMP000000013', 'Holiday Gift Guide', 'Curated gift items', 10.00),
('CAMP000000014', 'Easter Eggstravaganza', 'Hop into savings', 10.00),
('CAMP000000015', 'Labor Day Sale', 'End of summer deals', 15.00),
('CAMP000000016', 'Memorial Day', 'Kickoff to summer', 15.00),
('CAMP000000017', 'Independence Day', 'Fourth of July savings', 17.76),
('CAMP000000018', 'Tech Week', 'Discounts on gadgets', 20.00),
('CAMP000000019', 'Home Office Upgrade', 'Furniture and tech deals', 15.00),
('CAMP000000020', 'Welcome Offer', 'First time buyer discount', 10.00);

INSERT INTO dim_date (date_id, day, month, year, quarter, day_of_week, month_name, is_weekend, is_holiday) VALUES
('2023-01-01', 1, 1, 2023, 1, 'Sunday', 'January', TRUE, TRUE),
('2023-01-02', 2, 1, 2023, 1, 'Monday', 'January', FALSE, FALSE),
('2023-01-03', 3, 1, 2023, 1, 'Tuesday', 'January', FALSE, FALSE),
('2023-01-04', 4, 1, 2023, 1, 'Wednesday', 'January', FALSE, FALSE),
('2023-01-05', 5, 1, 2023, 1, 'Thursday', 'January', FALSE, FALSE),
('2023-01-06', 6, 1, 2023, 1, 'Friday', 'January', FALSE, FALSE),
('2023-01-07', 7, 1, 2023, 1, 'Saturday', 'January', TRUE, FALSE),
('2023-01-08', 8, 1, 2023, 1, 'Sunday', 'January', TRUE, FALSE),
('2023-01-09', 9, 1, 2023, 1, 'Monday', 'January', FALSE, FALSE),
('2023-01-10', 10, 1, 2023, 1, 'Tuesday', 'January', FALSE, FALSE),
('2023-01-11', 11, 1, 2023, 1, 'Wednesday', 'January', FALSE, FALSE),
('2023-01-12', 12, 1, 2023, 1, 'Thursday', 'January', FALSE, FALSE),
('2023-01-13', 13, 1, 2023, 1, 'Friday', 'January', FALSE, FALSE),
('2023-01-14', 14, 1, 2023, 1, 'Saturday', 'January', TRUE, FALSE),
('2023-01-15', 15, 1, 2023, 1, 'Sunday', 'January', TRUE, FALSE),
('2023-01-16', 16, 1, 2023, 1, 'Monday', 'January', FALSE, TRUE),
('2023-01-17', 17, 1, 2023, 1, 'Tuesday', 'January', FALSE, FALSE),
('2023-01-18', 18, 1, 2023, 1, 'Wednesday', 'January', FALSE, FALSE),
('2023-01-19', 19, 1, 2023, 1, 'Thursday', 'January', FALSE, FALSE),
('2023-01-20', 20, 1, 2023, 1, 'Friday', 'January', FALSE, FALSE);

INSERT INTO fact_orders (order_id, user_id, merchant_id, staff_id, campaign_id, transaction_date, estimated_arrival, delay_in_days, campaign_availed) VALUES
('ORD-2023-001', 'USR000001', 'MERCH00000001', 'STF000000001', 'CAMP000000001', '2023-01-01', 5, 0, TRUE),
('ORD-2023-002', 'USR000002', 'MERCH00000002', 'STF000000002', 'CAMP000000001', '2023-01-02', 3, 1, TRUE),
('ORD-2023-003', 'USR000003', 'MERCH00000003', 'STF000000003', 'CAMP000000002', '2023-01-03', 7, 0, TRUE),
('ORD-2023-004', 'USR000004', 'MERCH00000004', 'STF000000004', 'CAMP000000002', '2023-01-04', 4, 2, FALSE),
('ORD-2023-005', 'USR000005', 'MERCH00000005', 'STF000000005', 'CAMP000000003', '2023-01-05', 5, 0, TRUE),
('ORD-2023-006', 'USR000006', 'MERCH00000006', 'STF000000006', 'CAMP000000003', '2023-01-06', 6, 0, TRUE),
('ORD-2023-007', 'USR000007', 'MERCH00000007', 'STF000000007', 'CAMP000000004', '2023-01-07', 3, 1, FALSE),
('ORD-2023-008', 'USR000008', 'MERCH00000008', 'STF000000008', 'CAMP000000004', '2023-01-08', 4, 0, TRUE),
('ORD-2023-009', 'USR000009', 'MERCH00000009', 'STF000000009', 'CAMP000000005', '2023-01-09', 2, 0, TRUE),
('ORD-2023-010', 'USR000010', 'MERCH00000010', 'STF000000010', 'CAMP000000005', '2023-01-10', 5, 3, TRUE),
('ORD-2023-011', 'USR000011', 'MERCH00000011', 'STF000000011', 'CAMP000000006', '2023-01-11', 4, 0, TRUE),
('ORD-2023-012', 'USR000012', 'MERCH00000012', 'STF000000012', 'CAMP000000006', '2023-01-12', 3, 0, TRUE),
('ORD-2023-013', 'USR000013', 'MERCH00000013', 'STF000000013', 'CAMP000000007', '2023-01-13', 5, 1, FALSE),
('ORD-2023-014', 'USR000014', 'MERCH00000014', 'STF000000014', 'CAMP000000007', '2023-01-14', 6, 0, TRUE),
('ORD-2023-015', 'USR000015', 'MERCH00000015', 'STF000000015', 'CAMP000000008', '2023-01-15', 2, 0, TRUE),
('ORD-2023-016', 'USR000016', 'MERCH00000016', 'STF000000016', 'CAMP000000008', '2023-01-16', 3, 0, FALSE),
('ORD-2023-017', 'USR000017', 'MERCH00000017', 'STF000000017', 'CAMP000000009', '2023-01-17', 4, 1, TRUE),
('ORD-2023-018', 'USR000018', 'MERCH00000018', 'STF000000018', 'CAMP000000009', '2023-01-18', 5, 0, TRUE),
('ORD-2023-019', 'USR000019', 'MERCH00000019', 'STF000000019', 'CAMP000000010', '2023-01-19', 3, 0, TRUE),
('ORD-2023-020', 'USR000020', 'MERCH00000020', 'STF000000020', 'CAMP000000010', '2023-01-20', 4, 0, FALSE);

INSERT INTO fact_line_items (order_id, product_id, merchant_id, user_id, transaction_date, price, quantity) VALUES
('ORD-2023-001', 'PROD00000001', 'MERCH00000001', 'USR000001', '2023-01-01', 25.50, 1),
('ORD-2023-002', 'PROD00000002', 'MERCH00000002', 'USR000002', '2023-01-02', 89.99, 1),
('ORD-2023-003', 'PROD00000003', 'MERCH00000003', 'USR000003', '2023-01-03', 299.00, 1),
('ORD-2023-004', 'PROD00000004', 'MERCH00000004', 'USR000004', '2023-01-04', 45.00, 2),
('ORD-2023-005', 'PROD00000005', 'MERCH00000005', 'USR000005', '2023-01-05', 150.00, 1),
('ORD-2023-006', 'PROD00000006', 'MERCH00000006', 'USR000006', '2023-01-06', 59.99, 1),
('ORD-2023-007', 'PROD00000007', 'MERCH00000007', 'USR000007', '2023-01-07', 12.99, 3),
('ORD-2023-008', 'PROD00000008', 'MERCH00000008', 'USR000008', '2023-01-08', 19.99, 1),
('ORD-2023-009', 'PROD00000009', 'MERCH00000009', 'USR000009', '2023-01-09', 120.00, 2),
('ORD-2023-010', 'PROD00000010', 'MERCH00000010', 'USR000010', '2023-01-10', 65.00, 1),
('ORD-2023-011', 'PROD00000011', 'MERCH00000011', 'USR000011', '2023-01-11', 350.00, 1),
('ORD-2023-012', 'PROD00000012', 'MERCH00000012', 'USR000012', '2023-01-12', 450.00, 1),
('ORD-2023-013', 'PROD00000013', 'MERCH00000013', 'USR000013', '2023-01-13', 35.00, 4),
('ORD-2023-014', 'PROD00000014', 'MERCH00000014', 'USR000014', '2023-01-14', 9.99, 1),
('ORD-2023-015', 'PROD00000015', 'MERCH00000015', 'USR000015', '2023-01-15', 100.00, 1),
('ORD-2023-016', 'PROD00000016', 'MERCH00000016', 'USR000016', '2023-01-16', 950.00, 1),
('ORD-2023-017', 'PROD00000017', 'MERCH00000017', 'USR000017', '2023-01-17', 300.00, 1),
('ORD-2023-018', 'PROD00000018', 'MERCH00000018', 'USR000018', '2023-01-18', 199.00, 1),
('ORD-2023-019', 'PROD00000019', 'MERCH00000019', 'USR000019', '2023-01-19', 99.00, 2),
('ORD-2023-020', 'PROD00000020', 'MERCH00000020', 'USR000020', '2023-01-20', 15.00, 5);


-- What kinds of campaigns drive the highest order volume?
SELECT 
    dc.campaign_name,
    COUNT(fo.order_id) AS total_order_volume
FROM 
    fact_orders fo
JOIN 
    dim_campaign dc ON fo.campaign_id = dc.campaign_id
WHERE 
    fo.campaign_availed = TRUE
GROUP BY 
    dc.campaign_name
ORDER BY 
    total_order_volume DESC;

-- How do merchant performance metrics affect sales?
SELECT 
    dm.name AS merchant_name,
    AVG(fo.delay_in_days) AS avg_delay_days,
    SUM(fli.price * fli.quantity) AS total_revenue
FROM 
    fact_orders fo
JOIN 
    fact_line_items fli ON fo.order_id = fli.order_id
JOIN 
    dim_merchant dm ON fo.merchant_id = dm.merchant_id
GROUP BY 
    dm.name
ORDER BY 
    avg_delay_days DESC; -- Ordering by delay to see if high delay correlates with low revenue
    
    
-- What customer segments contribute most to revenue?
SELECT 
    du.user_type,
    du.job_level,
    COUNT(DISTINCT fli.order_id) AS number_of_orders,
    SUM(fli.price * fli.quantity) AS total_revenue
FROM 
    fact_line_items fli
JOIN 
    dim_user du ON fli.user_id = du.user_id
GROUP BY 
    du.user_type, 
    du.job_level
ORDER BY 
    total_revenue DESC;
    