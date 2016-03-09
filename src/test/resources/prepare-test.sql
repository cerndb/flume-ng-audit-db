CREATE DATABASE IF NOT EXISTS testdb;

GRANT ALL ON testdb.* TO 'testuser'@'localhost' IDENTIFIED BY 'password';

CREATE TABLE IF NOT EXISTS testdb.customers (customer_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, first_name TEXT, last_name TEXT);
                                                                   