DROP DATABASE IF EXISTS testdb;

CREATE DATABASE testdb;

GRANT ALL ON testdb.* TO 'testuser'@'localhost' IDENTIFIED BY 'password';

CREATE TABLE testdb.customers (customer_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, first_name TEXT, last_name TEXT);

INSERT INTO testdb.customers (first_name, last_name) VALUES ('Cardinal0','Tom B. Erichsen0');
INSERT INTO testdb.customers (first_name, last_name) VALUES ('Cardinal1','Tom B. Erichsen1');
INSERT INTO testdb.customers (first_name, last_name) VALUES ('Cardinal2','Tom B. Erichsen2');
                                                                   