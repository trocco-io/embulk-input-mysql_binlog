DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
DROP TABLE IF EXISTS test;
 
CREATE TABLE test (
id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
name TEXT NOT NULL
)DEFAULT CHARACTER SET=utf8mb4;
 
INSERT INTO test (name) VALUES ("foo"),("bar"),("baz");
