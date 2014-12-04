CREATE TABLE table (a integer);

INSERT INTO table (a) values (1);
INSERT INTO table (a) values (2);
INSERT INTO table (a) values (3);
INSERT INTO table (a) values (4);
INSERT INTO table (a) values (5);

SELECT table.a FROM table WHERE table.a > 3;
SELECT table.a FROM table WHERE table.a < 3;
SELECT table.a FROM table WHERE table.a >= 3;
SELECT table.a FROM table WHERE table.a <= 3;

DROP TABLE table;