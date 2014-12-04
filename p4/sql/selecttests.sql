CREATE TABLE first (a integer);

INSERT INTO first (a) values (1);
INSERT INTO first (a) values (2);
INSERT INTO first (a) values (3);
INSERT INTO first (a) values (4);
INSERT INTO first (a) values (5);

SELECT first.a FROM first WHERE first.a > 3;
SELECT first.a FROM first WHERE first.a < 3;
SELECT first.a FROM first WHERE first.a >= 3;
SELECT first.a FROM first WHERE first.a <= 3;

DROP TABLE table;