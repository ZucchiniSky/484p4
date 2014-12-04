CREATE TABLE first (a integer);
CREATE TABLE second (b integer);

INSERT INTO first (a) values (1);
INSERT INTO second (b) values (2);
INSERT INTO first (a) values (3);
INSERT INTO second (b) values (4);
INSERT INTO first (a) values (5);
INSERT INTO second (b) values (6);

SELECT first.a, second.b FROM first, second WHERE first.a < second.b;
SELECT first.a, second.b FROM first, second WHERE first.a > second.b;
SELECT first.a, second.b FROM first, second WHERE first.a != second.b;

DROP TABLE first;
DROP TABLE second;