
CREATE TABLE first (a integer, b integer);
CREATE TABLE second (b integer, a integer);

CREATE INDEX first (a);
CREATE INDEX second (b);

INSERT INTO first (a, b) values (3, 2);
INSERT INTO second (a, b) values (4, 2);
INSERT INTO first (a, b) values (5, 2);
INSERT INTO second (a, b) values (4, 3);
INSERT INTO first (a, b) values (4, 3);

SELECT * FROM first;
SELECT * FROM second;

SELECT first.a, first.b FROM first, second WHERE first.a = second.a;

SELECT first.a, first.b FROM first, second WHERE first.b = second.b;

DROP TABLE first;
DROP TABLE second;