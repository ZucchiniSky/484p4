CREATE TABLE mergeEdge1(a integer);
CREATE TABLE mergeEdge2(b integer);

INSERT INTO mergeEdge1 (a) values (5);
INSERT INTO mergeEdge2 (b) values (5);
INSERT INTO mergeEdge1 (a) values (5);
INSERT INTO mergeEdge2 (b) values (5);
INSERT INTO mergeEdge1 (a) values (5);
INSERT INTO mergeEdge2 (b) values (5);
INSERT INTO mergeEdge1 (a) values (5);
INSERT INTO mergeEdge2 (b) values (5);

SELECT mergeEdge1.a FROM mergeEdge1, mergeEdge2 WHERE mergeEdge1.a = mergeEdge2.b;

DROP TABLE mergeEdge1;
DROP TABLE mergeEdge2;

CREATE TABLE mergeEdge1(a integer);
CREATE TABLE mergeEdge2(b integer);

INSERT INTO mergeEdge1 (a) values (4);
INSERT INTO mergeEdge2 (b) values (5);
INSERT INTO mergeEdge1 (a) values (6);
INSERT INTO mergeEdge2 (b) values (6);
INSERT INTO mergeEdge1 (a) values (6);
INSERT INTO mergeEdge2 (b) values (6);
INSERT INTO mergeEdge1 (a) values (7);
INSERT INTO mergeEdge2 (b) values (8);

SELECT mergeEdge1.a FROM mergeEdge1, mergeEdge2 WHERE mergeEdge1.a = mergeEdge2.b;

DROP TABLE mergeEdge1;
DROP TABLE mergeEdge2;

CREATE TABLE mergeEdge1(a integer);
CREATE TABLE mergeEdge2(b integer);

INSERT INTO mergeEdge1 (a) values (1);
INSERT INTO mergeEdge2 (b) values (1);
INSERT INTO mergeEdge1 (a) values (2);
INSERT INTO mergeEdge2 (b) values (2);
INSERT INTO mergeEdge1 (a) values (3);
INSERT INTO mergeEdge2 (b) values (3);
INSERT INTO mergeEdge1 (a) values (4);
INSERT INTO mergeEdge2 (b) values (4);

SELECT mergeEdge1.a FROM mergeEdge1, mergeEdge2 WHERE mergeEdge1.a = mergeEdge2.b;

DROP TABLE mergeEdge1;
DROP TABLE mergeEdge2;

CREATE TABLE mergeEdge1(a integer);
CREATE TABLE mergeEdge2(b integer);

INSERT INTO mergeEdge1 (a) values (4);
INSERT INTO mergeEdge2 (b) values (1);
INSERT INTO mergeEdge1 (a) values (3);
INSERT INTO mergeEdge2 (b) values (2);
INSERT INTO mergeEdge1 (a) values (2);
INSERT INTO mergeEdge2 (b) values (3);
INSERT INTO mergeEdge1 (a) values (1);
INSERT INTO mergeEdge2 (b) values (4);

SELECT mergeEdge1.a FROM mergeEdge1, mergeEdge2 WHERE mergeEdge1.a = mergeEdge2.b;

DROP TABLE mergeEdge1;
DROP TABLE mergeEdge2;