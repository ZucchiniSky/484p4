CREATE TABLE inserto (a integer, b double, c char[1]);

INSERT INTO inserto (c, b, a) values (a, 3.14, 5);
INSERT INTO inserto (a, b) values (5, 3.14);
INSERT INTO inserto (b, c) values (3.14, a);
INSERT INTO inserto (a, c, b) values (5, a, 3.14);
INSERT INTO inserto (b, c, a) values (3.14, a, 5);
INSERT INTO inserto (b, a, c) values (3.14, 5, a);
INSERT INTO inserto (c, a, b) values (a, 5, 3.14);
INSERT INTO inserto (c, b, a) values (a, 3.14, 5);
INSERT INTO inserto (a, a) values (5, 5);

DROP TABLE inserto;