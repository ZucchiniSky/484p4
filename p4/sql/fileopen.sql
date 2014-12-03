--
-- test Insert with and without indices
--

-- create relations
CREATE TABLE soaps(soapid integer, name char(32), 
                   network char(4), rating double);
CREATE TABLE stars(starid integer, real_name char(20), 
                   plays char(12), soapid integer);


-- insert with attributes in order:
INSERT INTO stars(starid, real_name, plays, soapid) 
	VALUES (100, 'Posey, Parker', 'Tess', 6);

-- insert with attributes out of order:
INSERT INTO stars (real_name, soapid, starid, plays) 
	VALUES ('Bonarrigo, Laura', 3, 101, 'Cassie');

SELECT * FROM stars;

-- run a few select queries
SELECT stars.soapid, stars.real_name FROM stars;

DROP TABLE stars;
