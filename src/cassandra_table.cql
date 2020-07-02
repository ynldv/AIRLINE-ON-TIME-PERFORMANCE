USE ladeveya_projet;



DROP TABLE IF EXISTS flight;
CREATE TABLE flight(
	creation_year int,
	tailnum text,
	year int,
	month int,
	day int,
	crs_deptime int,
	date timestamp,
	cancelled int,
	arr_delay int,
	dep_delay int,
	manufacturer text,
	primary key(creation_year, tailnum, year, month, day, crs_deptime)
);
