DROP TABLE IF EXISTS weather;

CREATE TABLE IF NOT EXISTS weather (

	city_id integer NOT NULL,
	city_name varchar(25) NOT NULL,
	country char(2) NOT NULL,
	temperature decimal(3,1) NOT NULL,
	temperature_min decimal(3,1) NOT NULL,
	temperature_max decimal(3,1) NOT NULL,
	humidity integer NOT NULL,
	pressure integer NOT NULL,
	wind_speed decimal(4,2) NOT NULL,
	cloudiness integer NOT NULL,
	rain_last_3h decimal (4,2),
	query_timestamp date NOT NULL,
	PRIMARY KEY (city_id, query_timestamp)
)