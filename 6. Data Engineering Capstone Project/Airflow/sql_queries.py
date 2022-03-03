import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('config.cfg')

# DROP TABLES
immigration_table_drop = "DROP TABLE IF EXISTS immigration"
temperature_table_drop = "DROP TABLE IF EXISTS temperature "


# CREATE TABLES

immigration_table_create= ("""
 CREATE TABLE IF NOT EXISTS immigration (    
                            cicid FLOAT PRIMARY KEY,
                            i94yr FLOAT SORTKEY,
                            i94mon FLOAT DISTKEY,
                            i94cit FLOAT ,
                            i94res FLOAT ,
                            i94port CHAR,
                            arrdate FLOAT,
                            i94mode FLOAT ,
                            i94addr VARCHAR ,
                            depdate FLOAT,
                            i94bir FLOAT,
                            i94visa FLOAT,
                            count FLOAT,
                            dtadfile VARCHAR,
                            visapost CHAR,
                            occup CHAR,
                            entdepa CHAR,
                            entdepd CHAR,
                            entdepu CHAR,
                            matflag CHAR,
                            biryear FLOAT,
                            dtaddto VARCHAR,
                            gender CHAR,
                            insnum VARCHAR,
                            airline VARCHAR,
                            admnum FLOAT,
                            fltno VARCHAR,
                            visatype VARCHAR
                                            );
                                            """

temperature_table_create = ("""
 CREATE TABLE IF NOT EXISTS temperature (    

                            dt DATE,
                            AverageTemperature FLOAT,
                            AverageTemperatureUncertainty FLOAT,
                            City VARCHAR,
                            Country VARCHAR,
                            Latitude VARCHAR,
                            Longitude VARCHAR
                              )
                           """)




# QUERY LISTS

create_table_queries = [immigration ,temperature ]
drop_table_queries = [immigration ,temperature]

