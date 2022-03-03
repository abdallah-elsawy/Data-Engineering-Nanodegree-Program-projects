class SqlQueries:


    immigration_table_insert = ("""
        SELECT distinct cicid ,i94yr  ,i94mon  ,i94cit  ,i94res  ,i94port ,arrdate ,i94mode  ,i94addr  ,depdate ,i94bir ,i94visa ,count  ,visapost ,occup ,entdepa ,entdepd ,entdepu  ,gender ,airline,visatype
        FROM immigration
    """)

    temperature_table_insert = ("""
        SELECT distinct dt, AverageTemperature, AverageTemperatureUncertainty, City, Country
        FROM temperature
 
