import duckdb


def create_agregate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


def agregate_dim_city():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)

def agregate_dim_station():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT
        ID,
        CODE,
        NAME,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITTY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql_statement)


def agregate_fact_station_statement():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    sql_statement = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
    SELECT
        css.STATION_ID,
        cs.CITY_CODE AS CITY_ID,
        css.BICYCLE_DOCKS_AVAILABLE,
        css.BICYCLE_AVAILABLE,
        css.LAST_STATEMENT_DATE,
        css.CREATED_DATE
    FROM CONSOLIDATE_STATION_STATEMENT css
    INNER JOIN CONSOLIDATE_STATION cs
        ON css.STATION_ID = cs.ID
        AND css.CREATED_DATE = cs.CREATED_DATE
    WHERE css.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION_STATEMENT);
    """

    con.execute(sql_statement)