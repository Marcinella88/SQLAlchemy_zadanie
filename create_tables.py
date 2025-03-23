from sqlalchemy import Table, Column, Float, Integer, String, MetaData
from sqlalchemy import create_engine

engine = create_engine('sqlite:///database.db')

meta = MetaData()

stations = Table(
    "stations",
    meta,
    Column("station", String, primary_key = True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String), 
)

measure = Table(
    "measure",
    meta,
    Column("station", String),
    Column("date", String),
    Column("precip", Float),
    Column("tobs", Integer),
)

meta.create_all(engine)
print(engine.table_names())