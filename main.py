from sqlalchemy import create_engine, MetaData, Float, String, Table, Column

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

conn = engine.connect()
result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()

for row in result:
   print(row)