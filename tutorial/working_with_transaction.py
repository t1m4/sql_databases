from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
print(engine)

# using engine
with engine.connect() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())

# using Session object
stmt = text("SELECT x, y FROM some_table WHERE y > :y ORDER BY x, y").bindparams(y=6)
with Session(engine) as session:
    result = session.execute(stmt)
    for row in result:
        print(f"x: {row.x}  y: {row.y}")
