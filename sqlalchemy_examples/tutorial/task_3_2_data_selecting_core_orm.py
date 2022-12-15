from sqlalchemy import select, text, and_, or_, func, union_all
from sqlalchemy.orm import Session, aliased

from task_2_metadata import User, engine, Address

# select with where and connection
stmt = select(User).where(User.name == 'spongebob')
with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)

# select with where and session. Get full user
with Session(engine) as session:
    for row in session.execute(stmt):
        print(row)

# selecting individual Column and one result
print(select(User.id, User.name))
with Session(engine) as session:
    row = session.execute(select(User.name, User.fullname)).first()
    print(row)

# selecting all row with related objects
with Session(engine) as session:
    result = session.execute(
        select(User.name, Address).
            where(User.id == Address.user_id).
            order_by(Address.id)
    ).all()
    print(result)

# selecting with labels
stmt = (
    select(("Username: " + User.name).label("username"), ).order_by(User.name)
)
with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(f"{row.username}")

# selecting with some user text
stmt = (
    select(
        text("'some phrase'"), User.name
    ).order_by(User.name)
)
with engine.connect() as conn:
    print(conn.execute(stmt).all())

# selecting with where AND/OR
print(select(User).where(and_(User.name == 'squidward', User.id == 1)))  # AND
print(select(User).where(or_(User.name == 'squidward', User.id == 1)))  # OR
print(select(User).filter_by(name='spongebob', fullname='Spongebob Squarepants'))

# selecting with JOIN
print(select(User.name, Address.email_address))
print(select(User.name, Address.email_address).join_from(User, Address))
print(select(User.name, Address.email_address).join(Address))
print(select(Address.email_address).select_from(User).join(Address))
print(select(func.count('*')).select_from(User))
# provide explicit ON
print(select(Address.email_address).
      select_from(User).
      join(Address, User.id == Address.user_id)
      )

# aggregate with group by and having
print("\n")
with engine.connect() as conn:
    result = conn.execute(
        select(User.name, func.count(Address.id).label("count")).
            join(Address).
            group_by(User.name).
            having(func.count(Address.id) > 1)
    )
    print(result.all())

# using aliases for tables
user_alias_1 = aliased(User)
user_alias_2 = aliased(User)
print(
    select(user_alias_1.name, user_alias_2.name).
        join_from(user_alias_1, user_alias_2, user_alias_1.id > user_alias_2.id)
)

# working with subquery
print('\n')
subq = select(
    func.count(Address.id).label("count"),
    Address.user_id
).group_by(Address.user_id).subquery()
print(select(subq.c.user_id, subq.c.count))

stmt = select(
    User.name,
    User.fullname,
    subq.c.count
).join_from(User, subq)
print(stmt)

# working with cte
print('\n')
subq = select(
    func.count(Address.id).label("count"),
    Address.user_id
).group_by(Address.user_id).cte()
stmt = select(
    User.name,
    User.fullname,
    subq.c.count
).join_from(User, subq)
print(stmt)

# Union and Union all
stmt1 = select(User).where(User.name == 'sandy')
stmt2 = select(User).where(User.name == 'spongebob')
u = union_all(stmt1, stmt2)
with engine.connect() as conn:
    result = conn.execute(u)
    print(result.all())

# Exists
subq = (
    select(func.count(Address.id)).
        where(User.id == Address.user_id).
        group_by(Address.user_id).
        having(func.count(Address.id) > 1)
).exists()
with engine.connect() as conn:
    result = conn.execute(
        select(User.name).where(subq)
    )
    print(result.all())
