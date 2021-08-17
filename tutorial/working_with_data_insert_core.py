from sqlalchemy import insert, text, select, bindparam

from tutorial.working_with_metadata import User, engine, Address

INSERT = ''
# insert single
stmt = insert(User).values(name='spongebob', fullname="Spongebob Squarepants")
compiled = stmt.compile()
print(compiled.params)
with engine.connect() as conn:
    result = conn.execute(stmt)
    conn.commit()
    print(result.inserted_primary_key)

# insert many
with engine.connect() as conn:
    result = conn.execute(
        insert(User),
        [
            {"name": "sandy", "fullname": "Sandy Cheeks"},
            {"name": "patrick", "fullname": "Patrick Star"}
        ]
    )
    conn.commit()

# fetch rows
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM user_account"))
    for i in result:
        print(i)

# insert ... from select
select_stmt = select(User.id, User.name + "@aol.com")
insert_stmt = insert(Address).from_select(
    ["user_id", "email_address"], select_stmt
)
print(insert_stmt)

with engine.connect() as conn:
    select_stmt = select(User.id, User.name)
    result = conn.execute(select_stmt)
    print(result)
    for i in result:
        print(i)

# returning
insert_stmt = insert(Address).returning(Address.id, Address.email_address)
print(insert_stmt)

# inserting related objects
scalar_subquery = (
    select(User.id).
        where(User.name == bindparam('username')).
        scalar_subquery()
)

with engine.connect() as conn:
    result = conn.execute(
        insert(Address).values(user_id=scalar_subquery),
        [
            {"username": 'spongebob', "email_address": "spongebob@sqlalchemy.org"},
            {"username": 'sandy', "email_address": "sandy@sqlalchemy.org"},
            {"username": 'sandy', "email_address": "sandy@squirrelpower.org"},
        ]
    )
    conn.commit()
