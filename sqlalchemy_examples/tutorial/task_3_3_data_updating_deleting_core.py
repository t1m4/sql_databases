from sqlalchemy import update, bindparam, delete

from task_2_metadata import User, engine


# Updating using update
stmt = (
    update(User).where(User.name == 'patrick').
        values(fullname='Patrick the Star')
)
with engine.begin() as conn:
    result = conn.execute(stmt)
    print(result)

# updating many
stmt = (
    update(User).
        where(User.name == bindparam('oldname')).
        values(name=bindparam('newname'))
)
with engine.begin() as conn:
    result = conn.execute(
        stmt,
        [
            {'oldname': 'jack', 'newname': 'ed'},
            {'oldname': 'wendy', 'newname': 'mary'},
            {'oldname': 'jim', 'newname': 'jake'},
        ]
    )
    print(result.rowcount)

# delete
stmt = delete(User).where(User.name == 'patrick')
print(stmt)
