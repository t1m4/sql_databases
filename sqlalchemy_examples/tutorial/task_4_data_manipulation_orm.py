from sqlalchemy import select, update, delete
from sqlalchemy.orm import Session

from task_2_metadata import User, engine

print('\n')
with Session(engine) as session:

    # updating
    # create python object as database row. Object have NEW status
    squidward = User(name="squidward", fullname="Squidward Tentacles")
    krabs = User(name="ehkrabs", fullname="Eugene H. Krabs")
    sandy = User(name="sandy", fullname="Sandy H. Krabs")
    # create session
    session = Session(engine)
    # add object to the session. Object have PENDING status
    session.add(squidward)
    session.add(krabs)
    session.add(sandy)
    # flush session. Create objects in database
    session.flush()
    # ...
    # now transaction open until you call .commit() .rollback() .close()
    # now object have PERSISTENT status
    # now we commit transaction
    # session.commit()
    session.rollback()
    print(sandy in session)

# Updating
with Session(engine) as session:
    sandy = session.execute(select(User).filter_by(name="sandy")).scalar_one()
    print("Before", session.dirty)  # False
    # autoflush using and put in session.dirty
    sandy.fullname = "Sandy Squirrel"
    print("After", session.dirty)  # True
    # after query session send change from dirty objects
    sandy_fullname = session.execute(
        select(User.fullname).where(User.id == 3)
    ).scalar_one()
    print(sandy_fullname)

    session.execute(
        update(User).
            where(User.name == "sandy").
            values(fullname="Sandy Squirrel Extraordinaire")
    )
    # synchronize_session option change fullname of sandy object.
    print(sandy.fullname)


    print("\n Deleting ")
    patrick = session.get(User, 2)
    session.delete(patrick)
    print(patrick)
    session.execute(select(User).where(User.name == "patrick")).first()

    # ORM enabled delete
    session.execute(delete(User).where(User.name == "squidward"))

