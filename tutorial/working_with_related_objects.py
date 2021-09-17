from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload, joinedload

from tutorial.working_with_metadata import User, Address, engine

u1 = User(name='pkrabs', fullname='Pearl Krabs')
u2 = User(name='Ruslan', fullname='Ruslan Khamidullin')
print(u1.addresses)
a1 = Address(email_address="pearl.krabs@gmail.com")
u1.addresses.append(a1)
print(u1.addresses)
a2 = Address(email_address="pearl@aol.com", user=u1)
print("First" ,u1.addresses)
u2.addresses.append(a2)
print("First" ,u1.addresses)
print("Second", u2.addresses)
# print(u1.addresses, u2.addresses[0])

with Session(engine) as session:
    session.add(a1)
    session.add(u2)
    print(u1.id)
    print(session.execute(select(User)).all())
    session.commit()



print("\nANOTHER SELECT LOAD Strategies")
with Session(engine) as session:
    for user_obj in session.execute(select(User).options(selectinload(User.addresses))).scalars():
        print(user_obj, user_obj.addresses)


print("\nANOTHER JOIN LOAD Strategies")
with Session(engine) as session:
    stmt = select(Address).options(joinedload(Address.user, innerjoin=True)).order_by(Address.id)
    for row in session.execute(stmt).scalars():
        print(row, row.user)

