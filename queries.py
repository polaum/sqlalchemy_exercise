from db_mapper import load_session, Region, Employee, Order, Customer, OrderDetails, Product
from sqlalchemy import desc, or_, and_, func
import datetime


def query1():
    session = load_session()
    res = session.query(Region).all()
    for result in res:
        print(result.region_id, result.region_description)


def query2():
    session = load_session()
    res = session.query(Employee.first_name, Employee.last_name)
    for result in res:
        print(result[0], result[1])


def query3():
    session = load_session()
    res = session.query(Employee.first_name, Employee.last_name).order_by(Employee.last_name)
    for result in res:
        print(result[0], result[1])


def query4():
    session = load_session()
    res = session.query(Order.order_id, Order.order_date, Order.shipped_date, Order.customer_id, Order.freight)\
        .order_by(desc(Order.freight))
    for result in res:
        print(result)


def query5():
    session = load_session()
    res = session.query(Employee.title, Employee.first_name, Employee.last_name).filter(Employee.title == 'Sales Representative')
    for result in res:
        print(result)


def query6():
    session = load_session()
    res = session.query(Employee.first_name, Employee.last_name).filter(
        Employee.region != 'null')
    for result in res:
        print(result)


def query7():
    session = load_session()
    res = session.query(Employee.first_name, Employee.last_name).filter(
        Employee.last_name >= 'M').order_by(desc(Employee.last_name))
    for result in res:
        print(result)


def query8():
    session = load_session()
    res = session.query(Employee.title_of_courtesy, Employee.first_name, Employee.last_name).\
        filter(Employee.title_of_courtesy.like('M%'))
    for result in res:
        print(result)


def query9():
    session = load_session()
    res = session.query(Employee.first_name, Employee.last_name). \
        filter(Employee.title == 'Sales Representative', or_(Employee.city == 'Seattle', Employee.city == 'Redmond'))
    for result in res:
        print(result)


def query10():
    session = load_session()
    res = session.query(Customer.company_name, Customer.contact_title, Customer.city, Customer.country). \
        filter(or_(Customer.country == 'Mexico', and_(Customer.country == 'Spain', Customer.city != 'Madrid')))
    for result in res:
        print(result)


def query11():
    session = load_session()
    res = session.query(Order.order_id, Order.freight, Order.freight*1.1).filter(Order.freight >= 500.00)
    for result in res:
        print(result)


def query12():
    session = load_session()
    res = session.query(func.sum(OrderDetails.quantity)).filter(OrderDetails.product_id == 3)
    for result in res:
        print(result[0])


def query13():
    session = load_session()
    res = session.query(Employee.city, func.count(Employee.first_name)).group_by(Employee.city)
    for result in res:
        print(result)


def query14():
    session = load_session()
    res = session.query(Employee.city, func.count(Employee.first_name)).filter(Employee.title == 'Sales Representative').\
        group_by(Employee.city).having(func.count(Employee.first_name) >= 2).order_by(func.count(Employee.first_name))
    for result in res:
        print(result)


def query15():
    session = load_session()
    res = session.query(Customer.company_name).join(Order).\
        filter(Order.order_date >= datetime.date(1997, 1, 1), Order.order_date <= datetime.date(1997, 12, 31))
    for result in res:
        print(result)


def query16():
    session = load_session()
    res = session.query(Employee.first_name, Employee.last_name, Order.order_id).join(Order).\
        order_by(Employee.last_name)
    for result in res:
        print(result)


def query17():
    session = load_session()
    if __name__ == '__main__':
        res = session.query(Order.order_id, Customer.company_name, Employee.first_name, Employee.last_name).\
            join(Employee).join(Customer).\
            filter(Order.order_date >= datetime.date(1998, 1, 1), Order.shipped_date > Order.required_date).\
            order_by(Customer.company_name)
    for result in res:
        print(result)


def query18():
    session = load_session()
    res = session.query(Product.product_name, func.sum(OrderDetails.quantity)).join(OrderDetails).\
        group_by(Product.product_name).having(func.sum(OrderDetails.quantity) < 200)
    for result in res:
        print(result)
