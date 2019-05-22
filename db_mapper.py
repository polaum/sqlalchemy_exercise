from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker


class Supplier(object):
    pass


class Employee(object):
    pass


class Shipper(object):
    pass


class Product(object):
    pass


class EmployeeTerritory(object):
    pass


class Region(object):
    pass


class CustomerDemographic(object):
    pass


class UsState(object):
    pass


class OrderDetails(object):
    pass


class Category(object):
    pass


class Territory(object):
    pass


class CustomerCustomerDemo(object):
    pass


class Customer(object):
    pass


class Order(object):
    pass


def create_table(table_name:str, metadata):
    return Table(table_name, metadata, autoload=True)


def load_session():
    engine = create_engine('postgresql://localhost: 5432/northwind', echo=True)
    metadata = MetaData(engine)
    regions = create_table('region', metadata)
    suppliers = create_table('suppliers', metadata)
    employees = create_table('employees', metadata)
    shippers = create_table('shippers', metadata)
    products = create_table('products', metadata)
    employee_territories = create_table('employee_territories', metadata)
    customer_demographics = create_table('customer_demographics', metadata)
    us_states = create_table('us_states', metadata)
    order_details = create_table('order_details', metadata)
    categories = create_table('categories', metadata)
    territories = create_table('territories', metadata)
    customer_customer_demo = create_table('customer_customer_demo', metadata)
    customers = create_table('customers', metadata)
    orders = create_table('orders', metadata)
    mapper(Region, regions)
    mapper(Supplier, suppliers)
    mapper(Employee, employees)
    mapper(Shipper, shippers)
    mapper(Product, products)
    mapper(EmployeeTerritory, employee_territories)
    mapper(CustomerDemographic, customer_demographics)
    mapper(UsState, us_states)
    mapper(OrderDetails, order_details)
    mapper(Category, categories)
    mapper(Territory, territories)
    mapper(CustomerCustomerDemo, customer_customer_demo)
    mapper(Customer, customers)
    mapper(Order, orders)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


if __name__ == "__main__":
    session = load_session()
    res = session.query(Region).all()
    res2 = session.query(Employee).all()
    for result in res:
        print(result.region_id, result.region_description)
    for result in res2:
        print(result.last_name, result.first_name)




