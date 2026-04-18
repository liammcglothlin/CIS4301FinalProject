from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
    raise NotImplementedError("you must implement this function")


def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    raise NotImplementedError("you must implement this function")


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    raise NotImplementedError("you must implement this function")


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    raise NotImplementedError("you must implement this function")


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    raise NotImplementedError("you must implement this function")

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    raise NotImplementedError("you must implement this function")


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    raise NotImplementedError("you must implement this function")


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    query = """
        SELECT i_item_id, i_product_name, i_brand, i_category,
               i_manufact, i_current_price, YEAR(i_rec_start_date), i_num_owned
        FROM item
        WHERE 1=1
    """
    params = []

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            if use_patterns:
                query += " AND i_item_id LIKE ?"
            else:
                query += " AND i_item_id = ?"
            params.append(filter_attributes.item_id)

        if filter_attributes.product_name is not None:
            if use_patterns:
                query += " AND i_product_name LIKE ?"
            else:
                query += " AND i_product_name = ?"
            params.append(filter_attributes.product_name)

        if filter_attributes.brand is not None:
            if use_patterns:
                query += " AND i_brand LIKE ?"
            else:
                query += " AND i_brand = ?"
            params.append(filter_attributes.brand)

        if filter_attributes.category is not None:
            if use_patterns:
                query += " AND i_category LIKE ?"
            else:
                query += " AND i_category = ?"
            params.append(filter_attributes.category)

        if filter_attributes.manufact is not None:
            if use_patterns:
                query += " AND i_manufact LIKE ?"
            else:
                query += " AND i_manufact = ?"
            params.append(filter_attributes.manufact)

    if min_price != -1:
        query += " AND i_current_price >= ?"
        params.append(min_price)

    if max_price != -1:
        query += " AND i_current_price <= ?"
        params.append(max_price)

    if min_start_year != -1:
        query += " AND YEAR(i_rec_start_date) >= ?"
        params.append(min_start_year)

    if max_start_year != -1:
        query += " AND YEAR(i_rec_start_date) <= ?"
        params.append(max_start_year)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    items = []
    for row in rows:
        item = Item(
            item_id=row[0].strip(),
            product_name=row[1].strip(),
            brand=row[2].strip(),
            category=row[3].strip(),
            manufact=row[4].strip(),
            current_price=float(row[5]),
            start_year=int(row[6]),
            num_owned=int(row[7])
        )
        items.append(item)

    return items


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    query = """
        SELECT c.c_customer_id,
               CONCAT(c.c_first_name, ' ', c.c_last_name) AS full_name,
               CONCAT(ca.ca_street_number, ' ', ca.ca_street_name, ', ',
                      ca.ca_city, ', ', ca.ca_state, ' ', ca.ca_zip) AS full_address,
               c.c_email_address
        FROM customer c
        JOIN customer_address ca
          ON c.c_current_addr_sk = ca.ca_address_sk
        WHERE 1=1
    """
    params = []

    if filter_attributes is not None:
        if filter_attributes.customer_id is not None:
            if use_patterns:
                query += " AND c.c_customer_id LIKE ?"
            else:
                query += " AND c.c_customer_id = ?"
            params.append(filter_attributes.customer_id)

        if filter_attributes.name is not None:
            if use_patterns:
                query += " AND CONCAT(c.c_first_name, ' ', c.c_last_name) LIKE ?"
            else:
                query += " AND CONCAT(c.c_first_name, ' ', c.c_last_name) = ?"
            params.append(filter_attributes.name)

        if filter_attributes.address is not None:
            full_addr_expr = """CONCAT(ca.ca_street_number, ' ', ca.ca_street_name, ', ',
                                       ca.ca_city, ', ', ca.ca_state, ' ', ca.ca_zip)"""
            if use_patterns:
                query += f" AND {full_addr_expr} LIKE ?"
            else:
                query += f" AND {full_addr_expr} = ?"
            params.append(filter_attributes.address)

        if filter_attributes.email is not None:
            if use_patterns:
                query += " AND c.c_email_address LIKE ?"
            else:
                query += " AND c.c_email_address = ?"
            params.append(filter_attributes.email)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    customers = []
    for row in rows:
        customer = Customer(
            customer_id=row[0].strip(),
            name=row[1].strip(),
            address=row[2].strip(),
            email=row[3].strip()
        )
        customers.append(customer)

    return customers


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    query_item = "SELECT i_num_owned FROM item WHERE i_item_id = ?"
    cur.execute(query_item, (item_id,))
    item_row = cur.fetchone()

    if item_row is None:
        return -1

    num_owned = item_row[0]

    query_rented = "SELECT COUNT(*) FROM rental WHERE item_id = ?"
    cur.execute(query_rented, (item_id,))
    rented_row = cur.fetchone()
    num_rented = rented_row[0] if rented_row is not None else 0

    return num_owned - num_rented


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    query = "SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (item_id, customer_id))
    result = cur.fetchone()
    return result[0] if result is not None else -1


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    query = "SELECT COUNT(*) FROM waitlist WHERE item_id = ?"
    cur.execute(query, (item_id,))
    result = cur.fetchone()
    return result[0] if result is not None else 0


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    cur.close()
    conn.close()

