from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta

conn = connect(
    user=DB_CONFIG["username"],
    password=DB_CONFIG["password"],
    host=DB_CONFIG["host"],
    database=DB_CONFIG["database"],
    port=DB_CONFIG["port"]
)

cur = conn.cursor()


def clean(value):
    if value is None:
        return None
    return str(value).strip()


def split_name(full_name):
    parts = full_name.strip().split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""
    return first_name, last_name


def parse_address(address):
    """
    Expected format:
    123 Main St, Gainesville, FL 32601
    """
    parts = [p.strip() for p in address.split(",")]

    street_part = parts[0] if len(parts) > 0 else ""
    city = parts[1] if len(parts) > 1 else ""
    state_zip = parts[2] if len(parts) > 2 else ""

    street_tokens = street_part.split(" ", 1)
    street_number = street_tokens[0] if len(street_tokens) > 0 else ""
    street_name = street_tokens[1] if len(street_tokens) > 1 else ""

    state_zip_tokens = state_zip.split(" ", 1)
    state = state_zip_tokens[0] if len(state_zip_tokens) > 0 else ""
    zip_code = state_zip_tokens[1] if len(state_zip_tokens) > 1 else ""

    return street_number, street_name, city, state, zip_code


def add_item(new_item: Item = None):
    """
    Adds a new item to the item table.
    """
    if new_item is None:
        return
    try:
        cur.execute("SELECT COALESCE(MAX(i_item_sk), 0) + 1 FROM item")
        new_item_sk = cur.fetchone()[0]
        rec_start_date = f"{new_item.start_year}-01-01"
        cur.execute("""
            INSERT INTO item (
                i_item_sk,
                i_item_id,
                i_rec_start_date,
                i_product_name,
                i_brand,
                i_class,
                i_category,
                i_manufact,
                i_current_price,
                i_num_owned
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            new_item_sk,
            new_item.item_id,
            rec_start_date,
            new_item.product_name,
            new_item.brand,
            new_item.category,
            new_item.category,
            new_item.manufact,
            new_item.current_price,
            new_item.num_owned
        ))
    except Exception as e:
        conn.rollback()
        print(f"Error in add_item: {e}")


def add_customer(new_customer: Customer = None):
    """
    Adds a new customer and customer address.
    """
    if new_customer is None:
        return
    try:
        street_number, street_name, city, state, zip_code = parse_address(new_customer.address)
        cur.execute("SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address")
        new_addr_sk = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO customer_address (
                ca_address_sk,
                ca_street_number,
                ca_street_name,
                ca_city,
                ca_state,
                ca_zip
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            new_addr_sk,
            street_number,
            street_name,
            city,
            state,
            zip_code
        ))
        cur.execute("SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer")
        new_customer_sk = cur.fetchone()[0]
        first_name, last_name = split_name(new_customer.name)
        cur.execute("""
            INSERT INTO customer (
                c_customer_sk,
                c_customer_id,
                c_first_name,
                c_last_name,
                c_email_address,
                c_current_addr_sk
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            new_customer_sk,
            new_customer.customer_id,
            first_name,
            last_name,
            new_customer.email,
            new_addr_sk
        ))
    except Exception as e:
        conn.rollback()
        print(f"Error in add_customer: {e}")


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    Updates only the fields that are not None.
    """
    if new_customer is None:
        return
    try:
        customer_updates = []
        params = []
        if new_customer.customer_id is not None:
            customer_updates.append("c_customer_id = ?")
            params.append(new_customer.customer_id)

        if new_customer.name is not None:
            first_name, last_name = split_name(new_customer.name)
            customer_updates.append("c_first_name = ?")
            params.append(first_name)
            customer_updates.append("c_last_name = ?")
            params.append(last_name)

        if new_customer.email is not None:
            customer_updates.append("c_email_address = ?")
            params.append(new_customer.email)

        if customer_updates:
            query = "UPDATE customer SET " + ", ".join(customer_updates) + " WHERE c_customer_id = ?"
            params.append(original_customer_id)
            cur.execute(query, tuple(params))

        if new_customer.address is not None:
            lookup_id = new_customer.customer_id if new_customer.customer_id is not None else original_customer_id

            cur.execute("""
                SELECT c_current_addr_sk
                FROM customer
                WHERE c_customer_id = ?
            """, (lookup_id,))

            row = cur.fetchone()

            if row is not None:
                addr_sk = row[0]
                street_number, street_name, city, state, zip_code = parse_address(new_customer.address)

                cur.execute("""
                    UPDATE customer_address
                    SET ca_street_number = ?,
                        ca_street_name = ?,
                        ca_city = ?,
                        ca_state = ?,
                        ca_zip = ?
                    WHERE ca_address_sk = ?
                """, (
                    street_number,
                    street_name,
                    city,
                    state,
                    zip_code,
                    addr_sk
                ))
    except Exception as e:
        conn.rollback()
        print(f"Error in edit_customer: {e}")


def rent_item(item_id: str = None, customer_id: str = None):
    """
    Adds an active rental.
    rental_date = today
    due_date = today + 14 days
    """
    try:
        today = date.today()
        due = today + timedelta(days=14)
        cur.execute("""
            INSERT INTO rental (
                item_id,
                customer_id,
                rental_date,
                due_date
            )
            VALUES (?, ?, ?, ?)
        """, (
            item_id,
            customer_id,
            today,
            due
        ))
    except Exception as e:
        conn.rollback()
        print(f"Error in rent_item: {e}")


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Adds a customer to the waitlist.
    Returns the customer's new place in line.
    """
    try:
        new_place = line_length(item_id) + 1
        cur.execute("""
            INSERT INTO waitlist (
                item_id,
                customer_id,
                place_in_line
            )
            VALUES (?, ?, ?)
        """, (
            item_id,
            customer_id,
            new_place
        ))

        return new_place
    except Exception as e:
        conn.rollback()
        print(f"Error in waitlist_customer: {e}")
        return -1


def update_waitlist(item_id: str = None):
    """
    Removes the customer at position 1 and shifts everyone else down.
    """
    try:
        cur.execute("""
            DELETE FROM waitlist
            WHERE item_id = ?
              AND place_in_line = 1
        """, (item_id,))

        cur.execute("""
            UPDATE waitlist
            SET place_in_line = place_in_line - 1
            WHERE item_id = ?
              AND place_in_line > 1
        """, (item_id,))
    except Exception as e:
        conn.rollback()
        print(f"Error in update_waitlist: {e}")


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history.
    """
    try:
        cur.execute("""
            SELECT rental_date, due_date
            FROM rental
            WHERE item_id = ?
              AND customer_id = ?
        """, (
            item_id,
            customer_id
        ))

        rental_row = cur.fetchone()

        if rental_row is None:
            return

        rental_date = rental_row[0]
        due_date = rental_row[1]
        return_date = date.today()

        cur.execute("""
            INSERT INTO rental_history (
                item_id,
                customer_id,
                rental_date,
                due_date,
                return_date
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            item_id,
            customer_id,
            rental_date,
            due_date,
            return_date
        ))

        cur.execute("""
            DELETE FROM rental
            WHERE item_id = ?
              AND customer_id = ?
        """, (
            item_id,
            customer_id
        ))
    except Exception as e:
        conn.rollback()
        print(f"Error in return_item: {e}")


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due date of an active rental.
    """
    try:
        cur.execute("""
            UPDATE rental
            SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY)
            WHERE item_id = ?
              AND customer_id = ?
        """, (
            item_id,
            customer_id
        ))
    except Exception as e:
        conn.rollback()
        print(f"Error in grant_extension: {e}")


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns Item objects matching the filters.

    Important:
    The TPC-DS item table may contain duplicate i_item_id rows with different
    start dates. This query keeps only the newest row for each i_item_id.
    """
    query = """
        SELECT i.i_item_id,
               i.i_product_name,
               i.i_brand,
               i.i_category,
               i.i_manufact,
               i.i_current_price,
               YEAR(i.i_rec_start_date),
               i.i_num_owned
        FROM item i
        JOIN (
            SELECT i_item_id, MAX(i_rec_start_date) AS max_start_date
            FROM item
            GROUP BY i_item_id
        ) latest
          ON i.i_item_id = latest.i_item_id
         AND i.i_rec_start_date = latest.max_start_date
        WHERE 1=1
    """

    params = []

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            query += " AND i.i_item_id LIKE ?" if use_patterns else " AND i.i_item_id = ?"
            params.append(filter_attributes.item_id)

        if filter_attributes.product_name is not None:
            query += " AND i.i_product_name LIKE ?" if use_patterns else " AND i.i_product_name = ?"
            params.append(filter_attributes.product_name)

        if filter_attributes.brand is not None:
            query += " AND i.i_brand LIKE ?" if use_patterns else " AND i.i_brand = ?"
            params.append(filter_attributes.brand)

        if filter_attributes.category is not None:
            query += " AND i.i_category LIKE ?" if use_patterns else " AND i.i_category = ?"
            params.append(filter_attributes.category)

        if filter_attributes.manufact is not None:
            query += " AND i.i_manufact LIKE ?" if use_patterns else " AND i.i_manufact = ?"
            params.append(filter_attributes.manufact)

    if min_price != -1:
        query += " AND i.i_current_price >= ?"
        params.append(min_price)

    if max_price != -1:
        query += " AND i.i_current_price <= ?"
        params.append(max_price)

    if min_start_year != -1:
        query += " AND YEAR(i.i_rec_start_date) >= ?"
        params.append(min_start_year)

    if max_start_year != -1:
        query += " AND YEAR(i.i_rec_start_date) <= ?"
        params.append(max_start_year)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    items = []

    for row in rows:
        items.append(Item(
            item_id=clean(row[0]),
            product_name=clean(row[1]),
            brand=clean(row[2]),
            category=clean(row[3]),
            manufact=clean(row[4]),
            current_price=float(row[5]),
            start_year=int(row[6]),
            num_owned=int(row[7])
        ))

    return items


def get_filtered_customers(filter_attributes: Customer = None,
                           use_patterns: bool = False) -> list[Customer]:
    """
    Returns Customer objects matching the filters.
    """
    full_name_expr = "CONCAT(TRIM(c.c_first_name), ' ', TRIM(c.c_last_name))"

    full_addr_expr = """
        CONCAT(
            TRIM(ca.ca_street_number), ' ',
            TRIM(ca.ca_street_name), ', ',
            TRIM(ca.ca_city), ', ',
            TRIM(ca.ca_state), ' ',
            TRIM(ca.ca_zip)
        )
    """

    query = f"""
        SELECT c.c_customer_id,
               {full_name_expr},
               {full_addr_expr},
               c.c_email_address
        FROM customer c
        JOIN customer_address ca
          ON c.c_current_addr_sk = ca.ca_address_sk
        WHERE 1=1
    """

    params = []

    if filter_attributes is not None:
        if filter_attributes.customer_id is not None:
            query += " AND c.c_customer_id LIKE ?" if use_patterns else " AND c.c_customer_id = ?"
            params.append(filter_attributes.customer_id)

        if filter_attributes.name is not None:
            query += f" AND {full_name_expr} LIKE ?" if use_patterns else f" AND {full_name_expr} = ?"
            params.append(filter_attributes.name)

        if filter_attributes.address is not None:
            query += f" AND {full_addr_expr} LIKE ?" if use_patterns else f" AND {full_addr_expr} = ?"
            params.append(filter_attributes.address)

        if filter_attributes.email is not None:
            query += " AND c.c_email_address LIKE ?" if use_patterns else " AND c.c_email_address = ?"
            params.append(filter_attributes.email)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    customers = []

    for row in rows:
        customers.append(Customer(
            customer_id=clean(row[0]),
            name=clean(row[1]),
            address=clean(row[2]),
            email=clean(row[3])
        ))

    return customers


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns Rental objects matching the filters.
    """
    query = """
        SELECT item_id,
               customer_id,
               rental_date,
               due_date
        FROM rental
        WHERE 1=1
    """

    params = []

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            query += " AND item_id = ?"
            params.append(filter_attributes.item_id)

        if filter_attributes.customer_id is not None:
            query += " AND customer_id = ?"
            params.append(filter_attributes.customer_id)

        if filter_attributes.rental_date is not None:
            query += " AND rental_date = ?"
            params.append(filter_attributes.rental_date)

        if filter_attributes.due_date is not None:
            query += " AND due_date = ?"
            params.append(filter_attributes.due_date)

    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        params.append(min_rental_date)

    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        params.append(max_rental_date)

    if min_due_date is not None:
        query += " AND due_date >= ?"
        params.append(min_due_date)

    if max_due_date is not None:
        query += " AND due_date <= ?"
        params.append(max_due_date)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    rentals = []

    for row in rows:
        rentals.append(Rental(
            item_id=clean(row[0]),
            customer_id=clean(row[1]),
            rental_date=str(row[2]),
            due_date=str(row[3])
        ))

    return rentals


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns RentalHistory objects matching the filters.
    """
    query = """
        SELECT item_id,
               customer_id,
               rental_date,
               due_date,
               return_date
        FROM rental_history
        WHERE 1=1
    """

    params = []

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            query += " AND item_id = ?"
            params.append(filter_attributes.item_id)

        if filter_attributes.customer_id is not None:
            query += " AND customer_id = ?"
            params.append(filter_attributes.customer_id)

        if filter_attributes.rental_date is not None:
            query += " AND rental_date = ?"
            params.append(filter_attributes.rental_date)

        if filter_attributes.due_date is not None:
            query += " AND due_date = ?"
            params.append(filter_attributes.due_date)

        if filter_attributes.return_date is not None:
            query += " AND return_date = ?"
            params.append(filter_attributes.return_date)

    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        params.append(min_rental_date)

    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        params.append(max_rental_date)

    if min_due_date is not None:
        query += " AND due_date >= ?"
        params.append(min_due_date)

    if max_due_date is not None:
        query += " AND due_date <= ?"
        params.append(max_due_date)

    if min_return_date is not None:
        query += " AND return_date >= ?"
        params.append(min_return_date)

    if max_return_date is not None:
        query += " AND return_date <= ?"
        params.append(max_return_date)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    histories = []

    for row in rows:
        histories.append(RentalHistory(
            item_id=clean(row[0]),
            customer_id=clean(row[1]),
            rental_date=str(row[2]),
            due_date=str(row[3]),
            return_date=str(row[4])
        ))

    return histories


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns Waitlist objects matching the filters.
    """
    query = """
        SELECT item_id,
               customer_id,
               place_in_line
        FROM waitlist
        WHERE 1=1
    """

    params = []

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            query += " AND item_id = ?"
            params.append(filter_attributes.item_id)

        if filter_attributes.customer_id is not None:
            query += " AND customer_id = ?"
            params.append(filter_attributes.customer_id)

        if filter_attributes.place_in_line is not None and filter_attributes.place_in_line != -1:
            query += " AND place_in_line = ?"
            params.append(filter_attributes.place_in_line)

    if min_place_in_line != -1:
        query += " AND place_in_line >= ?"
        params.append(min_place_in_line)

    if max_place_in_line != -1:
        query += " AND place_in_line <= ?"
        params.append(max_place_in_line)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    waitlist_entries = []

    for row in rows:
        waitlist_entries.append(Waitlist(
            item_id=clean(row[0]),
            customer_id=clean(row[1]),
            place_in_line=int(row[2])
        ))

    return waitlist_entries


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals.
    Returns -1 if the item does not exist.

    Uses latest item row because item IDs may appear more than once.
    """
    cur.execute("""
        SELECT i_num_owned
        FROM item
        WHERE i_item_id = ?
        ORDER BY i_rec_start_date DESC
        LIMIT 1
    """, (item_id,))

    item_row = cur.fetchone()

    if item_row is None:
        return -1

    num_owned = int(item_row[0])

    cur.execute("""
        SELECT COUNT(*)
        FROM rental
        WHERE item_id = ?
    """, (item_id,))

    rented_count = int(cur.fetchone()[0])

    return num_owned - rented_count


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place in line.
    Returns -1 if not on waitlist.
    """
    cur.execute("""
        SELECT place_in_line
        FROM waitlist
        WHERE item_id = ?
          AND customer_id = ?
    """, (
        item_id,
        customer_id
    ))

    row = cur.fetchone()

    if row is None:
        return -1

    return int(row[0])


def line_length(item_id: str = None) -> int:
    """
    Returns the number of customers on the waitlist for an item.
    """
    cur.execute("""
        SELECT COUNT(*)
        FROM waitlist
        WHERE item_id = ?
    """, (item_id,))

    row = cur.fetchone()

    if row is None:
        return 0

    return int(row[0])


def save_changes():
    """
    Commits all changes made to the database.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    cur.close()
    conn.close()

# Enter Item ID: AAAAAAAAOEGEAAAA
# Enter Customer ID: AAAAAAAAAKGIBAAA
