import sqlite3
from employee_crud.exceptions import DataError
from employee_crud.server import get_logger

logger = get_logger()

# using context manager to close connection even when unexpected errors occurred
class DbConnection:
    def __enter__(self):
        # Create a SQL connection to our SQLite database
        self.con = sqlite3.connect("employee1.sqlite")
        return self.con

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()


def create_employee_table():
    create_sql_statement = """
    CREATE TABLE IF NOT EXISTS employee (
        emp_id integer NOT NULL UNIQUE,
        email text NOT NULL UNIQUE,
        name text NOT NULL,
        mobile_number NULL,
        gender text NOT NULL,
        company text NOT NULL,
        manager text NOT NULL
    )
    """
    with DbConnection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_sql_statement)


def drop_employee_table():
    create_sql_statement = """
        DROP table employee
        """
    with DbConnection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_sql_statement)


class EmployeeModel:
    """
    Employee table crud operations
    """

    def get_by_id(self, id):
        sql_query = "select * from employee where emp_id={id}"
        sql_query = sql_query.format(id=id)

        with DbConnection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            emp_record = cursor.fetchone()
            if emp_record:
                data = {}
                data['emp_id'] = emp_record[0]
                data['email'] = emp_record[1]
                data['name'] = emp_record[2]
                data['mobile_number'] = emp_record[3]
                data['gender'] = emp_record[4]
                data['company'] = emp_record[5]
                data['manager'] = emp_record[6]
                return data

            return {}

    def get_all_employees(self):
        sql_query = "select * from employee"
        sql_query = sql_query.format(id=id)

        employee_output = []
        with DbConnection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            emp_records = cursor.fetchall()
            if emp_records:
                for emp_record in emp_records:
                    data = {}
                    data['emp_id'] = emp_record[0]
                    data['email'] = emp_record[1]
                    data['name'] = emp_record[2]
                    data['mobile_number'] = emp_record[3]
                    data['gender'] = emp_record[4]
                    data['company'] = emp_record[5]
                    data['manager'] = emp_record[6]

                    employee_output.append(data)

        return employee_output

    def update(self, employee_data):
        sql_update_statement = """
        UPDATE employee set name='{name}', 
                            email='{email}', 
                            mobile_number='{mobile_number}',
                            gender='{gender}',
                            company='{company}',
                            manager='{manager}'
                            WHERE emp_id={emp_id}
        """

        # bind values with sql statement
        sql_update_statement = sql_update_statement.format(**employee_data)

        try:
            with DbConnection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql_update_statement)
                cursor.connection.commit()
                rows_affected = cursor.rowcount
                if rows_affected:
                    print("db updated successfully")

        except sqlite3.IntegrityError as error:
            logger.info("DB update failed while updating employee data.")
            logger.error(error.args)
            raise DataError("Update failed due to unique constraints")

    def delete(self, id):
        sql_delete_statement = """
        DELETE FROM employee where emp_id={id}
        """
        sql_delete_statement = sql_delete_statement.format(id=id)

        with DbConnection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_delete_statement)
            cursor.connection.commit()
            rows_deleted = cursor.rowcount
            if rows_deleted:
                print("record deleted successfully")

    def insert(self, employee_data):

        sql_insert_statement = """
        INSERT INTO employee(name,email,mobile_number,gender,company,emp_id,manager)
                    VALUES('{name}','{email}','{mobile_number}','{gender}','{company}',{emp_id},'{manager}') 
        
        """
        # bind the value into query params
        sql_insert_statement = sql_insert_statement.format(**employee_data)

        try:
            with DbConnection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql_insert_statement)
                cursor.connection.commit()
                print("Successfully inserted into db")

        except sqlite3.IntegrityError as error:
            # skip the duplicate records and process other data
            logger.error(error.args)

    def insert_from_data_frame(self, data_frame):
        with DbConnection() as conn:
            # creating column list for insertion

            # Insert DataFrame record one by one.
            for i, row in data_frame.iterrows():

                employee_dict = row.to_dict()
                self.insert(employee_dict)


if __name__ == '__main__':
    # create employee table
    create_employee_table()
    #drop_employee_table()
    pass

