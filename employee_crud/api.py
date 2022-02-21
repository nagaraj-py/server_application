import pandas as pd
from flask_restful import Resource, abort
from flask import jsonify
from flask import request

from employee_crud.db import EmployeeModel
from employee_crud.exceptions import DataError
from employee_crud.server import get_logger

logger = get_logger()


class EmployeeAPI(Resource):

    def post(self):
        # hard coded path
        #file_path = r'C:\Users\91901\Downloads\New excel.xlsx'
        #data_frame = pd.read_excel(file_path)

        # Dynamic upload from client
        # lets load uploaded excel file into pandas dataFrame
        data_frame = pd.read_excel(request.files.get('file'))

        # lets handle null values with default
        data_frame.fillna("DEFAULT", inplace=True)

        # Rename file columns as per db column names
        data_frame.rename(columns={'EMP ID': 'emp_id',
                                   'EMAIL': 'email',
                                   'NAME': 'name',
                                   'NUMBER': 'mobile_number',
                                   'GENDER': 'gender',
                                   'COMPANY': 'company',
                                   'MANAGER': 'manager'
                                   }, inplace=True)

        employee_table = EmployeeModel()
        employee_table.insert_from_data_frame(data_frame)

        return jsonify({
            'message': "File processed successfully"})

    def get(self):
        # query string params
        emp_id = request.args.get('emp_id')
        employee_table = EmployeeModel()
        if emp_id:
            data = employee_table.get_by_id(emp_id)
            if not data:
                abort(http_status_code=404, message="Record not found")
        else:
            # list all employee
            data = employee_table.get_all_employees()

        return jsonify(data)

    def put(self):
        payload = request.get_json()
        # check record existence before doing update operation
        employee_table = EmployeeModel()
        employee_record = employee_table.get_by_id(payload['emp_id'])

        if not employee_record:
            abort(http_status_code=404, message="Request resource not found in server")

        try:
            employee_table = EmployeeModel()
            employee_table.update(payload)
        except DataError:
            abort(http_status_code=400, message="Update Failed.")

        return jsonify({'message': "Updated successfully"})

    def delete(self):
        emp_id = request.args.get('emp_id')
        if not emp_id:
            abort(http_status_code=400, message='Please provide valid emp_id')
        # check record existence
        employee_table = EmployeeModel()
        employee_record = employee_table.get_by_id(emp_id)
        if not employee_record:
            abort(http_status_code=404, message="Record not found")

        employee_table = EmployeeModel()
        employee_table.delete(emp_id)

        return jsonify({'message': "Record deleted successfully"})
