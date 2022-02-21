from flask import Flask
from flask_restful import Api
import logging

app = Flask(__name__)
api = Api(app)

# log data is going to store into this file employee_crud.log
logging.basicConfig(filename='employee_crud.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')


def get_logger():
    return app.logger


def register_resources(api_instance):
    from employee_crud.api import EmployeeAPI
    # we will keep all api endpoints here

    api_instance.add_resource(EmployeeAPI, '/employee', endpoint='employee')


if __name__ == '__main__':
    register_resources(api_instance=api)
    app.run(debug=True, host='127.0.0.1', port=5000)

