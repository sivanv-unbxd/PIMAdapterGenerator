#!flask/bin/python

adapter_id = "5e55375dfcbe020009e823c3"
org_id = "5a846024f4d84006237106def240b6bd"
url_prefix = "http://pimqa.unbxd.com/"
auth_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjo5LCJleHAiOjE1ODMyMzg5ODR9.DmxxCHOS8S4Rwax2upqRpJ6dYMdBY4zubXGVMiGbb8w"
az_kurta = "http://unbxd-pimento.s3.amazonaws.com/c83cfa4e2d7e19b47d1c92813a85bf55/imports/sftpFiles/1581609768372_1581609766688_AZ_kurtas.xlsm"

from flask import Flask, jsonify, abort, request, make_response, url_for
import logging
from logging.handlers import RotatingFileHandler
from time import strftime
import traceback
import json
import requests

# from mongodb.MongoClient import MongoClient
# from mongodb.DbSingleton import DbSingleton
import threading
from FileParser import FileParser

app = Flask(__name__)
handler = RotatingFileHandler('app.log', maxBytes=100000, backupCount=3)
logger = logging.getLogger('tdm')
logger.setLevel(logging.INFO)
logger.addHandler(handler)


@app.errorhandler(400)
def bad_request(error):
    return make_response(jsonify({'error': 'Bad request'}), 400)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.before_request
def log_request_info():
    timestamp = strftime('[%Y-%b-%d %H:%M]')
    logger.info('%s %s %s %s %s', timestamp, request.method, request.url, request.data, request.headers)


@app.errorhandler(Exception)
def exceptions(e):
    tb = traceback.format_exc()
    timestamp = strftime('[%Y-%b-%d %H:%M]')
    logger.error('%s %s %s %s %s 5xx INTERNAL SERVER ERROR\n%s', timestamp, request.remote_addr, request.method,
                 request.scheme, request.full_path, tb)
    print('%s %s %s %s %s 5xx INTERNAL SERVER ERROR\n%s', timestamp, request.remote_addr, request.method,
          request.scheme, request.full_path, tb)
    return make_response(jsonify({'data': {'message': 'Some internal problem occurred'}}), 500)


def get_adapter_props(page):
    url = url_prefix + "paprika/api/v2/" + org_id + "/networkAdapters/" + adapter_id + "/propertyMappings"

    payload = json.dumps({"page": page, "count": 100, "name": ""})
    headers = {
        'Content-Type': "application/json",
        'Authorization': auth_token
    }

    response = requests.request("POST", url, data=payload, headers=headers)

    res_data = json.loads(response.text)

    adapter_props = res_data["data"]["entries"]
    #     print(res_data)
    print("returning ", len(adapter_props), " back to the iterator, out of ", res_data["data"]["total"])
    return adapter_props, res_data["data"]["total"]


def get_pim_adapter_props():
    pim_adapter_props = []
    # print(pim_adapter_props)
    page = 1
    total = 0
    pim_adapter_props, total = get_adapter_props(page)
    while len(pim_adapter_props) < total:
        print("inside while. ", page)
        page += 1
        _adapter_props, total = get_adapter_props(page)
        pim_adapter_props += _adapter_props

    return pim_adapter_props


def get_schema_props(properties_list, valid_enum_values):
    adapter_props = []
    for key, value in properties_list.iterrows():
        #             print(key, "=====> ",value)
        if key > 1 and value[1] and isinstance(value[1], str):
            prop_obj = {}
            name = value[2]
            prop_obj["adapter_property_name"] = name
            prop_obj["alias_name"] = value[1] if isinstance(value[1], str) else ""
            prop_obj["description"] = value[3] if isinstance(value[3], str) else ""
            prop_obj["is_editable"] = False
            prop_obj["mapping_type"] = "SIMPLE"
            if value[5] == "Required":
                prop_obj["required"] = True

            prop_obj["validation_rules"] = []

            if value[1] in valid_enum_values.columns:
                enum_values = valid_enum_values[value[1]][1:]
                enum_list = list(filter(lambda x: (isinstance(x, str)), enum_values))
                prop_obj["data_type"] = "string"
                prop_obj["validation_rules"].append({"enum": enum_list})
            else:
                #TODO set data type using xsd parser for actual type, cant parse from text of XLSM
                prop_obj["data_type"] = "string"

            if len(prop_obj) != 0 and prop_obj and bool(prop_obj):
                adapter_props.append(prop_obj)

    return adapter_props


def generate_schema(url):
    print(url)
    file_parse = FileParser()
    properties_list, valid_enum_values = file_parse.load(url)
    properties_schema = get_schema_props(properties_list, valid_enum_values)
    pim_adapter_props = get_pim_adapter_props()

    # diff = [x for x in adapter_props if x not in request_data]

    adapter_prop_names = list(map(lambda x: x["adapter_property_name"], pim_adapter_props))
# Filter non added properties, still around 10 are not added to the adapter which are present in the XLSM file
    new_properties_schema = []
    for d1, d2 in zip(properties_schema, pim_adapter_props):
        for key, value in d1.items():
            if value["adapter_property_name"] not in adapter_prop_names:
                new_properties_schema.append(value)
    #new_properties_schema = list(
    #    filter(lambda x: x["adapter_property_name"] not in adapter_prop_names, properties_schema))

    print(len(new_properties_schema))
    return new_properties_schema


def update_pim_props(properties_schema):
    request_data = {"network_adapter_summary": {
        "property_details_with_mappings": properties_schema}, "clone_adapter": False}
    url = url_prefix + "api/v2/" + org_id + "/networks/adapters/" + adapter_id

    payload = json.dumps(request_data)
    headers = {
        'Content-Type': "application/json",
        'Authorization': auth_token
    }

    response = requests.request("PATCH", url, data=payload, headers=headers)
    return response


@app.route('/generate_schema', methods=['POST', 'GET'])
def post_infer_schema():
    if not request.json:
        abort(400)

    url = request.json.get('url', "")
    properties_schema = generate_schema(url)

    response = update_pim_props(properties_schema)

    return make_response(jsonify({'data': {'message': 'Successfully started creating/updating your adapter configurations  '}}),
                         200)


# @app.route('/infer_status', methods=['POST','GET'])
# def get_infer_status():
#     import_id = request.args.get('import_id', "")
#     mongo_client = MongoClient(DbSingleton(), db_name="m7p_db")
#     import_obj = mongo_client.find_one("schema_inference_status", {"import_id": import_id, "status": "completed"})
#     if import_obj and "import_id" in import_obj:
#         print("Inference completed for Import Id ==> ", import_id)
#         inferred_props = mongo_client.find("import_schema_inferences",{"import_id": import_id} )
#         return make_response(
#             jsonify({'errors': [], 'errorList': [], 'data': {'status': 'complete','total':len(inferred_props), 'schema':inferred_props}}), 200)
#     else:
#         return make_response(jsonify({ 'errors': [], 'errorList': [], 'data': {"status" : "running"}}), 200)

@app.route('/health', methods=['POST', 'GET'])
def get_health():
    return make_response(jsonify({'errors': [], 'errorList': [], 'data': "Server is up and kicking"}), 200)


if __name__ == '__main__':
    # app.run(port=1077)
    app.run(host='0.0.0.0', port=2020)
