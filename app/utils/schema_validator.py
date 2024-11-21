import json
from jsonschema import validate, ValidationError


class SchemaValidator:

    def __init__(self, schema_file_path):
        self.schema_file_path = schema_file_path

    def __load_schema_file(self):
        # print("load schema" + self.schema_file_path)
        with open(self.schema_file_path, 'r') as file:
            # print("xyz" + self.schema_file_path)
            return json.load(file)

    def validate_json(self, json_data):
        try:
            print("*******")
            validate(instance=json_data, schema=self.__load_schema_file())
            print("JSON data is valid.")
            return True
        except ValidationError as err:
            print("JSON data is invalid:", err.message)
        return False

# if __name__ == "__main__":
#     file_path='data/locations.json'
#     schema = load_schema_file('app/schemas/location_schema.json')
#     validate_data_file(file_path, schema)
