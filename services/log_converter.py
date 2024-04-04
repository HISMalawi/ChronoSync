def map_keys_to_columns(data, table_columns_map):
    table_name = data.get('table')
    data = data.get('data')
    columns_map = table_columns_map.get(table_name, {})
    return {columns_map.get(key, key): value for key, value in data.items()}

def process_list(data, table_columns_map):
    result = []
    for item in data:
        result.append(process_request(item, table_columns_map))
    return result

# def main():
#     data = {
#   "operation": "DELETE",
#   "database": "zomba",
#   "table": "concept_name",
#   "data": {
#    "@1": "183",
#    "@2": "manzy",
#    "@3": "",
#    "@4": "1",
#    "@5": "2024-04-04 12:57:23",
#    "@6": "106360",
#    "@7": "0",
#    "@8": "NULL",
#    "@9": "NULL",
#    "@10": "NULL",
#    "@11": "e13d76ef-f282-11ee-a254-0242ac110005",
#    "@12": "NULL",
#    "@13": "0"
#   }
#  }
#     table_columns_map = {
#         "concept_name": {
#             "@1": "id",
#             "@2": "name",
#             "@3": "age",
#             "@4": "is_active",
#             "@5": "created_at",
#             "@6": "created_by",
#             "@7": "is_deleted",
#             "@8": "updated_by",
#             "@10": "voided_by",
#             "@11": "uuid",
#             "@12": "description",
#             "@13": "is_voided"
#         }
#     }
#     print(map_keys_to_columns(data, table_columns_map))


# if __name__ == "__main__":
#     main()

