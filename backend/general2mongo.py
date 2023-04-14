import json
from pymongo import MongoClient
import pprint

# connect to MongoDB
# client = MongoClient('mongodb://localhost:27017/')
# db = client["mydatabase"]
# with open(f'schema/northwind-general.json','r') as f:
#     schema = json.load(f)

#logic : 
#If the relationship between two documents is one-to-one or one-to-few,
# embedding the documents may be a good choice. 
# If the relationship is one-to-many or many-to-many,
# it may be better to store the related documents in a separate collection and link to them.

def convert_general_to_mongo(schema):
    mongo_schema = {}
    for table_name, table_dict in schema.items():
        mongo_table = {}
        if table_dict["Type"] == 'entity':
            for col_dict in table_dict['Attributes']:
                col_name = col_dict['name']
                col_type = col_dict['type']
                if col_type == 'INTEGER' or col_type == 'NUMERIC':
                    mongo_type = 'int'
                elif col_type == 'REAL':
                    mongo_type = 'double'
                elif col_type == 'TEXT':
                    mongo_type = 'string'
                elif col_type == 'DATE' or col_type == 'DATETIME':
                    mongo_type = 'date'
                elif col_type == 'BLOB':
                    mongo_type = 'binary'
                else:
                    raise ValueError(f"Unsupported data type {col_type}")
                mongo_table[col_name] = {'type': mongo_type}
                if col_dict['primary_key']:
                    mongo_table[col_name]['primary_key'] = True
                if not col_dict['nullable']:
                    mongo_table[col_name]['required'] = True

            # else :
            #     if table_dict['Cardinality'][1] == "1->1":
            #         #Embed the code of in the [cardinality][0]
            #         #Embed the code of the referenced entity
            #         parent_table_name = table_dict['Cardinality'][0]
            #         parent_table = schema[parent_table_name]
            #         parent_embedded = {parent_table_name: {'$ref': f'#{parent_table_name}'}}
            #         mongo_table[parent_table_name] = parent_embedded



            mongo_schema[table_name] = {'schema': mongo_table}


    return mongo_schema


def load_schema_data_in_mongo(mongo_schema,original_schema,db): 
    
    # Loop through each collection in the input dictionary
    for collection_name, collection_data in mongo_schema.items():
        # Get the schema for the current collection
        try : 
            schema = collection_data["schema"]
            # Create a new collection with the current name and schema
            if collection_name not in db.list_collection_names():
                collection = db[collection_name]
                idx = list(schema.keys())[0]
                # print(idx)
                collection.create_index([(idx, 1)])
                

                # Insert sample data to the new collection
                data = original_schema[collection_name]['Data']
                
                collection.insert_many(data)
        except TypeError as t :
            pass

def general2mongo(schema,db):
    #Main driver function 
    mongo_schema = convert_general_to_mongo(schema)
    # print(mongo_schema)
    load_schema_data_in_mongo(mongo_schema,schema,db)

def delete_all_collections(db_name):
    # connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]

    # get a list of all collection names
    collection_names = db.list_collection_names()

    # delete each collection
    for collection_name in collection_names:
        db[collection_name].drop()

    # print a message
    print(f"All collections in {db.name} have been deleted.")
# if __name__ == '__main__':
#     delete_all_collections("mydatabase")
#     general2mongo(schema,db)
#     pp = pprint.PrettyPrinter(indent=4)
#     pp.pprint(convert_general_to_mongo(schema))
#     collection = db["Orders"]
#     # query = {"OrderID": 10248}
#     pipeline = [
#     {
#         "$lookup": {
#             "from": "Order Details",
#             "localField": "OrderID",
#             "foreignField": "OrderID",
#             "as": "OrderDetails"
#         }
#     },
#     {
#         "$unwind": "$OrderDetails"
#     },
#     {
#         "$group": {
#             "_id": "$CustomerID",
#             "TotalAmount": {
#                 "$sum": {
#                     "$multiply": [ 
#                                     { "$toDouble": "$OrderDetails.UnitPrice" },
#                                         {"$toInt": "$OrderDetails.Quantity" }]
#                 }
#             }
#         }
#     },
#     {
#         "$sort": {"TotalAmount": 1}
#     },
#     {
#         "$limit": 10
#     }
# ]

#     # results = collection.find(query)
#     results = collection.aggregate(pipeline)
#     for i in results:
#         print(i)
