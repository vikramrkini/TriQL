import re
import json

input_str = "db.Orders.aggregate([{ $match : {Orders.OrderID : {$lt:10250}}, { $project : { _id : 0 , Orders.OrderID : 1} }])"

# Extract collection
collection = re.findall(r'db\.(\w+)\.aggregate', input_str)[0]

# Extract aggregation pipeline
pipeline_str = re.findall(r'aggregate\((.+)\)', input_str)[0]
# pipeline = json.loads(pipeline_str.replace("'", '"'))

print(f"Collection: {collection}")
print(f"Pipeline: {pipeline_str}")
