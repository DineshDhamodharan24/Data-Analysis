!pip install pymongo
!pip install dnspython

from pymongo import MongoClient
from pymongo.server_api import ServerApi
import urllib
import ssl  # Import the ssl module

username =<Enter your mongobd UserName>
password =<Enter your mongobd password>
# Encode the username and password
encoded_username = urllib.parse.quote_plus(username)
encoded_password = urllib.parse.quote_plus(password)
# Construct the URI with encoded credentials
uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.3kgdqmn.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'), tz_aware=False, connect=True)
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"Connection failed: {e}")

acc = client["sample_analytics"]

pipline = [
    {"$match":{
        "transactions.transaction_code":"buy"
    }},
    {
        "$unwind":"$transactions"
    },
    {"$group":{
        "_id":"$account_id",
        "total_amount":{
            "$sum":"$transactions.amount"}
    }},
    {
        "$sort":{"total_amount":-1}
    },
    {
        "$project":{"_id":0,"total_amount":1}
    }
    ]

result = acc.transactions.aggregate(pipline)

amount = [doc["total_amount"] for doc in result]

mean = statistics.mean(amount)
median = statistics.median(amount)
minimum = min(amount)
maximum = max(amount)
# standard deviation
stdev = statistics.stdev(amount)

print(f"mean:{mean}")
print(f"median:{median}")
print(f"minimum:{minimum}")
print(f"maximum:{maximum}")
print(f"standard_Deviation:{stdev}")
