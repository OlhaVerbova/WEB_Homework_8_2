import time
import pika
import json

from bson import ObjectId 
from pymongo import MongoClient


credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True) 
print(' [*] Waiting for messages. To exit press CTRL+C')

#Use MongoDB 
client = MongoClient("mongodb+srv://userweb17:567234@cluster0.x8roo9r.mongodb.net/hw8_1", ssl=True)
db = client["hw8_1"]
collection_emails = db["emails"]


def callback(ch, method, properties, body):    
    message = json.loads(body.decode())    
    message["message_status"] = True 
    emails_id = message.get("_id")
  

    #Change message_status from False to True 
    if emails_id:
        result = collection_emails.update_one(
            {"_id": ObjectId(emails_id), "message_status": False},
            {"$set": {"message_status": True}}
        )       
    print(f" [x] Received {message}")
    time.sleep(1)
    print(f" [x] Done: {method.delivery_tag}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1) 
channel.basic_consume(queue='task_queue', on_message_callback=callback)


if __name__ == '__main__':
    channel.start_consuming()