import pika
import json
import sys

from datetime import datetime
from faker import Faker
from pymongo import MongoClient
from model import Emails
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))


fake = Faker("uk-UA")

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='task_mock', exchange_type='direct')
channel.queue_declare(queue='task_queue', durable=True) #durable - знижує швидкість, але збільшує надійність, щоб не загубить данні
channel.queue_bind(exchange='task_mock', queue='task_queue')

client = MongoClient("mongodb+srv://userweb17:567234@cluster0.x8roo9r.mongodb.net/hw8_1", ssl=True)
db = client["hw8_1"]
collection_emails = db["emails"]

def main():
    for i in range(5):
        contact = Emails(
            full_name=fake.name(),
            email=fake.email(),
            message_status=False,           
        )
        contact.save()

        message = {
            "_id": str(contact.id),
            "full_name": contact.full_name,
            "email": contact.email,
            "message_status": contact.message_status,
            "date": datetime.now().isoformat()            
        }

        inserted_result = collection_emails.insert_one(message)
        document_id = str(inserted_result.inserted_id)
        message["_id"] = document_id
        
        channel.basic_publish(
            exchange='task_mock',
            routing_key='task_queue',
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        print(" [x] Sent %r" % message)
    connection.close()
    
    
if __name__ == '__main__':
    main()