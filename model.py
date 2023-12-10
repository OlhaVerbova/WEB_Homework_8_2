from mongoengine import *

connect(host="mongodb+srv://userweb17:567234@cluster0.x8roo9r.mongodb.net/hw8_1", ssl=True)

class Emails(Document):
    full_name = StringField(required=True)
    email = StringField(required=True)
    message_status = BooleanField(default=False)
    