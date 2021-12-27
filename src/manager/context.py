import time
import datetime
from src.define import *

class Context:
    def __init__(self):
        self.messageType = None
        self.timestamp = None
        self.raw = None
        self.openEvents = []
        self.closeEvents = []
        self.reopenEvents = []
        self.events = []
        self.store = True

    def parse(self, kafka_message):
        self.raw = kafka_message.get('data')
        self.timestamp = datetime.datetime.utcfromtimestamp(kafka_message.get('timestamp')) + datetime.timedelta(
            hours=DIFF_UTC_TO_KST)
        self.messageType = kafka_message.get('messageType')
