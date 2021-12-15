import time

class Context:
    def __init__(self):
        self.processingTime = time.time()
        self.performanceData = {
            'totalTime': None,
            'endTime': None,
            'detectorTime': None,
            'filterTime': None,
            'alerterTime': None,
            'totalTime': None
        }
        self.messageType = None
        self.timestamp = None
        self.raw = None
        self.openEvents = []
        self.closeEvents = []
        self.reopenEvents = []
        self.events = []
        self.notis = []
        self.store = True
