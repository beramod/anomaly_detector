import datetime
from slacker import Slacker

class Slack:
    token = 'token'

    def sendMessage(self, channel, message):
        slack = Slacker(self.token)
        try:
            slack.chat.post_message(channel, message)
        except Exception as e:
            return False
        return slack.chat.post_message(channel, message)
