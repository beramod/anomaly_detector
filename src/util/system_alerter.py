import datetime, multiprocessing
from src.util.slack import Slack

class SystemAlerter:
    def __init__(self):
        self._slackModule = Slack()
        self._logger = multiprocessing.get_logger()

    def sendAlert(self, message, errorMessage):
        print(errorMessage)
        alertMessage = '[anomaly_detector]\n' \
                       + 'time: {}\n'.format(str(datetime.datetime.now())) \
                       + 'message: {}\n'.format(message) \
                       + 'errorMessage: {}'.format(errorMessage)
        self._sendSlack('mini_system_alert', alertMessage)

    def _sendSlack(self, channel, content):
        result = self._slackModule.sendMessage(channel, content)
        try:
            result = result.body.get('ok')
        except Exception as e:
            return False
        return result
