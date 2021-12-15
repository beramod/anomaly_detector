import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
ENV = 'LOCAL'

detector_env = os.environ.get('ANOMALY_DETECTOR_ENV')

if detector_env:
    ENV = detector_env

print("BASE_DIR: ", BASE_DIR)
print("ENV: ", ENV)
