from multiprocessing import Queue


class AnomalyDetectorQueue:
    def __init__(self, partition=6):
        self._queues = []
        self._rr = 0
        self._partition = partition
        for idx in range(0, self._partition):
            self._queues.append(Queue())

    def get(self, id):
        return self._queues[id].get()

    def put(self, id, message):
        self._queues[id].put(message)

    def putrr(self, message):
        self._queues[self._rr].put(message)
        self._rr += 1
        if self._rr >= self._partition:
            self._rr = 0

    def getSize(self):
        return len(self._queues)
