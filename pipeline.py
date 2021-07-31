from jobs import Job


class Pipeline(object):
    """
    In memory pipeline for each worker

    In real world, we should have one pipeline and each worker
    adding to it, but that seems overkill here
    """

    def __init__(self):
        self._jobs = []

    def add(self, job):
        self._jobs.append(job)
        return self

    def run(self, data):
        for job in self._jobs:
            status, data = job.run(data)
            if not status:
                print(f"Something bad happened")
                return status, data
        return status, data
