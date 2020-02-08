from zerog.jobs import BaseJob, BaseJobSchema


class NoJobTypeJob(BaseJob):
    SCHEMA = BaseJobSchema

    def run(self):
        return 200
