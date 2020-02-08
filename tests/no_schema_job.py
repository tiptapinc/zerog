from zerog.jobs import BaseJob


class NoSchemaJob(BaseJob):
    JOB_TYPE = "no_schema_test_job"

    def run(self):
        return 200
