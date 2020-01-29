from marshmallow import fields

from ..jobs import BaseJob, BaseJobSchema, NO_RESULT


class GoodJobSchema(BaseJobSchema):
    goodness = fields.String()


class GoodJob(BaseJob):
    JOB_TYPE = "good_test_job"
    SCHEMA = GoodJobSchema

    def __init__(self, *args, **kwargs):
        super(GoodJob, self).__init__(*args, **kwargs)

        self.goodness = kwargs.get("goodness", "gracious")

    def run(self):
        return 200


class NoRunJob(BaseJob):
    JOB_TYPE = "no_run_test_job"
    SCHEMA = BaseJobSchema


class NoJobTypeJob(BaseJob):
    SCHEMA = BaseJobSchema

    def run(self):
        return 200


class NoSchemaJob(BaseJob):
    JOB_TYPE = "no_schema_test_job"

    def run(self):
        return 200


class ExceptionJob(BaseJob):
    JOB_TYPE = "exception_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        1 / 0


class RequeueJob(BaseJob):
    JOB_TYPE = "requeue_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        self.add_to_completeness(0.6)

        if self.completeness < 1:
            return NO_RESULT
        else:
            return 200


class NoReturnValJob(BaseJob):
    JOB_TYPE = "no_return_val_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        return


class JobLogInfoJob(BaseJob):
    JOB_TYPE = "log_event_test_job"
    SCHEMA = BaseJobSchema

    msg = "something routine happened"

    def run(self):
        self.job_log_info(self.msg)
        return 200


class WarningFinishJob(BaseJob):
    JOB_TYPE = "warning_finish_test_job"
    SCHEMA = BaseJobSchema

    msg = "warning, mon"
    warningCode = 291

    def run(self):
        self.raise_warning_finish(self.warningCode, self.msg)


class ErrorContinueJob(BaseJob):
    JOB_TYPE = "error_continue_test_job"
    SCHEMA = BaseJobSchema

    msg = "it errored, dude"
    errorCode = 512

    def run(self):
        self.raise_error_continue(self.errorCode, self.msg)


class ErrorFinishJob(BaseJob):
    JOB_TYPE = "error_finish_test_job"
    SCHEMA = BaseJobSchema

    msg = "it errored to death, chum"
    errorCode = 476

    def run(self):
        self.raise_error_finish(self.errorCode, self.msg)
