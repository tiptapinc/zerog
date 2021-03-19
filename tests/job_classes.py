from marshmallow import fields
import time

from zerog.jobs import BaseJob, BaseJobSchema, NO_RESULT


class GoodJobSchema(BaseJobSchema):
    goodness = fields.String()


class GoodJob(BaseJob):
    JOB_TYPE = "good_test_job"
    SCHEMA = GoodJobSchema

    def __init__(self, *args, **kwargs):
        super(GoodJob, self).__init__(*args, **kwargs)

        self.goodness = kwargs.get("goodness", "gracious")

    def run(self):

        return 200, None


class SleepJobSchema(BaseJobSchema):
    delay = fields.Integer(missing=5)


class SleepJob(BaseJob):
    JOB_TYPE = 'sleep_job'
    SCHEMA = SleepJobSchema

    def __init__(self, *args, **kwargs):
        super(SleepJob, self).__init__(*args, **kwargs)
        self.delay = kwargs.get('delay', 5)

    def run(self):
        self.job_log_info(f"{self.uuid} sleeping {self.delay} seconds")
        time.sleep(self.delay)
        self.job_log_info(f"{self.uuid} done sleeping")
        return 200, None


class NoRunJob(BaseJob):
    JOB_TYPE = "no_run_test_job"
    SCHEMA = BaseJobSchema


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
            return NO_RESULT, 1
        else:
            return 200, None


class NoReturnValJob(BaseJob):
    JOB_TYPE = "no_return_val_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        return


class ReturnGoodListJob(BaseJob):
    JOB_TYPE = "return_good_list_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        return [220, None]


class ReturnBadListJob(BaseJob):
    JOB_TYPE = "return_bad_list_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        return [None, None]


class ReturnGoodStringJob(BaseJob):
    JOB_TYPE = "return_good_string_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        return "220", None


class ReturnBadStringJob(BaseJob):
    JOB_TYPE = "return_bad_string_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        return "Bad", None


class ReturnDictJob(BaseJob):
    JOB_TYPE = "return_dict_test_job"
    SCHEMA = BaseJobSchema

    def run(self):
        return dict(a=10, b=30)


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
