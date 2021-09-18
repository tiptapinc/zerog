Simple Example
==============

Create a Job Class
------------------
This example creates a job that will waste a specified amount of time, while randomly logging approximately 10 messages

.. code-block:: python

    from marshmallow import fields
    import random
    import time
    import zerog

    class WasteTimeJobSchema(zerog.BaseJobSchema):
        delay = fields.Integer()


    class WasteTimeJob(zerog.BaseJob):
        JOB_TYPE = "waste_time"
        SCHEMA = WasteTimeJobSchema

        def __init__(self, *args, **kwargs):
            super(WasteTimeJob, self).__init__(*args, **kwargs)
            self.delay = kwargs.get('delay', 30)

        def run(self):
            end = time.time() + self.delay
            logInterval = self.delay / 10

            while True:
                if time.time() > end:
                    break

                logDelay = (random.random() + 0.5) * logInterval
                time.sleep(logDelay)
                self.add_to_completeness(logDelay / self.delay)
                self.job_log_info(f"{end - time.time():.2f} seconds remaining")

            return 200, None

Create a ZeroG Service
----------------------
Creating a ZeroG service is as simple as creating a new :code:`zerog.Server` instance.

.. code-block:: python

    import tornado.ioloop
    import zerog

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - "
               "%(message)s - [%(process)s:%(name)s:%(funcName)s]"
    )
    log = logging.getLogger(__name__)


    def make_datastore():
        return zerog.CouchbaseDatastore(
            "couchbase", "Administrator", "password", "test"
        )


    def make_queue(queueName):
        return zerog.BeanstalkdQueue("beanstalkd", 11300, queueName)


    handlers = [
        (f"/job/{zerog.JOB_TYPE_PATT}", zerog.RunJobHandler),
        (f"/progress/{zerog.UUID_PATT}", zerog.ProgressHandler),
        (f"/info/{zerog.UUID_PATT}", zerog.InfoHandler),
        (f"/data/{zerog.UUID_PATT}", zerog.GetDataHandler)
    ]

    server = zerog.Server(
        "myService",
        make_datastore,
        make_queue,
        [WasteTimeJob],
        handlers
    )
    server.listen(8888)
    tornado.ioloop.IOLoop.current().start()

