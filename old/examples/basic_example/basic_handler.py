import json
import tornado

import tornado.web

from geyser.examples.basic_example import make_basic_job

import logging
log = logging.getLogger(__name__)


class BasicHandler(tornado.web.RequestHandler):
    def get(self, argsDict):
        '''
        Get the status of a job.
        '''
        self.set_status(200)
        self.finish()

    def post(self):
        '''
        Kick off a BasicJob.
        '''
        params = tornado.escape.json_decode(self.request.body)

        log.info(f'kicking off basic job with args: {params}')

        job = make_basic_job(values=params)
        job.enqueue()

        output = dict(uuid=job.uuid)
        self.write("%s\n" % output)

        self.set_status(200)
        self.finish()
