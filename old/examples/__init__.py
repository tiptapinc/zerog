from .basic_example import BasicJob
from .handlers import handlers

import geyser.registry


geyser.registry.JOB_MODULES = geyser.registry.JOB_MODULES + [
    "geyser.examples"
]
