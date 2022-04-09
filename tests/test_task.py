#!/usr/bin/env python
import requests
from cirrus.lib.task import Task



class NothingTask(Task):

    _name = 'nothing-task'
    _description = 'this task does nothing'

    def process(self):
        pass


if __name__ == "__main__":
    output = NothingTask.cli()
