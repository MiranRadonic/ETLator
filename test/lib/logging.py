import os
from datetime import datetime


class Logger(object):
    """ Creates a log file and outputs logs inside it. """

    levels = {
        'DEBUG' : 1,
        'INFO' : 2,
        'WARNING' : 3,
        'ERROR' : 4,
        'CRITICAL' : 5
    }

    def __init__(self, name, filename, minlevel, master = None, paths = None):
        self.name = name
        filepath = os.path.dirname(filename)
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        self.file = open(filename, 'a+')
        self.minlevel = minlevel
        self.master = master
        self.reported_paths = []
        if paths is not None:
            if filename.startswith('./flow/'):
                path = filename.split('/')[2]
                fname = filename.split('/')[4].split('.py')[0] + '.py'
                paths[name] = (path, fname)

    def log(self, level, message):
        if self.levels[level] >= self.levels[self.minlevel]:
            line = level + ' -- ' + datetime.now().strftime('%Y.%m.%d._%H:%M:%S -- ') + message + '\n'
            self.file.write(line)
            self.file.flush()
        if self.master is not None:
            self.master.log(level, self.name + ": " + message)

    def finalize(self):
        self.file.close()
