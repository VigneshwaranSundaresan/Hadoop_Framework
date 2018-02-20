import os
from datetime import datetime,date,time, timedelta
import logging
import time

class logger(object):

    def __init__(self,logfilename,jobname,where):

        name = jobname+'_'+where
        logger = logging.getLogger('%s' % name)
        logger.setLevel(logging.INFO)

        logfiledir="/Users/vigneshwaransundaresan/MetlifeHadoopFramework/Logs"

        logfile = os.path.join(logfiledir,'%s.log' %logfilename)

        fhandler = logging.FileHandler(logfile)

        shandler = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        fhandler.setFormatter(formatter)

        fhandler.setLevel(logging.INFO)

        logger.addHandler(fhandler)

        self.logs = logger

    def extract(self):

        return self.logs



















