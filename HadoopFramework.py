# This Hadoop Framework is development for Metlife
# Author : Vigneshwaran Sundaresan

#Import all the Python components

import os
import csv
import sys
import pkgutil
import subprocess
import re
from datetime import datetime,date,time, timedelta
import logging
import time
import configparser
from logger import logger
from qualitychecks import PerformQChecks


class HCF:

    def __init__(self,parmfile="/Users/vigneshwaransundaresan/MetlifeHadoopFramework/Parmfiles/test_parm.parm"):

        #self.jobname = sys.argv[1]
        global logfile
        self.jobname = 'ABCD1234'
        #self.rundate = sys.argv[2]
        self.odate = '2018-01-29'

        today = datetime.today()
        today_hhmm = today.strftime('%Y_%m_%d_%H_%M_%S')

        self.rundate = datetime.strptime(self.odate,'%Y-%m-%d').date()

        logfile = self.jobname+'_'+str(self.rundate)+'_'+today_hhmm

        logging = logger(logfile,self.jobname,'HCF_GETPARM').extract()

        logging.info('Job : %s started for date : %s'%(self.jobname,str(self.rundate)))

        if not os.path.exists(parmfile):

            sys.exit("Parm File : %s is missing. Job Failed"%(parmfile))

        #if len(sys.argv) < 2:
        #    sys.exit("Error: Pass <job-name> <date> for job : %s"%(self.jobname))

        #Global Parms list

        location = self.getparmlist(parmfile,self.jobname,logfile,'location') # LatAm location i.e. Mexico, Chile or Argentina
        logging.info("Location is : {}".format(location))

        source_file_path = self.getparmlist(parmfile,self.jobname,logfile,'source_file_path')  # File landing location in Hadoop
        logging.info("Source File Path : {}".format(source_file_path))

        email_address_list = self.getparmlist(parmfile,self.jobname,logfile,'email_address_list') # Email address list
        logging.info("Email Address List : {}".format(email_address_list))

        #Local parms related to job

        entity_name = self.getparmlist(parmfile,self.jobname,logfile,'entity_name')
        logging.info("Entity name : {}".format(entity_name))

        source_system_type = self.getparmlist(parmfile,self.jobname,logfile,'source_system_type')
        logging.info("Source System Type : {}".format(source_system_type))

        target_system_type = self.getparmlist(parmfile, self.jobname, logfile, 'target_system_type')
        logging.info("Target System Type : {}".format(target_system_type))

        job_description = self.getparmlist(parmfile,self.jobname, logfile, 'job_description')
        logging.info("Job Description : {}".format(job_description))

        batch_frequency = self.getparmlist(parmfile, self.jobname, logfile, 'batch_frequency')
        logging.info("batch frequency : {}".format(batch_frequency))

        source_file_desc = self.getparmlist(parmfile, self.jobname, logfile, 'source_file_desc')
        logging.info("Source File Description : {}".format(source_file_desc))

        dev_status_flag = self.getparmlist(parmfile, self.jobname, logfile, 'dev_status_flag') #Development status flag; 1 - Extended log for trouble shooting 0 - Production jobs
        logging.info("Dev status flag : {}".format(dev_status_flag))

        notification_flag = self.getparmlist(parmfile, self.jobname, logfile, 'notification_flag')  # 1 - Send email 0 - No notification
        logging.info("Notification flag : {}".format(notification_flag))

        file_has_header = self.getparmlist(parmfile, self.jobname, logfile,'file_has_header')
        logging.info("File has header : {}".format(file_has_header))

        schema_file_name = self.getparmlist(parmfile, self.jobname, logfile,'schema_file_name')
        logging.info("File has header : {}".format(schema_file_name))

        #Control parameters

        qc_check_ind = self.getparmlist(parmfile, self.jobname, logfile, 'qc_check_ind')
        logging.info("QC Check indicator : {}".format(qc_check_ind))

        identity_check_indicator = self.getparmlist(parmfile, self.jobname, logfile, 'identity_check_indicator')
        logging.info("Identity Check indicator : {}".format(identity_check_indicator))

        empty_file_check_indicator = self.getparmlist(parmfile, self.jobname, logfile, 'empty_file_check_indicator')
        logging.info("Empty File check Indicator : {}".format(empty_file_check_indicator))

        ThersholdCheckInd = self.getparmlist(parmfile, self.jobname, logfile, 'thershold_check')
        logging.info("Thershold check Indicator : {}".format(ThersholdCheckInd))

        ThersholdPercentage = self.getparmlist(parmfile, self.jobname, logfile, 'thershold_percentage')
        logging.info("Thershold Percentage : {}".format(ThersholdPercentage))

        #Hive Related Details

        source_qc_file = self.getparmlist(parmfile, self.jobname, logfile, 'source_qc_file_name')
        logging.info("Source QC file name : {}".format(source_qc_file))

        source_data_file_name = self.getparmlist(parmfile, self.jobname, logfile, 'source_data_file_name')
        logging.info("Source data file name : {}".format(source_data_file_name))

        hive_table_name = self.getparmlist(parmfile,self.jobname, logfile, 'hive_table_name')
        logging.info("Hive table name : {}".format(hive_table_name))

        hive_data_location = self.getparmlist(parmfile, self.jobname, logfile, 'hive_data_location')
        logging.info("Hive data location : {}".format(hive_data_location))

        hive_table_location = self.getparmlist(parmfile, self.jobname, logfile, 'hive_table_location')
        logging.info("Hive data location : {}".format(hive_table_location))

        logging = logger(logfile, self.jobname, 'HCF_Quality_Checks_starts_here').extract()

        logging.info("Quality Checks started for Job : {}".format(self.jobname))

        varlist = ['Y', 'YES', 'SI', 'SIM']

        #si - Yes in spanish
        #sim - yes in Portugese

        md5 = PerformQChecks.getmd5value(self, logfile, self.jobname, source_file_path, source_data_file_name)

        datacount = PerformQChecks.getdatacount(self, logfile, self.jobname, source_file_path, source_data_file_name)

        if (qc_check_ind.upper() in varlist) or (int(qc_check_ind) == 1):

            qcCount = PerformQChecks.qccheck(self, logfile, self.jobname, source_file_path, source_qc_file, datacount)

        if int(qcCount) > 0:

            if (empty_file_check_indicator.upper() in varlist) or (int(empty_file_check_indicator) == 1):

                empty_check = PerformQChecks().emptyfilecheck(logfile,self.jobname,source_file_path,source_data_file_name,datacount)

        if (identity_check_indicator.upper() in varlist) or (int(identity_check_indicator) == 1):

            identity_check = PerformQChecks.identitycheck(self, logfile,self.jobname,source_file_path, source_data_file_name, md5)

        if (ThersholdCheckInd.upper() in varlist) or (int(ThersholdCheckInd) == 1):

            ThersholdCheck = PerformQChecks.thersholdcheck(self, logfile, self.jobname, datacount)

        logging = logger(logfile, self.jobname,'HCF_CREATE_SCHEMA_STARTS_HERE').extract()

    def getparmlist(self, parm_file_name, jobname, logfile, parmname):

        jobparms = {}
        globalparms = {}
        parmvalue = ''

        parser = configparser.ConfigParser()
        parser.read_file(open(parm_file_name))

        if not parser.has_section('global'):
            logging.info("Global section not defined in control file")

        globallist = parser.items('global')

        if not parser.has_section(jobname):
            logging.error(jobname + " Section not defined in control file")

        joblist = parser.items(jobname)

        for key, value in globallist:
            globalparms[key] = value
        for key, value in joblist:
            jobparms[key] = value

        if (parmname.lower() or parmname.upper()) in jobparms:
            parmvalue = jobparms[parmname]
        elif (parmname.lower() or parmname.upper()) in globalparms:
            parmvalue = globalparms[parmname]
        else:
            parmvalue=''

        return parmvalue

def main():

    HCF()

if __name__ == '__main__':

    main()



