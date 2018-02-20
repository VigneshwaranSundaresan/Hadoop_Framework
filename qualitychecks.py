import os
import csv
import sys
import pkgutil
import subprocess
import re
from datetime import datetime, date, time, timedelta
import logging
import time
import configparser
from logger import logger


class PerformQChecks:

    def qccheck(self, logfile, jobname, source_file_path, source_qc_file, datacount):

        logging = logger(logfile, jobname, 'Qc_File_Check').extract()

        self.qcFile = ""

        self.qcFile += source_file_path
        self.qcFile += '/'
        self.qcFile += source_qc_file

        logging.info("qc file name is : {}".format(self.qcFile))

        with open(self.qcFile) as csvfile:
            readQc = csv.reader(csvfile, delimiter='~')  # The delimiter can be modified
            for row in readQc:
                qcCount = row[2]

        if int(qcCount) == int(datacount):

            logging.info("QC check completed.qc count : {} and data count is : {}".format(qcCount, datacount))

        else:

            logging.error("Data Count : {} not equal to qc count : {}".format(datacount, qcCount))
            sys.exit("QC check failed")

        return qcCount

    def emptyfilecheck(self, logfile, jobname, source_file_path, source_file_name, data_count):

        logging = logger(logfile, jobname, 'Empty_File_Check').extract()

        if int(data_count) == 0:
            logging.error("Data Count : {} equal to zero".format(data_count))
            sys.exit("QC count is > 0 but and data file is empty")
        else:
            logging.info("Empty file check completed.Data File not empty and count is : {}".format(data_count))

    def identitycheck(self, logfile, jobname, source_file_path, source_file_name, md5):

        # This module will be developed by Karthik
        # MD5CHKSUM value is generated for every input file and the run information is stored in hive table
        # This Hive table will have following columns,
        # 1) Jobname 2) File Date 3) Job run datetime 4) qc count 5) Data count 6) MD5CHKSUM value 7) Job Status i.e. Success/Fail --> Audit Table
        # For the 1st run,
        # The job details are inserted into the hive table
        # for subsequent runs MD5CHKSUM from max run datetime is taken and compared against the todays file MD5CHKSUM
        # If the value is same then we have received same --> send failure email

        pass

    def thersholdcheck(self,logfile, jobname, data_count):

        # Run a hive query against the Audit table to get the rows for the # of days given as Thershold days
        # Add all the data count / Thershold days --> Average data count
        # Average data count * Thershold Percentage / 100 --> deviation
        # If Today's Data count - Average Data count > Deviation --> Fail job in Thershold

        logging = logger(logfile, jobname, 'thersholdcheck').extract()

        pass

    def getdatacount(self, logfile, jobname, source_file_path, source_file_name):

        logging = logger(logfile, jobname, 'Get_Data_count').extract()

        get_file_count = 'wc -l ' + source_file_path + '/' + source_file_name + " | awk '{print $1}'"

        logging.info("Get count command is : {}".format(get_file_count))

        p = subprocess.Popen(get_file_count, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)

        data_count = p.communicate()[0].decode('utf-8').strip()

        logging.info("Data Count in Input file : {1} is {0}".format(data_count, source_file_name))

        if p.returncode != 0:
            logging.error("Command {} failed in Empty file check with return code : {}".format(get_file_count, p.returncode))
            sys.exit("Failed in executing shell command in Empty file check module")

        return data_count

    def getmd5value(self, logfile, jobname, source_file_path, source_file_name):

        Md5Cmd = ""

        logging = logger(logfile, jobname, 'Get_MD5_value').extract()

        Md5Cmd += "md5 "

        Md5Cmd += "-q "

        Md5Cmd += source_file_path

        Md5Cmd += '/'

        Md5Cmd += source_file_name

        logging.info("Get Md5 command is : {}".format(Md5Cmd))

        p = subprocess.Popen(Md5Cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)

        Md5Value = p.communicate()[0].decode('utf-8').strip()

        logging.info("MD5 Checksum value is {}".format(Md5Value))

        return Md5Value












