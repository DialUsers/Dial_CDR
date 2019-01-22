r"""
This module will not be used individually. It is a common code container.

####Author#### Dongdong Cheng, dongdong_cheng@infosys.com

This file define common code used in metadata repository python toolkits, includes
    1, the configuration variables 
    2, common functions for all modules

####Change Log####
v1.0, 20180608, initialize the code

"""

####python standard library####
import datetime
import inspect

####configuration####
#P1, SPI personal ID, number
personal_ID_number=10000
#P2, row size
average_row_size=256
#P3, total row count
total_tow_count=100000
#P4, file count
file_count=5
#P5, avg(each file size)=2*3/4

#C1, number of parallel
#number_of_parallel=3

#YYYYMMDD
current_day_YYYYMMDD="{:%Y%m%d}".format(datetime.datetime.now())

#F1, source file folder
source_file_folder=r'd:\source'
#F2, working folder for data
working_folder_data=r'd:\cache_data\\'+current_day_YYYYMMDD
#F3, working folder for mapping
working_folder_mapping=r'd:\cache_mapping\\'+current_day_YYYYMMDD
#F4, global mapping folder
global_mapping_folder=r'd:\mapping'
#F5, output data file folder
target_file_folder=r'd:\target'

#### debug level
#debugLevel=1  #, reserverd
#debugLevel=2  #, program level, start, end, error. in main function only
#debugLevel=3  #, key function level, start end, error. in sub function
#debugLevel=4  #, warning messages
debugLevel=5  #, message can track the key process of the program
#debugLevel=6  #, little more detail why the dicide is made
#debugLevel=7  #, detail inside function and class
#debugLevel=8  #, detail that can be used to debug and tuning
#debugLevel=9  #, very detail, a little boring

####shared function####
def _print(level,message):
    """
This function print the debug message on standard output.
The level parameter control how detail message will be printed.
Normally production code will use level=5, which contains the main progress and all warning messages.
While working in debug mode, we set level=9, which contains most detail message, include key branch program made each time.  
"""
    caller=inspect.stack()[1]
    if level<=debugLevel:
        currentDatetimeString="{:%Y%m%d %H:%M:%S}".format(datetime.datetime.now())
        print(currentDatetimeString+'|'+caller.filename+'|'+str(caller.lineno)+'|'+str(level)+'|'+message.encode('utf-8', errors="backslashreplace").decode('ASCII',"ignore"))
    return True

####unit test####
if __name__ == '__main__':  #unit testing case

    #initial program
    _print(2,'INFO: shared code unit testing starting...')
    _print(2,'INFO: shared code unit testing finished')
