import mysql.connector as cn
import boto3
import os
import botocore
import MySQLdb
import logging
import S3Credentials as cred



array = []

def S3GetFile(bucket, locationString, destinationString=''):
        '''
        This function downloads the needed files found from the database

        bucket = log bucket no default format, 
        locationString = location path for log, 
        destinationString = path where files and directories will be saved to

        no returns - downloads files onto local 
        '''     
        #Split up location String for directories:
        arr = [ x.strip() for x in locationString.strip('[]').split('/') ]
        serialNumber = arr[1]
        type = arr[2]
        fileName = arr[3]

        #Get connection:
        try:
            client_s3 = boto3.client('s3', endpoint_url=cred.hosts3, aws_access_key_id=cred.aws_access_key_id,
            aws_secret_access_key=cred.aws_secret_access_key)
        except botocore.exceptions.ParamValidationError as error:
            logging.basicConfig(filename='./pytestresults.log', format='%(asctime)s %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
            logging.error("Incorrect Parameters Given : Connection Error")

            raise ValueError('The connection parameters you provided are incorrect: {}'.format(error))
                        

        #Split up destination string to grab input destination, check if destination string is given
        if(destinationString != ''):
            destinationArray = os.path.split(destinationString)
            #destinationArray = [ x.strip() for x in destinationString.strip('[]').split('/') ]
            userPath = os.path.join('', *destinationArray) #empty space incase it is empty
            
            os.chdir(userPath) #change to destination path wanted
        else:        
            userPath = os.getcwd() 
            
        
        generalPath = os.path.join(userPath)
        serialNumPathway = os.path.join(generalPath, serialNumber)
        typePathway = os.path.join(serialNumPathway, type)
        directorypath = os.path.join(serialNumPathway, type)
        #Check if serial number directory is created:
        if os.path.isdir(serialNumPathway) != True:
            os.mkdir(serialNumber)  #create first directory

        #Check if type directory has already been created:
        if(os.path.isdir(typePathway) != True):              
            os.mkdir(directorypath) 
        

        filePathway = os.path.join(directorypath, fileName) #join with directories
        try:
            client_s3.download_file(bucket, locationString, filePathway) #download to created directories
            return True #for unit testing
        except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logging.basicConfig(filename='./pytestresults.log', format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
                        logging.error("Could not download file/s : Download Error")

                        print(e)
                    else:
                        raise
        return(filePathway)
        
        
def S3WhereClause( startDate="", endDate="", product="", serialNum="", swVersion="", filetype="", limit=100):
    '''
    This function creates a where clause to access the S3 file list

    startDate = get entries after this date date format is 'YYYY-MM-DD' default = None, 
    endDate = get entries before this date default format is 'YYYY-MM-DD' = None, 
    product = product to search for defualt = None
    serialNum= serial number to search for defualt = None
    swVersion= sw version to search for defualt = None
    limit = max number of rows to return default = 100

    returns where clause if valid or and empty string (should it be empty or should the default have the limit )
    '''
    searhCriteria = []
    if (startDate != ""):
        searhCriteria.append(f"""s3latest.filedate > '{startDate}'""")
    if (endDate != ""):
        searhCriteria.append(f"""s3latest.filedate < '{endDate}'""")
    if(serialNum != ""):
        searhCriteria.append(f"""s3latest.inserv_serial = '{serialNum}'""")
    if(product != ""):
        searhCriteria.append(f"""inservs.MODEL LIKE '%{product}%'""")
    if(swVersion != ""):
        searhCriteria.append(f"""inservs.TPDVER LIKE '%{swVersion}%'""")
    if(filetype != ""):
        searhCriteria.append(f"""filetype = '{filetype}'""")
    
    if(len(searhCriteria) > 0):
        finalStatement = f"""INNER JOIN inservs ON s3latest.inserv_serial = inservs.INSERV_SERIAL WHERE {' AND '.join(searhCriteria)} limit {limit}"""
        
        return(finalStatement)
    else:
        return(f"""INNER JOIN inservs ON s3latest.inserv_serial = inservs.INSERV_SERIAL limit {limit}""")

def List(filenames):
    
    # Open third file you are joining them to:
    with open('Final.evtlog', 'w') as outfile:
    
        # Iterate through list
        for names in filenames:
            # Open each file in read mode
            with open(names) as infile:
                # read the data from file1 and
                # file2 and write it in file3
                outfile.write(infile.read())
            # Add '\n' to enter data of file2
            # from next line
            outfile.write("\n")

def ReadDatabase(startDate="", endDate="", product="", serialNum="", swVersion="", filetype="",limit=100, destination=''):  
    '''
    This function creates a where clause to access the S3 file list, grab the needed sql statement for 
    wanted logs and download them to a chosen path

    startDate = get entries after this date date format is 'YYYY-MM-DD' default = None, 
    endDate = get entries before this date default format is 'YYYY-MM-DD' = None, 
    product = product to search for defualt = None
    serialNum= serial number to search for defualt = None
    swVersion= sw version to search for defualt = None
    limit = max number of rows to return default = 100
    destination = path files will be downloaded default = default

    returns the total number of files downloaded
    '''  
    

    #Grab logs from database
    sqls3 = f"""
    SELECT   bucket
            ,location
    FROM stats_config.s3latest {S3WhereClause(startDate, endDate, product, serialNum, swVersion, filetype, limit)} """
    
    
    try:
        con = cn.connect(host=cred.host,user=cred.user,passwd=cred.password,database=cred.database,autocommit = True)
        myCursor = con.cursor()
        connected  = True
    except MySQLdb.Error as ex:
        logging.basicConfig(filename='./pytestresults.log', format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
        logging.error("Failed to make database connection. : Database Connection Failure")
        print(ex)
        raise
    if connected:
        myCursor.execute(sqls3)
        myresult = myCursor.fetchall()
        
        count = 0
        if(len(myresult)> 0):
            for item in myresult: #Grab each string (bucket and location) to download 
                locationString = item[1]
                bucket = item[0]
                fileArray = []
                try:
                    file = S3GetFile(bucket, locationString, destination)
                    count+=1
                    fileArray.apppend(file)
                except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == "404":
                            logging.basicConfig(filename='./pytestresults.log', format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
                            logging.error("Could not download file/s : Download Error")
                            
                            #Letting user know what was unable to be downloaded:
                            print(f"Unable to download: Bucket: {bucket} and Location: {locationString}")
                        else:
                            raise
            
        else:
            print("No files found with selected categories.")
            
    return count #Returns the number of files downloaded




bucketname = 'stats-2021-05'
location = '3PAR.INSERV/1201735/evtlog/evtlog.210501.001744.debug'
bucketname2 = 'stats-2021-01'
location2 = '3PAR.INSERV/1116947/event/event.210102.001421.1+738483'
destinationString = r"C:\Users\riosel\Desktop\DownloadedFiles"

ReadDatabase(filetype='config', limit = 2, destination=destinationString)


# Python program to
# demonstrate merging of
# two files
  


