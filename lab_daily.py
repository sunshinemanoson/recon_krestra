# Import Library
import requests
import os.path
import config
import pandas as pd
import pandas.io.sql as psql
import time
from datetime import timedelta

#Get data frame and query function
def query_sc():
     sql = '''
          SELECT 
	   [LAB_NO]
      ,[DATE_RELEASE]
      ,[PID_HN]
      ,[SEX_code]
      ,[AGE_YEAR]
      ,[SOURCE_CODE]
      ,[Clinician_code]
      ,[Hemoglobin]
      ,[Hematocrit]
      ,[Rbc_count]
      ,[MCV]
      ,[MCH]
      ,[MCHC]
      ,[Red_cell_distribution_RDW]
      ,[NRC/_100_WBC]
      ,[RBC_comment]
      ,[Hypochromic]
      ,[Microcytic]
      ,[Macrocytic]
      ,[Anisocytosis]
      ,[Poikilocytosis]
      ,[Target_cell]
      ,[Polychromasia]
      ,[Basophillic_stippling]
      ,[Spherocyte]
      ,[Schistocyte]
      ,[Elliptocyte]
      ,[Howell_jolly_body]
      ,[Tear_drop_cell]
      ,[Reticulocyte_count]
      ,[Ret-He]
      ,[WBC_comment]
      ,[Wbc_count]
      ,[WBC-C]
      ,[% Neutrophils]
      ,[% Lymphocytes]
      ,[% Monocytes]
      ,[% Eosinophils]
      ,[% Basophils]
      ,[%Atypical_lymphocyte]
      ,[%Band_form]
      ,[%Metamyelocyte]
      ,[%Promyelocyte]
      ,[%Blasts]
      ,[%Myelocyte]
      ,[%Abnormal_cells]
      ,[Absolute_neutrophils]
      ,[Absolute_lymphocyte]
      ,[Absolute_monocyte]
      ,[Absolute_eosinophil]
      ,[Absolute_basophil]
      ,[Absolute_reticulocyte]
      ,[Platelet_count]
      ,[PLT_comment]
      ,[PLT_Reflex]
      ,[Smear_from_DI60]
      ,[Smear_request]
      ,[Smear_XN3000]
      ,[ESR]
      ,[TEST_COMMENT]
  FROM [SiIMC_MGHT].[dbo].[View_HCLAB_CBC_Group_PIVOT]	WITH(NOLOCK)

  WHERE CAST([DATE_RELEASE] AS DATE) = CAST(GETDATE()-1 AS DATE)
           '''
     return sql


def get_dataframe():
    conn = config.singin_to_panda_setup()
    sql_command = query_sc()
    dataframe = psql.read_sql(sql_command, conn)
    


    return dataframe


## Define Static Variable
# SiVWORK EKO_V1 API Token id and secret
sivwork_client_id = "7bcee21a-3f7e-4338-97af-a90842176849"
sivwork_client_secret = "81882fc8f8443e67f7d63e2ecdd5aa845870305a"
# SiVWORK EKO_V1 API URL
sivwork_token_endpoint = "https://sivwork-h1.ekoapp.com/oauth/token" # for generate_sivwork_token
sivwork_direct_msg_url = "https://sivwork-h1.ekoapp.com/bot/v1/direct/message"
sivwork_group_msg_url = "https://sivwork-h1.ekoapp.com/bot/v1/group/message"
sivwork_direct_file_url = "https://sivwork-h1.ekoapp.com/bot/v1/direct/file"
sivwork_group_file_url = "https://sivwork-h1.ekoapp.com/bot/v1/group/file"
sivwork_direct_pic_url = "https://sivwork-h1.ekoapp.com/bot/v1/direct/picture"
sivwork_group_pic_url = "https://sivwork-h1.ekoapp.com/bot/v1/group/picture"
# Specify the required grant type and scope (SIVWORK)
sivwork_grant_type = "client_credentials"
sivwork_scope = "bot"
sivwork_file_path = "D://TB//labdata//file_buffer//" # define path to use reference
#Get file as dataframe and save to machine

dataframe = get_dataframe()

date_order = dataframe['DATE_RELEASE'].iloc[0]
file_name = 'lab_data_'+ date_order +'.xlsx'
#print(file_name)
dataframe.to_excel(sivwork_file_path+file_name, encoding="TIS-620",index=False)




# Prepare the payload for the token request (SIVWORK)
sivworkAPI_RequestPayload = {
    "client_id": sivwork_client_id,
    "client_secret": sivwork_client_secret,
    "grant_type": sivwork_grant_type,
    "scope": sivwork_scope
}
# Line Notify API URL
line_notify_url = "https://notify-api.line.me/api/notify"
##


## Define Dynamic Variable (Sender)
# user_id , group_id and topic_id of SIVWORK Channel
sivwork_uid = "" # Taspon K.
sivwork_gid = "66304262395b897d98e1dfec" # Test Group Channel
sivwork_tid = "66304262395b89aa3be1dfee" # Common Topic 
# Line Notify Token List for Notify
 ## Used Listed to ref. Token
filepath_LinetokenListed = "D:\TB\@@@@ Test New Eko EndpointAPI\LineToken\LineTokenDics.txt"
with open(filepath_LinetokenListed) as LineTokenListed:
    LineAPItokenlist = [LineAPI.strip().split(',')[1] for LineAPI in LineTokenListed] # readonly column 1 mean token
    
##

## Define Data and Message to Send
# Data and Picture (SIVWORK)



sivwork_filelist = [
        file_name
]

# Notify Message (LINE)
linemessage_genratefail = ("Your Sending SIVWORK in Path : "+sivwork_file_path+" Is Failed in Generate Token Process")
linemessage_sivworkfail = ("Your Sending SIVWORK in Path : "+sivwork_file_path+" Is Failed in SendingData Process")
##




##### Define Function #####

def send_line_notify(lineNotifymessage):
    for LineToken in LineAPItokenlist:
        lineAPI_headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+LineToken}
        lineAPIresponse = requests.post(line_notify_url, headers=lineAPI_headers, data = {'message':lineNotifymessage})
        print (lineAPIresponse.text)

def IsAllFilesExisted(listedType):
    sivwork_chkfilestatus_listed = []
    for sivwork_item in listedType:
        sivwork_chkfilestatus = os.path.isfile(sivwork_file_path+sivwork_item)
        sivwork_chkfilestatus_listed.append(sivwork_chkfilestatus)
        sivwork_chkfilestatus_listed_IsAllExisted = all(sivwork_chkfilestatus_listed)
    print(sivwork_chkfilestatus_listed)
    print(sivwork_chkfilestatus_listed_IsAllExisted)
    return sivwork_chkfilestatus_listed_IsAllExisted

def WhichFileisNotExisted(listedType):
    sivwork_filesNotExisted =[]
    for sivwork_item in listedType:
        sivwork_chkfilestatus = os.path.isfile(sivwork_file_path+sivwork_item)
        if sivwork_chkfilestatus == False:
            sivwork_filesNotExisted.append(sivwork_item)
    for WhichFileisNotExistedItem in sivwork_filesNotExisted:
        linemessage_fileNotExistfail = ("Your File SIVWORK in Path : "+sivwork_file_path+" Named : "+WhichFileisNotExistedItem+" Doesn't Existed")
        send_line_notify(linemessage_fileNotExistfail);
    return sivwork_filesNotExisted


def send_group_file():
    # Generate Token for SIVWORK API BOT
    responseSivworkAPI = requests.post(sivwork_token_endpoint, data=sivworkAPI_RequestPayload)
    SivworkAPItoken_data = responseSivworkAPI.json()
    SivworkAPIaccess_token = SivworkAPItoken_data.get("access_token")
    print()
    print("SIVWORK ACCESS TOKEN :  ",SivworkAPIaccess_token)
    print()
    print("SIVWORKAPITOKEN_REQUEST_STATUS :  ",responseSivworkAPI.status_code)
    print()

    if responseSivworkAPI.status_code == 200: # Check Status of Generate Token
        print("Generate Token Comepleted")
        sivwork_header = {"Authorization": f"Bearer {SivworkAPIaccess_token}"}
        sivwork_payload = {"gid":sivwork_gid , "tid":sivwork_tid,}

        ## Send File Function all file have to existed
        if IsAllFilesExisted(sivwork_filelist) == True:
            for sivwork_File in sivwork_filelist: # Loop Send file to SIVWORK
                sviwork_FileLoopSendPayload = {"file": (sivwork_File, open(sivwork_file_path+sivwork_File, "rb"),'')}
                responseSivworkSender = requests.post(sivwork_group_file_url, data=sivwork_payload, headers=sivwork_header, files=sviwork_FileLoopSendPayload)

                if responseSivworkSender.status_code == 201: # Check Status of Sending Data
                    print("Response Status Code : ", responseSivworkSender.status_code)
                    print("Response : ", responseSivworkSender.text)
                    print("Sending Data to SIVWORK Comepleted")
                else:
                    print("Sending Data to SIVWORK Failed")
                    send_line_notify(linemessage_sivworkfail); # Line Notify Sending Data Failed
        else:
            print("Some file are not existed")
            linemessage_fileNotExistfail = ("Your File SIVWORK in Path : "+sivwork_file_path+" Are not Sending to SiVWork Cause All File doesn't existed")
            send_line_notify(linemessage_fileNotExistfail); 

    else:
        print("Generate Token Failed")
        send_line_notify(linemessage_genratefail);  # line Notify GenerateTOKEN Failed





WhichFileisNotExisted(sivwork_filelist); # for file
#WhichFileisNotExisted(sivwork_piclist); # for pics

send_group_file();

#deletefile 
os.remove(sivwork_file_path+file_name)