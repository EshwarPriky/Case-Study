import os


### Set Environment Variables

os.environ['envn'] = 'TEST'    #CHANGE TO 'PROD' FOR PRODUCTION
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

### Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']


### Set Other Variables
appName= 'app1'
current_path = os.getcwd()

file_list = [ "Charges", "Damages", "Endorse", "Primary_Person", "Restrict", "Units"]
file_name= [ i+"_use.csv" for i in file_list ]

input_file_path= current_path + '\\'+'..\staging\\'
file_path= [ input_file_path+i for i in file_name ]


output = current_path + '\\'+'..\\results\\'



