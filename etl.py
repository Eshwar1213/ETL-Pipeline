import glob
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"


def extract_from_csv(filetobeprocess):
    dataframe = pd.read_csv(filetobeprocess)
    return dataframe

def extract_from_json(filetobeprocess):
    dataframe = pd.read_json(filetobeprocess,lines=True)
    return dataframe

def extract_from_xml(filetobeprocess):
    dataframe = pd.DataFrame(columns = ['name','height','weight'])
    tree = ET.parse(filetobeprocess)
    root= tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True)
    return dataframe


def extract():
    extracted_data = pd.DataFrame(columns=['name','height','weight'])

    for csvfiles in glob.glob('*.csv'):
        extracted_data= pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfiles))], ignore_index=True)

    for jsonfile in glob.glob('*.json'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)
    
    for xmlfile in glob.glob('*.xml'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data


def transform(data):

    data['height']= round(data.height * 0.0254,2)

    data['weight']= round(data.weight * 0.45359237,2)

    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)



def log_process(message):
    timestramp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestramp = now.strftime(timestramp_format)

    with open(log_file,"a") as f:
        f.write(timestramp + ',' + message + '\n')


log_process("ETL Job Started")

log_process("Extract phase Started")
extracted_data = extract()
log_process("Extract phase Ended")

log_process("Transformed Phase Started")
transformed_data = transform(extracted_data)
print("transformed Data")
print(transformed_data)
log_process("Transformed phase Ended")

log_process("Load phase Started")
load_data(target_file,transformed_data)
log_process("Load phase Ended")

log_process("ETL Job Ended")
