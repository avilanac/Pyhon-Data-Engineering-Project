import glob                         
import pandas as pd                 
import xml.etree.ElementTree as ET  
import os
import logging

# Configure the logging level and the message format
logging.basicConfig(filename='./logs/loggingfile.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


"""
The above functions extract data from CSV, JSON, and XML files into pandas DataFrames.

"""
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    data = []
    try:
        tree = ET.parse(file_to_process)
        root = tree.getroot()
        for person in root.findall('person'):
            name = person.find('name').text
            height = person.find('height').text
            weight = person.find('weight').text
            data.append({'name': name, 'height': height, 'weight': weight})
    except Exception as e:
        error_msg = f"Error processing XML file {file_to_process}: {e}"
        logging.error(error_msg)
        return pd.DataFrame()  # Return empty DataFrame in case of error
    return pd.DataFrame(data)


def extract():
    """
    The 'extract' function processes CSV, JSON, and XML files from a specified directory to extract data
    into a pandas DataFrame.
    """
    extracted_data = pd.DataFrame(columns=['name','height','weight']) 
    
    # Define the correct path to the data directory
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    
    # Process all csv files
    for csvfile in glob.glob(os.path.join(data_dir, "*.csv")):
        csv_data = extract_from_csv(csvfile)
        extracted_data = pd.concat([extracted_data, csv_data], ignore_index=True)
        logging.info(f"Data loaded in the CSV file: {csvfile}")

    # Process all JSON files
    for jsonfile in glob.glob(os.path.join(data_dir, "*.json")):
        json_data = extract_from_json(jsonfile)
        extracted_data = pd.concat([extracted_data, json_data], ignore_index=True)
        logging.info(f"Data loaded in the JSON file: {jsonfile}")
    
    # Process all XML fiels
    for xmlfile in os.listdir(data_dir):
        if xmlfile.endswith('.xml'):
            xml_data = extract_from_xml(os.path.join(data_dir, xmlfile))
            extracted_data = pd.concat([extracted_data, xml_data], ignore_index=True)
            logging.info(f"Data loaded in the XML file: {xmlfile}")
        
    return extracted_data


def transform(data):

    # The code snippet is converting the 'height' and 'weight' columns in the DataFrame 'data' into numeric data types.
    data['height'] = pd.to_numeric(data['height'], errors='coerce')
    data['weight'] = pd.to_numeric(data['weight'], errors='coerce')
    
    # Convert height from inches to millimeters, change the data type of the column to float
    data['height'] = round(data.height * 0.0254, 2)
            
    # Convert weight from pounds to kilograms, change the data type of the column to float
    data['weight'] = round(data.weight * 0.45359237, 2)
    
    return data

def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)
    logging.info(f"Datos cargados en el archivo CSV: {targetfile}")  
    

# Execute the main code block if this script executes directly
if __name__ == "__main__":
    
    logging.info("ETL Job Started")
    logging.info("Extract phase Started")
    extracted_data = extract()
    logging.info("Extract phase Ended")
    
    logging.info("Transform phase Started")
    transformed_data = transform(extracted_data)
    logging.info("Transform phase Ended")    
    
    targetfile = "./data/transformed_data_new.csv" 
    logging.info("Load phase Started")
    load(targetfile, transformed_data)
    logging.info("Load phase Ended")
    
    logging.info("ETL Job Ended")
