import zipfile

file_zip = './ETL_project/source.zip'
target_directory = './ETL_project/data'

# Extract all files
with zipfile.ZipFile(file_zip, 'r') as zip_ref:   
    zip_ref.extractall(target_directory)
