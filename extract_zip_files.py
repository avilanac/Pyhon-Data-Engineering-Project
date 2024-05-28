import zipfile

archivo_zip = 'D:/Alejandra/Python projects/ETL_project/source.zip'

directorio_destino = 'D:/Alejandra/Python projects/ETL_project/data'

with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
    # Extraer todos los archivos
    zip_ref.extractall(directorio_destino)