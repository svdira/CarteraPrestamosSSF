import pandas as pd
import sqlite3
import glob
import os

libros_db = []
libros_folder = []
libros_subir = []

conn = sqlite3.connect('db.sqlite3')
cursor = conn.cursor()
cursor.execute("SELECT * FROM Archivos_SSF;")
tables = cursor.fetchall()

for t in tables:
    libros_db.append(t[0])

carpeta = 'raw_data/macros/outputs'
excel_files = glob.glob(os.path.join(carpeta, '*.xls')) + \
              glob.glob(os.path.join(carpeta, '*.xlsx'))

for file in excel_files:
    str_path = file.replace('\\','/')
    str_file = str_path.split('/')[-1]
    if '~$' not in str_file:
        libros_folder.append(str_file)

for lf in libros_folder:
    if lf not in libros_db:
        libros_subir.append(lf)

if len(libros_subir) > 0:
    for l in libros_subir:
        df = pd.read_excel(f'raw_data/macros/outputs/{l}', sheet_name='Sheet1')
        df.to_sql('raw_catera_prestamos', conn, if_exists='append', index=False)
        insert_sql = f"INSERT INTO Archivos_SSF (Archivo) VALUES ('{l}');"
        cursor.execute(insert_sql)
        conn.commit()
        print(f"Se insertó el archivo {l}")
else:
    print("No hay archivos para cargar")

conn.close()