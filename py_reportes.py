import pandas as pd
import os
import requests
import unidecode
import numpy as np
from datetime import datetime
import psycopg2
import openpyxl

os.chdir('C:/Users/DELL/OneDrive - 99minutos.com/procesos_99/reportes')

query = ' '.join(open('base_descargable.txt').readlines()).replace(str('\n'),' ')
query = query.replace(str('\t'),' ')
query = query.replace('  ',' ')

excel = None
i = 0
while excel is None:
    try:
        i = i + 1
        print(i)
        conn = psycopg2.connect(host='static.99minutos.app', database='pr99minutoscom', user='dsbruno', password='Mt&YMDRv6I5*')
        cursor = conn.cursor()
        exe = cursor.execute(query)
        excel = pd.DataFrame(data = cursor.fetchall())
        excel.columns = [desc[0] for desc in cursor.description]
        conn.close()
    except:
        pass
print('Cerrado')
# excel = pd.read_csv('distrit.csv', sep = ',')

dis_d = {'match_distritos':['SAN ISIDRO','ANCON','ATE','BARRANCO','CARABAYLLO','CERCADO DE LIMA','CHACLACAYO','CHORRILLOS','CIENEGUILLA','COMAS','EL AGUSTINO',
                      'INDEPENDENCIA','JESUS MARIA','LA MOLINA','LA VICTORIA','LINCE','LOS OLIVOS','LURIGANCHO','LURIN','MAGADALENA DEL MAR','MIRAFLORES',
                      'PACHACAMAC','PUCUSANA','PUEBLO LIBRE','PUENTE PIEDRA','PUNTA HERMOSA','PUNTA NEGRA','RIMAC','SAN BARTOLO','SAN BORJA','SAN JUAN DE LURIGANCHO',
                      'SAN JUAN DE MIRAFLORES','SAN LUIS','SAN MARTIN DE PORRES','SAN MIGUEL','SANTA ANITA','SANTA MARIA DEL MAR','SANTA ROSA','SANTIAGO DE SURCO',
                      'SURQUILLO','VILLA EL SALVADOR','VILLA MARIA DEL TRIUNFO', 'BRENA',
                      'VENTANILLA','BELLAVISTA','CALLAO','LA PERLA','LA PUNTA','MI PERU','CARMEN DE A LEGUA', 'LIMA CERCADO', 'MAGDALENA','SURCO',
                      'JOSE LEONARDO ORTIZ','FERRENAFE','CHICLAYO','LAMBAYEQUE','PIMENTEL','POMALCA','TUMAN','REQUE','MONSEFU','ETEN','PICSI'],
         'distritos':['SAN ISIDRO','ANCON','ATE','BARRANCO','CARABAYLLO','CERCADO DE LIMA','CHACLACAYO','CHORRILLOS','CIENEGUILLA','COMAS','EL AGUSTINO',
                'INDEPENDENCIA','JESUS MARIA','LA MOLINA','LA VICTORIA','LINCE','LOS OLIVOS','LURIGANCHO','LURIN','MAGADALENA DEL MAR','MIRAFLORES',
                'PACHACAMAC','PUCUSANA','PUEBLO LIBRE','PUENTE PIEDRA','PUNTA HERMOSA','PUNTA NEGRA','RIMAC','SAN BARTOLO','SAN BORJA','SAN JUAN DE LURIGANCHO',
                'SAN JUAN DE MIRAFLORES','SAN LUIS','SAN MARTIN DE PORRES','SAN MIGUEL','SANTA ANITA','SANTA MARIA DEL MAR','SANTA ROSA','SANTIAGO DE SURCO',
                'SURQUILLO','VILLA EL SALVADOR','VILLA MARIA DEL TRIUNFO', 'BREÑA',
                'VENTANILLA','BELLAVISTA','CALLAO','LA PERLA','LA PUNTA','MI PERU','CARMEN DE A LEGUA', 'CERCADO DE LIMA', 'MAGDALENA DEL MAR','SANTIAGO DE SURCO',
                'JOSE LEONARDO ORTIZ','FERRENAFE','CHICLAYO','LAMBAYEQUE','PIMENTEL','POMALCA','TUMAN','REQUE','MONSEFU','ETEN','PICSI']}

dis_distritos = pd.DataFrame(data = dis_d)
# PARA BO-18-16
excel['route_origen'] = excel['route_origen'].astype(str).apply(unidecode.unidecode).str.upper()
excel['route_dest'] = excel['route_dest'].astype(str).apply(unidecode.unidecode).str.upper()
excel['dis_origen'] = np.nan
excel['dis_origen_2'] = np.nan
excel['dis_destino'] = np.nan
excel['dis_destino_2'] = np.nan

for i in range(len(dis_distritos)):
    ser_dest = pd.Series(excel['route_dest']).str.contains(dis_distritos.iloc[i,0], na = False).tolist()
    ser_origen = pd.Series(excel['route_origen']).str.contains(dis_distritos.iloc[i,0], na = False).tolist()
    excel.iloc[ser_dest,excel.columns.get_loc('dis_destino')] = dis_distritos.iloc[i,1]
    excel.iloc[ser_origen,excel.columns.get_loc('dis_origen')] = dis_distritos.iloc[i,1]

excel['ruta_dest'] = excel.apply(lambda row : row['route_dest'].replace(str(row['dis_destino']), ''), axis = 1)
excel['ruta_origen'] = excel.apply(lambda row : row['route_origen'].replace(str(row['dis_origen']), ''), axis = 1)

for i in range(len(dis_distritos)):
    ser_dest_2 = pd.Series(excel['ruta_dest']).str.contains(dis_distritos.iloc[i,0], na = False).tolist()
    ser_origen_2 = pd.Series(excel['ruta_origen']).str.contains(dis_distritos.iloc[i,0], na = False).tolist()
    excel.iloc[ser_dest_2,excel.columns.get_loc('dis_destino_2')] = dis_distritos.iloc[i,1]
    excel.iloc[ser_origen_2,excel.columns.get_loc('dis_origen_2')] = dis_distritos.iloc[i,1]
    
excel.iloc[pd.isna(excel['dis_destino_2']).tolist(),excel.columns.get_loc('dis_destino_2')] = excel.iloc[pd.isna(excel['dis_destino_2']).tolist(),excel.columns.get_loc('dis_destino')]
excel.iloc[pd.isna(excel['dis_origen_2']).tolist(),excel.columns.get_loc('dis_origen_2')] = excel.iloc[pd.isna(excel['dis_origen_2']).tolist(),excel.columns.get_loc('dis_origen')]

excel.drop(['dis_destino','dis_origen','ruta_dest','ruta_origen'], inplace = True, axis = 1)
excel.rename(columns = {'dis_destino_2':'Distrito Destino','dis_origen_2':'Distrito Origen'}, inplace = True)

d_origen = excel['Distrito Origen']
d_destino = excel['Distrito Destino']

excel.drop(labels = ['Distrito Origen','Distrito Destino'], axis = 1, inplace = True)
excel.insert(12,'Distrito Origen',d_origen)
excel.insert(14,'Distrito Destino',d_destino)

nom = 'outputs/reporte ' + datetime.now().strftime("%d_%m_%y %H_%M") + '.xlsx'
excel.to_excel(nom)
print('Reporte creado')

files = os.listdir('outputs')
files.sort(key=lambda x: os.path.getmtime('outputs/' + x))
os.remove('outputs/' + files[0])
