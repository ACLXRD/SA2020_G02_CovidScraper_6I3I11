from pandas.core.base import nv
from pathlib import PureWindowsPath
import pymysql
from bs4 import BeautifulSoup
import requests
import pandas as pd
import matplotlib.pyplot as plt 

def sinEspacios(txt):
    nvtexto = ""
    for i in txt:
        if i == "\xa0" or i == "\n" or i == "'":
            continue
        if i == ",":
            i = "."
        nvtexto += i
    return nvtexto


url = 'https://es.wikipedia.org/wiki/Pandemia_de_enfermedad_por_coronavirus_de_2020_en_Colombia'
ruta = "/Users/andre/DatosCovid.CSV"

page_response = requests.get(url, timeout=5)
page_content = BeautifulSoup(page_response.content, "html.parser")
ciudadtxt = list()
numerosArray = list()

tablaCol = page_content.find(
    "table", attrs={"class": "wikitable sortable col1izq"})
ciudadRows = tablaCol.find_all("tr")

for row in ciudadRows:
    numerostd = row.find_all("td")[1:]
    for i in numerostd:
        numerosArray.append(sinEspacios(i.text))
    ciudadLink = row.find_all("a")
    for i in ciudadLink:
        ciudadtxt.append(i.text)

ciudadtxt.pop()
ciudadtxt.reverse()

total = list()
cxMhad = list()
muertesT = list()
porjeMT = list()
falleMhab = list()
recuT = list()
porjeRT = list()
CasActT = list()
porjeACT = list()
AxMhab = list()

for i in range(0, 330, 10):
    total.append(numerosArray[i])
total.reverse()

for i in range(1, 330, 10):
    cxMhad.append(numerosArray[i])
cxMhad.reverse()

for i in range(2, 330, 10):
    muertesT.append(numerosArray[i])
muertesT.reverse()

for i in range(3, 330, 10):
    porjeMT.append(numerosArray[i])
porjeMT.reverse()

for i in range(4, 330, 10):
    falleMhab.append(numerosArray[i])
falleMhab.reverse()

for i in range(5, 330, 10):
    recuT.append(numerosArray[i])
recuT.reverse()

for i in range(6, 330, 10):
    porjeRT.append(numerosArray[i])
porjeRT.reverse()

for i in range(7, 330, 10):
    CasActT.append(numerosArray[i])
CasActT.reverse()

for i in range(8, 330, 10):
    porjeACT.append(numerosArray[i])
porjeACT.reverse()

for i in range(9, 330, 10):
    AxMhab.append(numerosArray[i])
AxMhab.reverse()

df = pd.DataFrame({'Nombre': ciudadtxt, 'Casos_Confirmados_Totales': total, 'Casos_Confirmados_por_millon_de_habitantes': cxMhad,
                   'Muertes_Totales': muertesT, 'Porcentaje_de_muertes': porjeMT, 'Fallecido_por_millon_de_habitantes': falleMhab,
                   'Recuperados_Totales': recuT, 'Porcentaje_de_recuperados': porjeRT, 'Casos_Activos_Totales': CasActT,
                   'Porecentaje_CasosActivos': porjeACT, 'Casos_Activos_por_millon_de_habitantes': AxMhab})
print(df)
df.to_csv('DATOSCOVID.csv', index=False)

# Importe de datos a la DB
f = open(r"DATOSCOVID.csv","r")
fString = f.read()

flist = []

for line in fString.split('\n'):
    flist.append(line.split(','))

# Conexión con la base de datos
db= pymysql.connect("localhost", "root", "029624", "covid_database")
cursor = db.cursor()
cursor.execute("drop table if exists datoscovid")

#Nombre de las columnas
Ciudad=flist[0][0]; Total_Casos_Confirmados=flist[0][1]; CasosxMillon=flist[0][2]; Total_Fallecimientos=flist[0][3]; Porcentaje_Fallecidos=flist[0][4]; FallecidosxMillon=flist[0][5]; Total_Recuperados=flist[0][6]; Porcentaje_Recuperados=flist[0][7]; Casos_Activos=flist[0][8]; Porecentaje_CasosActivos=flist[0][9]; Casos_ActivosxMillon=flist[0][10]

queryCreateTable = """create table datoscovid(
                        {} varchar (50),
                        {} double,
                        {} double,
                        {} double,
                        {} varchar (50),
                        {} double,
                        {} double,
                        {} varchar (50),
                        {} double,
                        {} varchar (50),
                        {} double
                        )""".format(Ciudad, Total_Casos_Confirmados, CasosxMillon, Total_Fallecimientos, Porcentaje_Fallecidos, FallecidosxMillon, Total_Recuperados, Porcentaje_Recuperados, Casos_Activos, Porecentaje_CasosActivos, Casos_ActivosxMillon)

cursor.execute(queryCreateTable)
del flist [0]
rows = ''
for i in range(len(flist)-1):
    rows += "('{}', '{}', '{}', '{}','{}', '{}', '{}','{}','{}','{}','{}')".format(flist[i][0],flist[i][1],flist[i][2],flist[i][3],flist[i][4],flist[i][5],flist[i][6],flist[i][7],flist[i][8],flist[i][9],flist[i][10],)
    if i != len(flist) - 2:
        rows +=','
dataInsert = "insert into datoscovid values" + rows

try:
    cursor.execute(dataInsert)
    db.commit()
    print("Inserción de datos exitosa!")
except:
    print("Error en conexión :c")
    db.rollback()

db.close()

# Grafico barras
x = df['Nombre'].iloc[28:33]
y = df['Casos_Confirmados_Totales'].iloc[28:33]
plt.bar(x, y)
plt.xlabel('Departamento')
plt.ylabel('Casos_Confirmados_Totales')
plt.grid()
plt.show()

#Gráfico Torta 
x = df['Muertes_Totales'].iloc[28:33]
y = df['Nombre'].iloc[28:33]
plt.title('Muertes_Totales')
plt.pie(x,labels= y, autopct = "%0.1f%%"  )
plt.show()

#Gráfico 2D
x = df['Muertes_Totales'].iloc[28:33]
y = df['Nombre'].iloc[28:33]
plt.plot(x,y)
plt.xlabel('Cantidad')
plt.ylabel('Departamento')
plt.grid()
plt.show()

