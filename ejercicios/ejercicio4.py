import sqlite3
import pandas as pd
import json

def grafico_criticos():

    conn=sqlite3.connect('incidentes.db')

    incidentes=pd.read_sql_query("SELECT * from incidentes", conn)

    incidentes['critico']=(incidentes['es_mantenimiento'] == True) & (incidentes['tipo_incidencia'] != 1)

    patata=(incidentes['es_mantenimiento'] == True) & (incidentes['tipo_incidencia'] != 1)

    #incidentes=incidentes.sort_values(by='cliente')
    print(patata)
    #print(incidentes['critico'])

grafico_criticos()