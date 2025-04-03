import sqlite3
import pandas as pd
import numpy as np
import json

from flask import Flask
app = Flask(__name__)


@app.route('/')
def home():
    con = sqlite3.connect("incidentes.db")
    query_tickets = "SELECT * FROM tickets_emitidos"
    query_contactos = "SELECT * FROM contactos_empleados"
    df_tickets = pd.read_sql_query(query_tickets, con)
    df_contactos = pd.read_sql_query(query_contactos, con)

    df_clientes_mas_incidencias = df_tickets.groupby('id_cliente')['tipo_incidencia'].count().reset_index(name='count').sort_values(by='count', ascending=False)
    result_clientes = df_clientes_mas_incidencias.head(5).apply(lambda x: f"{x['id_cliente']} - {x['count']}", axis=1).str.cat(sep=", ")

    df_empleados_mas_tiempo = df_contactos.groupby('id_emp')['tiempo'].sum().reset_index(name='count').sort_values(by='count', ascending=False)
    result_empleados = df_empleados_mas_tiempo.head(5).apply(lambda x: f"{x['id_emp']} - {x['count']}", axis=1).str.cat(sep=", ")

    string_final = "Clientes con más incidencias (id - número de incidencias): " + result_clientes + "<br>Empleados con más tiempo (id - tiempo total): " + result_empleados
    return string_final




@app.route('/top-clientes')
def top_clientes():
    con = sqlite3.connect("incidentes.db")
    query_tickets = "SELECT * FROM tickets_emitidos"
    df_tickets = pd.read_sql_query(query_tickets, con)
    #seleccionamos del dataframe query_tickets los valores de id_cliente y los contamos y ordenamos segun el numero de incidencias de mayor a menor

    #queremos obtener los clientes segun la cantidad de incidentes relacionada con ellos e imprimir esta cantidad
    df_clientes_mas_incidencias = df_tickets.groupby('id_cliente')['tipo_incidencia'].count().reset_index(name='count').sort_values(by='count', ascending=False)
    result = df_clientes_mas_incidencias.head(5).to_string(index=False, header=False)
    return "Clientes con más incidencias" + result

@app.route('/top-empleados')
def top_empleados():
    con = sqlite3.connect("incidentes.db")
    query_contactos = "SELECT * FROM contactos_empleados"
    df_contactos = pd.read_sql_query(query_contactos, con)
    #seleccionamos del dataframe query_contactos los valores de id_emp y los del tiempo y ordenamos segun el id_emp que haya teido más tiempo en total ordenados de mayor a menor
    df_empleados_mas_tiempo = df_contactos.groupby('id_emp')['tiempo'].sum().reset_index(name='count').sort_values(by='count', ascending=False)
    result = df_empleados_mas_tiempo.head(5).to_string(index=False, header=False)
    return "Empleados con más tiempo: " + result



if __name__ == '__main__':
    app.run(debug=True)


