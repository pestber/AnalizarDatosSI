import sqlite3
import pandas as pd
import numpy as np
import json

from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route('/')
def home():
   #return "Bienvenido. Usa /top-clientes , /top-incidencias , /empleados para ver los datos."
    return '''
    <h1>Bienvenido</h1>
    <p>Usa los siguientes enlaces para ver los datos:</p>
    <ul>
        <li><a href="/top-clientes">Top clientes con más incidencias</a></li>
        <li><a href="/top-incidencias">Top tipo de incidencias con más tiempo</a></li>
        <li><a href="/empleados">Empleados con más tiempo resolviendo incidencias</a></li>
    </ul>
    '''


@app.route('/top-clientes')
def top_clientes():
    con = sqlite3.connect("incidentes.db")
    query_tickets = "SELECT * FROM tickets_emitidos"
    df_tickets = pd.read_sql_query(query_tickets, con)
    con.close()
    df_clientes_mas_incidencias = df_tickets.groupby('id_cliente')['tipo_incidencia'].count().reset_index(name='count').sort_values(by='count', ascending=False)
    result_clientes = df_clientes_mas_incidencias.head(5).apply(lambda x: f"Id del cliente: {x['id_cliente']} - Número de incidencias: {x['count']}", axis=1).str.cat(sep="<br>")
    return "Clientes con más incidencias (id - número de incidencias):<br>" + result_clientes + "<br>"


@app.route('/top-incidencias')
def top_incidencias():
    con = sqlite3.connect("incidentes.db")
    query_tipo_inc = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df_tipo_inc = pd.read_sql_query(query_tipo_inc, con)
    con.close()

    df_incidencia_mas_tiempo = df_tipo_inc.groupby('tipo_incidencia')['tiempo'].sum().reset_index(name='count').sort_values(by='count', ascending=False)
    result_incidencias = df_incidencia_mas_tiempo.head(5).apply(lambda x: f"Tipo de incidencia: {x['tipo_incidencia']} - Tiempo total: {x['count']}", axis=1).str.cat(sep="<br>")

    return "Incidencias con más tiempo (id - tiempo total): <br>" + result_incidencias + "<br>"


@app.route('/empleados')
def empleados():
    con = sqlite3.connect("incidentes.db")
    query_ticket = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    query_emp = "SELECT * FROM empleados"
    df_ticket = pd.read_sql_query(query_ticket, con)
    df_emp = pd.read_sql_query(query_emp, con)
    con.close()

    df_empleados = pd.merge(df_ticket, df_emp, on="id_emp", how="left")
    df_empleados_mas_tiempo = df_empleados.groupby('id_emp')['tiempo'].sum().reset_index(name='count')
    df_empleados_mas_tiempo = df_empleados_mas_tiempo.merge(df_empleados.drop_duplicates(subset=['id_emp']), on='id_emp', how='left').sort_values(by='count', ascending=False)

    result_empleados = df_empleados_mas_tiempo.head(30).apply(lambda x: f"Id de empleado: {x['id_emp']}, nombre: {x['nombre']}, nivel: {x['nivel']}, fecha de contrato: {x['fecha_contrato']}, número de incidencias: {x['count']}",  axis=1).str.cat(sep="<br>")

    return "Empleados que más tiempo han empleado en resolución de incidentes:<br>" + result_empleados + "<br>"






if __name__ == '__main__':
    app.run(debug=True)
