import sqlite3
import pandas as pd
import numpy as np
import json
import requests
from flask import Flask, request

app = Flask(__name__)


@app.route('/')
def home():
    return '''
    <h1>Práctica Sistemas de la Información<br></h1>
    <h2>Top de clientes con más incidencias, tipo de incidencias con más tiempo o empleados con mas tiempo resolviendo incidencias</h2>
    <p>Introduce el número de resultados que deseas ver:</p>
    <form action="/resultados" method="get">
        <label for="num_resultados">Número de resultados:</label>
        <input type="number" id="num_resultados" name="num_resultados" min="1" required>
        <input type="submit" value="Ver resultados">
    </form>
    
    <h2>Top 10 vulnerabilidades</h2>
    <li><a href="/top-vulnerabilidades">Top 10 vulnerabilidades basado a tiempo real</a></li>
    '''

@app.route('/resultados')
def resultados():
    num_resultados = int(request.args.get('num_resultados', 5))
    return '''
    <h1>Resultados</h1>
    <ul>
        <li><a href="/top-clientes?num_resultados={0}">Top clientes con más incidencias</a></li>
        <li><a href="/top-incidencias?num_resultados={0}">Top tipo de incidencias con más tiempo</a></li>
        <li><a href="/empleados?num_resultados={0}">Empleados con más tiempo resolviendo incidencias</a></li>
    </ul>
    '''.format(num_resultados)


@app.route('/top-clientes')
def top_clientes():
    num_resultados = int(request.args.get('num_resultados', 5))
    con = sqlite3.connect("incidentes.db")
    query_tickets = "SELECT * FROM tickets_emitidos"
    df_tickets = pd.read_sql_query(query_tickets, con)
    con.close()
    df_clientes_mas_incidencias = df_tickets.groupby('id_cliente')['tipo_incidencia'].count().reset_index(name='count').sort_values(by='count', ascending=False)
    result_clientes = df_clientes_mas_incidencias.head(num_resultados).apply(lambda x: f"Id del cliente: {x['id_cliente']} - Número de incidencias: {x['count']}", axis=1).str.cat(sep="<br>")
    return "Clientes con más incidencias (id - número de incidencias):<br>" + result_clientes + "<br>"


@app.route('/top-incidencias')
def top_incidencias():
    num_resultados = int(request.args.get('num_resultados', 5))
    con = sqlite3.connect("incidentes.db")
    query_tipo_inc = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df_tipo_inc = pd.read_sql_query(query_tipo_inc, con)
    con.close()

    df_incidencia_mas_tiempo = df_tipo_inc.groupby('tipo_incidencia')['tiempo'].sum().reset_index(name='count').sort_values(by='count', ascending=False)
    result_incidencias = df_incidencia_mas_tiempo.head(num_resultados).apply(lambda x: f"Tipo de incidencia: {x['tipo_incidencia']} - Tiempo total: {x['count']}", axis=1).str.cat(sep="<br>")

    return "Incidencias con más tiempo (id - tiempo total): <br>" + result_incidencias + "<br>"


@app.route('/empleados')
def top_empleados():
    num_resultados = int(request.args.get('num_resultados', 5))
    con = sqlite3.connect("incidentes.db")
    query_ticket = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    query_emp = "SELECT * FROM empleados"
    df_ticket = pd.read_sql_query(query_ticket, con)
    df_emp = pd.read_sql_query(query_emp, con)
    con.close()

    df_empleados = pd.merge(df_ticket, df_emp, on="id_emp", how="left")
    df_empleados_mas_tiempo = df_empleados.groupby('id_emp')['tiempo'].sum().reset_index(name='count')
    df_empleados_mas_tiempo = df_empleados_mas_tiempo.merge(df_empleados.drop_duplicates(subset=['id_emp']), on='id_emp', how='left').sort_values(by='count', ascending=False)

    result_empleados = df_empleados_mas_tiempo.head(num_resultados).apply(lambda x: f"Id de empleado: {x['id_emp']}, nombre: {x['nombre']}, nivel: {x['nivel']}, fecha de contrato: {x['fecha_contrato']}, número de incidencias: {x['count']}",  axis=1).str.cat(sep="<br>")

    return "Empleados que más tiempo han empleado en resolución de incidentes:<br>" + result_empleados + "<br>"


@app.route('/top-vulnerabilidades')
def top_vulnerabilidades():
    response = requests.get('https://cve.circl.lu/api/last')
    if response.status_code == 200:
        try:
            vulnerabilities = response.json()

            if isinstance(vulnerabilities, list) and len(vulnerabilities) > 0:
                vulnerabilities = vulnerabilities[:10]
                result_vulnerabilities = "<br>".join([f"CVE: {vuln}<br>" for vuln in vulnerabilities])
                return f"Top 10 vulnerabilidades basado a tiempo real:<br>{result_vulnerabilities}"
            else:
                return "Error: La respuesta no contiene una lista de vulnerabilidades."
        except ValueError as e:
            return f"Error al procesar los datos de vulnerabilidades: Respuesta no es JSON válida. {e}"
        except Exception as e:
            return f"Error al procesar los datos de vulnerabilidades: {e}"
    else:
        return "Error al obtener las vulnerabilidades."

if __name__ == '__main__':
    app.run(debug=True)
