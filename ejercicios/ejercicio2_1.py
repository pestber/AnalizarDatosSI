import sqlite3
import pandas as pd
import numpy as np
import json
import requests
from flask import Flask, request, redirect, url_for,session, url_for, render_template
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)


@app.route('/')
def inicio():
    return '''
    <h1>Práctica Sistemas de la Información<br></h1>
    <h2>Top de clientes con más incidencias, tipo de incidencias con más tiempo o empleados con mas tiempo resolviendo incidencias</h2>
    <p>Introduce el número de resultados que deseas ver:</p>
    <form action="/resultados" method="get">
        <label for="num_resultados">Número de resultados:</label>
        <input type="number" id="num_resultados" name="num_resultados" min="1" required>
        <input type="submit" value="Ver resultados">
    </form>
    
    <h2>Top 10</h2>
    <li><a href="/top-vulnerabilidades">Top 10 vulnerabilidades basado a tiempo real</a></li>
    <li><a href="/top-proveedores">Top 10 proveedores</a></li>
     
     <h2>Incidencias</h2>
     <li><a href="/tiempo-medio">Tiempo medio de resolución por tipo de incidencia</a></li>
    
    <h2>Registro y Login</h2>
    <li><a href="/register">Registrar usuario</a></li>
    <li><a href="/login">Iniciar sesión</a></li>
    <li><a href="/home">Página de inicio</a></li>
   
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



#Ejercicio 4

@app.route('/top-proveedores')
def top_proveedores():
    response = requests.get('https://cve.circl.lu/api/browse')
    if response.status_code == 200:
        try:
            proveedores = response.json()

            if isinstance(proveedores, list) and len(proveedores) > 0:
                proveedores = proveedores[:10]
                result_proveedores = "<br>".join([f"Proveedor: {vuln}<br>" for vuln in proveedores])
                return f"Top 10 proveedores:<br>{result_proveedores}"
            else:
                return "Error: La respuesta no contiene una lista de proveedores."
        except ValueError as e:
            return f"Error al procesar los datos de proveedores: Respuesta no es JSON válida. {e}"
        except Exception as e:
            return f"Error al procesar los datos de proveedores: {e}"
    else:
        return "Error al obtener los proveedores."


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = generate_password_hash(request.form['password'])
        con = sqlite3.connect('incidentes.db')
        cur = con.cursor()
        cur.execute("INSERT INTO usuarios (username, password) VALUES (?, ?)", (username, password))
        con.commit()
        con.close()
        return redirect(url_for('login'))
    return '''
        <form method="post">
            Usuario: <input type="text" name="username" required><br>
            Contraseña: <input type="password" name="password" required><br>
            <input type="submit" value="Registrar">
        </form>
    '''

# Ruta para iniciar sesión
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        con = sqlite3.connect('incidentes.db')
        cur = con.cursor()
        cur.execute("SELECT * FROM usuarios WHERE username = ?", (username,))
        user = cur.fetchone()
        con.close()
        if user and check_password_hash(user[2], password):
            session['user_id'] = user[0]
            return redirect(url_for('home'))
        return "Usuario o contraseña incorrectos"
    return '''
        <form method="post">
            Usuario: <input type="text" name="username" required><br>
            Contraseña: <input type="password" name="password" required><br>
            <input type="submit" value="Iniciar sesión">
        </form>
    '''

# Ruta protegida
@app.route('/home')
def home():
    if 'user_id' in session:
        return f"Bienvenido, usuario {session['user_id']}!"
    return redirect(url_for('login'))

@app.route('/tiempo-medio')
def tiempo_medio():
    con = sqlite3.connect('incidentes.db')
    query = "SELECT tipo_incidencia, AVG(julianday(fecha_cierre) - julianday(fecha_apertura)) AS tiempo_promedio FROM tickets_emitidos GROUP BY tipo_incidencia"
    df = pd.read_sql_query(query, con)
    con.close()
    result = df.apply(lambda x: f"Tipo de incidencia: {x['tipo_incidencia']} - Tiempo promedio: {x['tiempo_promedio']:.2f} días", axis=1).str.cat(sep="<br>")
    return f"Tiempo medio de resolución por tipo de incidencia:<br>{result}"

if __name__ == '__main__':
    app.run(debug=True)
