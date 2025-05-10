import sqlite3
import joblib
import pandas as pd
import numpy as np
import json
import requests
from flask import Flask, request, redirect, url_for,session, url_for, render_template
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import ejercicios.ejercicio2ETL

model_lr = joblib.load('modelos/model_lr.pkl')
model_dt = joblib.load('modelos/model_dt.pkl')
model_rf = joblib.load('modelos/model_rf.pkl')
with open('columnas_modelo.json', 'r') as f:
    expected_columns = json.load(f)
app = Flask(__name__)
app.secret_key = "secretkey"


@app.route('/')
def inicio():
    ejercicios.ejercicio2ETL.ejercicio2ETL()

    return '''
    <h1>Práctica Sistemas de la Información<br></h1>
    <h2>Top X</h2>
    <li> Clientes con más incidencias</li>
    <li> Tipo de incidencias con más tiempo</li>
    <li> Empleados con más tiempo resolviendo incidencias</li>
    
    <p><strong>Introduce el número de resultados que deseas ver:</strong></p>
    <form action="/resultados" method="get">
        <label for="num_resultados">Número de resultados:</label>
        <input type="number" id="num_resultados" name="num_resultados" min="1" required>
        <input type="submit" value="Ver resultados">
    </form>
    
    <h2>Top 10</h2>
    <li><a href="/top-vulnerabilidades">Vulnerabilidades, basado a tiempo real</a></li>
    <li><a href="/top-proveedores">Proveedores</a></li>
     
    <h2>Incidencias</h2>
    <li><a href="/tiempo-medio">Tiempo medio de resolución por tipo de incidencia</a></li>
    
    <h2>Registro y Login</h2>
    <li><a href="/register">Registrar usuario</a></li>
    <li><a href="/login">Iniciar sesión</a></li>
    <li><a href="/home">Página de inicio</a></li>
    
    <h2><a href="/predecir">Predecir Críticos</a></h2>
    
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
    return "Clientes con más incidencias (id - número de incidencias):<br>" + result_clientes + "<br>" + "<br><a href='/'>Volver al inicio</a>"


@app.route('/top-incidencias')
def top_incidencias():
    num_resultados = int(request.args.get('num_resultados', 5))
    con = sqlite3.connect("incidentes.db")
    query_tipo_inc = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df_tipo_inc = pd.read_sql_query(query_tipo_inc, con)
    con.close()

    df_incidencia_mas_tiempo = df_tipo_inc.groupby('tipo_incidencia')['tiempo'].sum().reset_index(name='count').sort_values(by='count', ascending=False)
    result_incidencias = df_incidencia_mas_tiempo.head(num_resultados).apply(lambda x: f"Tipo de incidencia: {x['tipo_incidencia']} - Tiempo total: {x['count']}", axis=1).str.cat(sep="<br>")

    return "Incidencias con más tiempo (id - tiempo total): <br>" + result_incidencias + "<br>" + "<br><a href='/'>Volver al inicio</a>"


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

    return "Empleados que más tiempo han empleado en resolución de incidentes:<br>" + result_empleados + "<br>" + "<br><a href='/'>Volver al inicio</a>"


@app.route('/top-vulnerabilidades')
def top_vulnerabilidades():
    response = requests.get('https://cve.circl.lu/api/last')
    if response.status_code == 200:
        try:
            vulnerabilities = response.json()

            if isinstance(vulnerabilities, list) and len(vulnerabilities) > 0:
                vulnerabilities = vulnerabilities[:10]
                result_vulnerabilities = "<br>".join([f"CVE: {vuln}<br><br>" for vuln in vulnerabilities])
                return (f"Top 10 vulnerabilidades basado a tiempo real:<br><br>{result_vulnerabilities}"
                        f"<br><a href='/'>Volver al inicio</a>")
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
                return (f"Top 10 proveedores:<br><br>{result_proveedores}"
                        f"<br><a href='/'>Volver al inicio</a>")
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
        return (f"Bienvenido, usuario {session['user_id']}!"
                f"<br><a href='/'>Volver al inicio</a>")

    return redirect(url_for('login'))

@app.route('/tiempo-medio')
def tiempo_medio():
    con = sqlite3.connect('incidentes.db')
    query = "SELECT tipo_incidencia, AVG(julianday(fecha_cierre) - julianday(fecha_apertura)) AS tiempo_promedio FROM tickets_emitidos GROUP BY tipo_incidencia"
    df = pd.read_sql_query(query, con)
    con.close()
    result = df.apply(lambda x: f"Tipo de incidencia: {x['tipo_incidencia']} - Tiempo promedio: {x['tiempo_promedio']:.2f} días", axis=1).str.cat(sep="<br>")
    return (f"Tiempo medio de resolución por tipo de incidencia:<br>{result}"
            f"<br><a href='/'>Volver al inicio</a>")



@app.route('/predecir', methods=['GET', 'POST'])
def algoritmosIA():
    if request.method == 'POST':

        cliente_id = request.form['cliente']
        fecha_apertura = request.form['fecha_apertura']
        fecha_cierre = request.form['fecha_cierre']
        mantenimiento = 1 if request.form['mantenimiento'] == '1' else 0
        tipo_incidente = int(request.form['tipo_incidente'])


        tiempo_resolucion = (datetime.strptime(fecha_cierre, '%Y-%m-%d') - datetime.strptime(fecha_apertura, '%Y-%m-%d')).days

        conn = sqlite3.connect('incidentes.db')
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM tickets_emitidos WHERE id_cliente = ?', (cliente_id,))
        total_incidencias = cursor.fetchone()[0]
        conn.close()


        input_data = pd.DataFrame([[total_incidencias, tiempo_resolucion, mantenimiento, tipo_incidente]], columns=['total_incidencias', 'tiempo_resolucion', 'mantenimiento', 'tipo_incidente'])
        input_data = pd.get_dummies(input_data)


        for col in expected_columns:
            if col not in input_data.columns:
                input_data[col] = 0
        input_data = input_data[expected_columns]

        metodo = request.form['metodo']
        if metodo == 'lr':
            prediction = model_lr.predict(input_data)[0]
            prediction = 1 if prediction >= 0.5 else 0
        elif metodo == 'dt':
            prediction = model_dt.predict(input_data)[0]
        else:
            prediction = model_rf.predict(input_data)[0]


        resultado = "CRÍTICO" if prediction == 1 or prediction==True else "NO CRÍTICO"

        return f'''
               <h3>Resultado: {resultado}</h3>
               <a href="/">Volver</a>
           '''

    return ''' <form method="POST">
      ID Cliente: <input type="text" name="cliente" placeholder="ID Cliente" required><br>
      Fecha Inicio: <input type="date" name="fecha_apertura" required><br>
      Fecha Fin:<input type="date" name="fecha_cierre" required><br>
      Es mantenimiento:
      <select name="mantenimiento">
        <option value="1">Sí</option>
        <option value="0">No</option>
      </select><br>
      Tipo de Incidente: <select name="tipo_incidente">
        <option value="1">Tipo 1</option>
        <option value="2">Tipo 2</option>
        <option value="3">Tipo 3</option>
      </select><br>
      Algoritmo a usar:<select name="metodo">
        <option value="lr">Regresión Lineal</option>
        <option value="dt">Árbol de Decisión</option>
        <option value="rf">Random Forest</option>
      </select><br>
      <button type="submit">Predecir</button>
    </form>'''



if __name__ == '__main__':
    app.run(debug=True)
