import sqlite3

import requests
from flask import Flask, request, redirect, url_for,session, url_for, render_template
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)

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