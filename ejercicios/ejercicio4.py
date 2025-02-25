import sqlite3
import pandas as pd
import json
import plotly.graph_objects as go
from flask import render_template, Flask

app = Flask(__name__)
def prueba():

    conn = sqlite3.connect('incidentes.db')


    incidentes = pd.read_sql_query("SELECT * from incidentes", conn)
    clientes = pd.read_sql_query("SELECT * from clientes", conn)
    contactos=pd.read_sql_query("SELECT * from contactos_empleados", conn)
    empleados=pd.read_sql_query("SELECT * from empleados", conn)

    contactos['patata']=empleados['nivel'] &contactos['id_emp']&empleados['id_emp']

    print(incidentes)
def grafico_criticos():


    clientesAux=[]

    conn=sqlite3.connect('incidentes.db')

    incidentes=pd.read_sql_query("SELECT * from incidentes", conn)
    clientes = pd.read_sql_query("SELECT * from clientes", conn)
    incidentes['critico']=(incidentes['es_mantenimiento'] == True) & (incidentes['tipo_incidencia'] != 1)

    #incidentes=incidentes.sort_values(by='cliente')
    #incidentes=incidentes.nsmallest(10, 'id_incidente')
    #print(incidentes)

    #nombreClientes=clientes.pop('nombre')
    for i in range(10):
        cliente = incidentes[(incidentes['cliente'] == i+1) & incidentes['critico'] == True]
        incidentesCriticos=len(cliente)#se puede hacer con cliente.size, pero devuelve todos los elementos del df en vez del numero incidentes criticos(todos los elementos son los incidentes *8)
        #nombreCliente=nombreClientes.loc[i]
        aux=(incidentesCriticos)
        clientesAux.append(aux)

    clientes['incidentesCriticos']=pd.Series(clientesAux)

    #clientesAux.sort(key= lambda x:x[0], reverse=True)
    #print(clientes)
    orden=clientes.nlargest(5, 'incidentesCriticos')

    fig2 = go.Figure(data=go.Bar(x=orden['nombre'], y=orden['incidentesCriticos']))

    fig2.update_layout(xaxis_title='Usuario', yaxis_title='Proporción')

    grafico = fig2.to_json()
    conn.close()
    return grafico

    #print(incidentes['critico'])



    #print(clientes)
#prueba()

@app.route('/')
def index():

    grafico = grafico_criticos()

    return render_template('index.html', grafico=json.dumps(grafico))


if __name__ == '__main__': app.run(debug = True)