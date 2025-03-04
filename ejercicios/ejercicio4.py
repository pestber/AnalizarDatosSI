import sqlite3
import pandas as pd
import json
import plotly.graph_objects as go
from flask import render_template, Flask

app = Flask(__name__)
def prueba():

    conn = sqlite3.connect('incidentes.db')


    incidentes = pd.read_sql_query("SELECT * from tickets_emitidos", conn)
    clientes = pd.read_sql_query("SELECT * from clientes", conn)
    contactos=pd.read_sql_query("SELECT * from contactos_empleados", conn)
    empleados=pd.read_sql_query("SELECT * from empleados", conn)

    contactos['patata']=empleados['nivel'] &contactos['id_emp']&empleados['id_emp']

    print(incidentes)
def grafico_criticos():


    clientesAux=[]

    conn=sqlite3.connect('incidentes.db')

    tickets=pd.read_sql_query("SELECT * from tickets_emitidos", conn)
    clientes = pd.read_sql_query("SELECT * from clientes", conn)
    tickets['critico']=(tickets['es_mantenimiento'] == True) & (tickets['tipo_incidencia'] != 1)

    #incidentes=incidentes.sort_values(by='cliente')
    #incidentes=incidentes.nsmallest(10, 'id_incidente')
    #print(incidentes)

    #nombreClientes=clientes.pop('nombre')
    for i in range(10):
        cliente = tickets[(tickets['id_cliente'] == i+1) & tickets['critico'] == True]
        incidentesCriticos=len(cliente)#se puede hacer con cliente.size, pero devuelve todos los elementos del df en vez del numero incidentes criticos(todos los elementos son los incidentes *8)
        #nombreCliente=nombreClientes.loc[i]
        aux=(incidentesCriticos)
        clientesAux.append(aux)

    clientes['incidentesCriticos']=pd.Series(clientesAux)

    #clientesAux.sort(key= lambda x:x[0], reverse=True)
    #print(clientes)
    orden=clientes.nlargest(5, 'incidentesCriticos')

    fig = go.Figure(data=go.Bar(x=orden['nombre'], y=orden['incidentesCriticos']))

    fig.update_layout(xaxis_title='Usuario', yaxis_title='Proporción')

    grafico = fig.to_json()
    conn.close()
    return grafico

    #print(incidentes['critico'])



    #print(clientes)
#prueba()


def grafico_acciones():

    conn = sqlite3.connect('incidentes.db')

    contactos = pd.read_sql_query("SELECT * from contactos_empleados", conn)
    empleados= pd.read_sql_query("SELECT * from empleados", conn)
    orden=contactos.sort_values('id_emp', ascending=True)

    aux=0
    cont=0
    accionesEmp=[]
#101=42
    for i in range(len(contactos)):

        idEmp=orden.iloc[i, 2]
        if(aux!=idEmp):
            accionesEmp.append(cont)
            aux=idEmp
            cont=1
        else:
            cont+=1

        if(i+1==len(contactos)):
            accionesEmp.append(cont)

    accionesEmp.remove(0)
    print(accionesEmp, len(accionesEmp))

    empleados['acciones']=accionesEmp

    fig = go.Figure(data=go.Bar(x=empleados['nombre'], y=empleados['acciones']))

    fig.update_layout(xaxis_title='Usuario', yaxis_title='Numero De Acciones')

    grafico = fig.to_json()
    conn.close()
    return grafico



@app.route('/')
def index():

    grafico = grafico_criticos()

    grafico2 =grafico_acciones()

    return render_template('index.html', grafico=json.dumps(grafico), grafico2=json.dumps(grafico2))


if __name__ == '__main__': app.run(debug = True)