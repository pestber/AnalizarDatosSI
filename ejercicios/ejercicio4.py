import sqlite3
import pandas as pd
import json
import plotly.graph_objects as go
from flask import render_template, Flask

app = Flask(__name__)
def grafico_criticos():

    clientesAux=[]

    conn=sqlite3.connect('incidentes.db')

    tickets=pd.read_sql_query("SELECT * from tickets_emitidos", conn)
    clientes = pd.read_sql_query("SELECT * from clientes", conn)
    tickets['critico']=(tickets['es_mantenimiento'] == True) & (tickets['tipo_incidencia'] != 1)


    for i in range(10):
        cliente = tickets[(tickets['id_cliente'] == i+1) & tickets['critico'] == True]
        incidentesCriticos=len(cliente) #se puede hacer con cliente.size, pero devuelve todos los elementos del df en vez del numero incidentes criticos(todos los elementos son los incidentes *8)
        aux=(incidentesCriticos)
        clientesAux.append(aux)

    clientes['incidentesCriticos']=pd.Series(clientesAux)

    orden=clientes.nlargest(5, 'incidentesCriticos')

    fig = go.Figure(data=go.Bar(x=orden['nombre'], y=orden['incidentesCriticos']))

    fig.update_layout(xaxis_title='Usuario', yaxis_title='Proporción')

    grafico = fig.to_json()
    conn.close()
    return grafico


def grafico_acciones():

    conn = sqlite3.connect('incidentes.db')

    contactos = pd.read_sql_query("SELECT * from contactos_empleados", conn)
    empleados= pd.read_sql_query("SELECT * from empleados", conn)
    orden=contactos.sort_values('id_emp', ascending=True)

    aux=0
    cont=0
    accionesEmp=[]

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

    #este for se puede evitar con el metodo value_counts() el cual descubrí mas tarde

    accionesEmp.remove(0)

    empleados['acciones']=accionesEmp

    fig = go.Figure(data=go.Bar(x=empleados['nombre'], y=empleados['acciones']))

    fig.update_layout(xaxis_title='Usuario', yaxis_title='Numero De Acciones')

    grafico = fig.to_json()
    conn.close()
    return grafico


def grafico_dias():

    conn = sqlite3.connect('incidentes.db')

    tickets = pd.read_sql_query("SELECT * from tickets_emitidos", conn)

    tickets["fecha_apertura"]=pd.to_datetime(tickets["fecha_apertura"])

    tickets["fecha_apertura"]=pd.to_datetime(tickets["fecha_apertura"])

    tickets['dia_semana_apretura']=tickets["fecha_apertura"].dt.day_name()
    orden = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dias=tickets['dia_semana_apretura'].value_counts()

    dias=dias.reindex(orden, fill_value=0)

    df_dias = dias.reset_index()

    df_dias.columns = ['dia_semana', 'cantidad']

    print(df_dias)

    fig = go.Figure(data=go.Bar(x=df_dias['dia_semana'], y=df_dias['cantidad']))

    fig.update_layout(xaxis_title='Dia', yaxis_title='Numero De Acciones')

    grafico = fig.to_json()
    conn.close()
    return grafico


def grafico_tiempo_resol_incidente():
    conn = sqlite3.connect('incidentes.db')

    tickets = pd.read_sql_query("SELECT * FROM tickets_emitidos", conn)
    contactos = pd.read_sql_query("SELECT * FROM contactos_empleados", conn)
    tipos_incidentes = pd.read_sql_query("SELECT * FROM tipos_incidentes", conn)

    tiempo_por_incidente = contactos.groupby("id_ticket_emitido")["tiempo"].sum().reset_index()

    incidencias = tickets.merge(tiempo_por_incidente).merge(tipos_incidentes, left_on="tipo_incidencia", right_on="id_inci")

    percentiles = incidencias.groupby("nombre")["tiempo"].quantile([0.05, 0.90]).unstack()

    fig = go.Figure()

    for incidente in percentiles.index:
        fig.add_trace(go.Box(y=incidencias[incidencias["nombre"] == incidente]["tiempo"], name=incidente, boxpoints=False,))


    grafico = fig.to_json()
    conn.close()
    return grafico


def grafico_media_tiempo():

    conn = sqlite3.connect('incidentes.db')

    tickets = pd.read_sql_query("SELECT * FROM tickets_emitidos", conn)

    tickets["fecha_apertura"] = pd.to_datetime(tickets["fecha_apertura"])
    tickets["fecha_cierre"] = pd.to_datetime(tickets["fecha_cierre"])

    tickets["tiempo_resolucion"] = (tickets["fecha_cierre"] - tickets["fecha_apertura"]).dt.total_seconds() / 3600

    media_tiempo = tickets.groupby("es_mantenimiento")["tiempo_resolucion"].mean().reset_index()

    fig = go.Figure(data=go.Bar(x=media_tiempo['es_mantenimiento'], y=media_tiempo['tiempo_resolucion']))

    fig.update_xaxes(tickvals=[1, 0], ticktext=["Mantenimiento", "No Mantenimiento"])

    fig.update_layout(xaxis_title='Es Mantenimiento', yaxis_title='Tiempo')

    grafico = fig.to_json()
    conn.close()
    return grafico



@app.route('/')
def index():

    grafico = grafico_criticos()

    grafico2 = grafico_acciones()

    grafico3 = grafico_dias()

    grafico4 = grafico_media_tiempo()

    grafico5 = grafico_tiempo_resol_incidente()


    return render_template('index.html', grafico=json.dumps(grafico), grafico2=json.dumps(grafico2),  grafico3=json.dumps(grafico3),  grafico4=json.dumps(grafico4), grafico5=json.dumps(grafico5))


if __name__ == '__main__': app.run(debug = True)