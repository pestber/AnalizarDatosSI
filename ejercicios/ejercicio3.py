
import sqlite3
import pandas as pd
import json


def agrupacion_empleado():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    id_empleado = input().strip()
    print(df['id_emp']==id_empleado)

def agrupacion_nivel_empleado():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    query_emp = "SELECT id_emp, nivel FROM empleados"
    df_tick = pd.read_sql_query(query, con)
    df_emp= pd.read_sql_query(query_emp, con)
    df= pd.merge(df_emp, df_tick, on="id_emp", how="left")
          #(df_emp, on=id_emp, how=left))

    nivel_empleado = input().strip()
    nivel_empleado=int(nivel_empleado)
    if (0<nivel_empleado<5):
        print(df[df['nivel'] == nivel_empleado])

def agrupacion_cliente():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos"
    df = pd.read_sql_query(query, con)

    id_cliente = input().strip()
    print(df['cliente'] == id_cliente)


def agrupacion_tipo_inc():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos"
    df = pd.read_sql_query(query, con)

    tipo_inc = input().strip()
    if(tipo_inc>=1 or tipo_inc<=5):
        print(df['tipo_incidencia'] == tipo_inc)

def agrupacion_dia_semana():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos"
    df = pd.read_sql_query(query, con)


agrupacion_nivel_empleado()