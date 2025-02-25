
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
    query_emp = "SELECT * FROM empleados"
    df = pd.read_sql_query(query, con)
    print(df)
    #df['nivel']=


    nivel_empleado = input().strip()
    print(df['nivel'] == nivel_empleado)

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
    print(df['tipo_incidencia'] == tipo_inc)

def agrupacion_dia_semana():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos"
    df = pd.read_sql_query(query, con)
