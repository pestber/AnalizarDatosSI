
import sqlite3
import pandas as pd
import json


def agrupacion_empleado():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    print("Agrupación por empleado")
    print("Introduzca id del empleado")
    id_empleado = input().strip()
    print(df['id_emp']==id_empleado)


def agrupacion_nivel_empleado():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    query_emp = "SELECT id_emp, nivel FROM empleados"
    df_tick = pd.read_sql_query(query, con)
    df_emp= pd.read_sql_query(query_emp, con)
    df= pd.merge(df_emp, df_tick, on="id_emp", how="left")

    print("Agrupación por nivel de empleado")
    print("Introduzca nivel de empleado (número del 1 al 4)")
    nivel_empleado = input().strip()
    nivel_empleado=int(nivel_empleado)
    if (0<nivel_empleado<5):
        print(df[df['nivel'] == nivel_empleado])


def agrupacion_cliente():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos"
    df = pd.read_sql_query(query, con)

    print("Agrupación por cliente")
    print("Introduzca id del cliente")
    id_cliente = input().strip()
    print(df['cliente'] == id_cliente)


def agrupacion_tipo_inc():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos"
    df = pd.read_sql_query(query, con)

    print("Agrupación por tipo de incidencia")
    print("Introduzca tipo de incidencia (número del 1 al 5)")
    tipo_inc = input().strip()
    if(tipo_inc>=1 or tipo_inc<=5):
        print(df['tipo_incidencia'] == tipo_inc)


def agrupacion_dia_semana():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    # Convertir las fechas a formato datetime
    df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"])
    df["fecha_cierre"] = pd.to_datetime(df["fecha_cierre"])
    df["fecha"] = pd.to_datetime(df["fecha"])                       #fecha contacto

    # Extraer el día de la semana (nombre completo)
    df["dia_semana_apertura"] = df["fecha_apertura"].dt.day_name()
    df["dia_semana_cierre"] = df["fecha_cierre"].dt.day_name()
    df["dia_semana_contacto"] = df["fecha"].dt.day_name()

    print("Agrupación por día de la semana")
    fecha = input("Introduzca un día de la semana en ingles: ").strip().capitalize()

    if fecha in df["dia_semana_apertura"].unique():
        print("\nTickets abiertos en", fecha)
        print(df[df["dia_semana_apertura"] == fecha])

    if fecha in df["dia_semana_cierre"].unique():
        print("\nTickets cerrados en", fecha)
        print(df[df["dia_semana_cierre"] == fecha])

    if fecha in df["dia_semana_contacto"].unique():
        print("\nContactos realizados en", fecha)
        print(df[df["dia_semana_contacto"] == fecha])



agrupacion_cliente()
agrupacion_empleado()
agrupacion_tipo_inc()
agrupacion_nivel_empleado()
agrupacion_dia_semana()
#con.close()