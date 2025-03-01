
import sqlite3
import pandas as pd
import numpy as np
import json
import ejercicio2ETL

def analisis_estadistico(datos):
    if len(datos) == 0:
        return {
            'mediana': None,
            'media': None,
            'varianza': None,
            'maximo': None,
            'minimo': None
        }
    return {
        'mediana': np.median(datos),
        'media': np.mean(datos),
        'varianza': np.var(datos),
        'maximo': np.max(datos),
        'minimo': np.min(datos)
    }

def agrupacion_empleado():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    print("Agrupación por empleado")
    print("Introduzca id del empleado")
    id_empleado = input().strip()
    id_empleado = int(id_empleado)
    df = df[df['id_emp']==id_empleado]

    #num_inc = len(df[df['id_ticket_emitido'].unique()])
    num_inc = len(df["id_ticket_emitido"].drop_duplicates())

    num_cont = len(df)
    #estadistica = analisis_estadistico(df)

    print(df)
    print(f"Número de incidentes: {num_inc}")
    print(f"Número de contactos: {num_cont}")
    #print(estadistica)

def agrupacion_nivel_empleado():
    con = sqlite3.connect("incidentes.db")
    query_tick = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    query_emp = "SELECT id_emp, nivel FROM empleados"
    df_tick = pd.read_sql_query(query_tick, con)
    df_emp= pd.read_sql_query(query_emp, con)
    df= pd.merge(df_tick, df_emp, on="id_emp", how="left")

    print("Agrupación por nivel de empleado")
    print("Introduzca nivel de empleado (número del 1 al 4)")
    nivel_empleado = input().strip()
    nivel_empleado=int(nivel_empleado)
    if (0>nivel_empleado>3):
        return 0
    df = df[df['nivel'] == nivel_empleado]

    #num_inc = len(df[df['id_ticket_emitido'].unique()])
    num_inc = len(df["id_ticket_emitido"].drop_duplicates())
    num_cont = len(df)
    #estadistica = analisis_estadistico(df)

    print(df)
    print(f"Número de incidentes: {num_inc}")
    print(f"Número de contactos: {num_cont}")
    #print(estadistica)

def agrupacion_cliente():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    print("Agrupación por cliente")
    print("Introduzca id del cliente")
    id_cliente = input().strip()
    id_cliente = int(id_cliente)
    df = df[df['id_cliente'] == id_cliente]

    #num_inc = len(df[df['id_ticket_emitido'].unique()])
    num_inc = len(df["id_ticket_emitido"].drop_duplicates())
    num_cont = len(df)
    #estadistica = analisis_estadistico(df)

    print(df)
    print(f"Número de incidentes: {num_inc}")
    print(f"Número de contactos: {num_cont}")
    #print(estadistica)


def agrupacion_tipo_inc():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    print("Agrupación por tipo de incidencia")
    print("Introduzca tipo de incidencia (número del 1 al 5)")
    tipo_inc = input().strip()
    tipo_inc = int(tipo_inc)
    if(0 > tipo_inc > 5):
        return 0
    df = df[df['tipo_incidencia'] == tipo_inc]

    #num_inc = len(df[df['id_ticket_emitido'].unique()])
    num_inc = len(df["id_ticket_emitido"].drop_duplicates())
    num_cont = len(df)
    #estadistica = analisis_estadistico(df)

    print(df)
    print(f"Número de incidentes: {num_inc}")
    print(f"Número de contactos: {num_cont}")
    #print(estadistica)


def agrupacion_dia_semana():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"])
    df["fecha_cierre"] = pd.to_datetime(df["fecha_cierre"])
    df["fecha"] = pd.to_datetime(df["fecha"])                       #fecha contacto

    df["dia_semana_apertura"] = df["fecha_apertura"].dt.day_name()
    df["dia_semana_cierre"] = df["fecha_cierre"].dt.day_name()
    df["dia_semana_contacto"] = df["fecha"].dt.day_name()

    print("Agrupación por día de la semana")
    fecha = input("Introduzca un día de la semana en ingles: ").strip().capitalize()

    if fecha in df["dia_semana_apertura"].unique():
        print("\nTickets abiertos en", fecha)
        df_ap = df[df["dia_semana_apertura"] == fecha]

    if fecha in df["dia_semana_cierre"].unique():
        print("\nTickets cerrados en", fecha)
        df_cerr = df[df["dia_semana_cierre"] == fecha]

    if fecha in df["dia_semana_contacto"].unique():
        print("\nContactos realizados en", fecha)
        df_cont = df[df["dia_semana_contacto"] == fecha]

    #Apertura
    #num_inc = len(df[df['id_ticket_emitido'].unique()])
    num_inc = len(df_ap["id_ticket_emitido"].drop_duplicates())
    num_cont = len(df_ap)
    #estadistica = analisis_estadistico(df)

    print("Fecha de apertura")
    print(df_ap)
    print(f"Número de incidentes: {num_inc}")
    print(f"Número de contactos: {num_cont}")
    #print(estadistica)

    #Cierre
    #num_inc = len(df[df['id_ticket_emitido'].unique()])
    num_inc = len(df_cerr["id_ticket_emitido"].drop_duplicates())
    num_cont = len(df_cerr)
    #estadistica = analisis_estadistico(df)

    print("Fecha de cierre")
    print(df_cerr)
    print(f"Número de incidentes: {num_inc}")
    print(f"Número de contactos: {num_cont}")
    #print(estadistica)

    #Contacto
    #num_inc = len(df[df['id_ticket_emitido'].unique()])
    num_inc = len(df_cont["id_ticket_emitido"].drop_duplicates())
    num_cont = len(df_cont)
    #estadistica = analisis_estadistico(df)

    print("Fecha de contacto")
    print(df_cont)
    print(f"Número de incidentes: {num_inc}")
    print(f"Número de contactos: {num_cont}")
    #print(estadistica)

    con.close()


def menu_principal():
    while True:
        print("\nOpciones:")
        print("1. Agrupación por empleado")
        print("2. Agrupación por nivel de empleado")
        print("3. Agrupación por cliente")
        print("4. Agrupación por tipo de incidente")
        print("5. Agrupación por día de la semana")
        opcion = input("Seleccione una opción: ").strip()
        if opcion == "1":
            agrupacion_empleado()
        elif opcion == "2":
            agrupacion_nivel_empleado()
        elif opcion == "3":
            agrupacion_cliente()
        elif opcion == "4":
            agrupacion_tipo_inc()
        elif opcion == "5":
            agrupacion_dia_semana()
        else:
            print("Opción no válida")

menu_principal()