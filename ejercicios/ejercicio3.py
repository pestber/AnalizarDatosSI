
import sqlite3
import pandas as pd
import numpy as np
import json
import ejercicio2ETL


def estadistica(datos):
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

def analisis_estadistico(df_fraude):
    num_inc = len(df_fraude["id_ticket_emitido"].drop_duplicates())
    num_cont = len(df_fraude)
    estadistica_mantenimiento = estadistica(df_fraude['es_mantenimiento'])
    estadistica_satisf = estadistica(df_fraude['satisfaccion_cliente'])
    estadistica_tiempo_cont = estadistica(df_fraude['tiempo'])

    print("Datos de fraude:")
    print(df_fraude)
    print(f"Número de incidentes: {num_inc}")
    print(f"Número de contactos: {num_cont}")
    print(f"Estadistica sobre mantenimiento: {estadistica_mantenimiento}")
    print(f"Estadistica sobre satisfaccion de cliente: {estadistica_satisf}")
    print(f"Estadistica sobre el tiempo de contacto: {estadistica_tiempo_cont}")


def agrupacion_empleado():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    print("Agrupación por empleado")
    print("Introduzca id del empleado")
    id_empleado = input().strip()
    id_empleado = int(id_empleado)
    df = df[df['id_emp']==id_empleado]
    df_fraude = df[df['tipo_incidencia']==5]
    print(df)
    analisis_estadistico(df_fraude)

    con.close()


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
    df_fraude = df[df['tipo_incidencia'] == 5]
    print(df)
    analisis_estadistico(df_fraude)

    con.close()


def agrupacion_cliente():
    con = sqlite3.connect("incidentes.db")
    query = "SELECT * FROM tickets_emitidos JOIN contactos_empleados ON tickets_emitidos.id_ticket_emitido = contactos_empleados.id_ticket_emitido"
    df = pd.read_sql_query(query, con)

    print("Agrupación por cliente")
    print("Introduzca id del cliente")
    id_cliente = input().strip()
    id_cliente = int(id_cliente)
    df = df[df['id_cliente'] == id_cliente]
    df_fraude = df[df['tipo_incidencia'] == 5]
    print(df)
    analisis_estadistico(df_fraude)

    con.close()


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
    df_fraude = df[df['tipo_incidencia'] == 5]
    print(df)
    analisis_estadistico(df_fraude)

    con.close()


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
        df_ap = df[df["dia_semana_apertura"] == fecha]
        df_ap_fraude = df_ap[df_ap['tipo_incidencia']==5]

    if fecha in df["dia_semana_cierre"].unique():
        df_cerr = df[df["dia_semana_cierre"] == fecha]
        df_cerr_fraude = df_cerr[df_cerr['tipo_incidencia']==5]

    if fecha in df["dia_semana_contacto"].unique():
        df_cont = df[df["dia_semana_contacto"] == fecha]
        df_cont_fraude = df_cont[df_cont['tipo_incidencia']==5]

    #Apertura
    print("Fecha de apertura")
    print(df_ap)
    analisis_estadistico(df_ap_fraude)

    #Cierre
    print("Fecha de cierre")
    print(df_cerr)
    analisis_estadistico(df_cerr_fraude)

    #Contacto
    print("Fecha de contacto")
    print(df_cont)
    analisis_estadistico(df_cont_fraude)

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