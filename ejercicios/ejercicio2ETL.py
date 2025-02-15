
import sqlite3
import pandas as pd
import json


def ejercicio2ETL():
    with open("datos.json", "r", encoding="utf-8") as file:
        data = json.load(file)

    con = sqlite3.connect("baseDeDatosIncidentes.db")
    cur = con.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS baseDeDatosIncidentes("
                "id_incidente INTEGER PRIMARY KEY AUTOINCREMENT,"
                "cliente INTEGER,"
                "fecha_apertura TEXT,"
                "fecha_cierre TEXT"
                "es_mantenimiento BOOLEAN,"
                "satisfaccion_cliente INTEGER,"
                "tipo_incidencia integer"
                ");")

    cur.execute("CREATE TABLE IF NOT EXISTS  contactos_empleados("
                "id_contacto INTEGER PRIMARY KEY AUTOINCREMENT,"
                "id_incidente INTEGER,"
                "id_emp INTEGER,"
                "fecha TEXT,"
                "tiempo FLOAT,"
                "FOREIGN KEY (id_incidente) REFERENCES baseDeDatosIncidentes(id_incidente)"
                ");")

    
