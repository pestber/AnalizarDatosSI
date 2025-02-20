
import sqlite3
import pandas as pd
import json


def ejercicio2ETL():
    with open("datos.json", "r", encoding="utf-8") as file:
        data = json.load(file)
    #print(data)
    #print(data["tickets_emitidos"])
    con = sqlite3.connect("incidentes.db")
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    #creamos las tablas con cur.execute con la info de data.json
    # Crear la tabla de clientes
    cur.execute("CREATE TABLE IF NOT EXISTS clientes ("
                "id_cliente INTEGER PRIMARY KEY,"
                "nombre TEXT,"
                "telefono INTEGER,"
                "provincia TEXT"
                ");")
    # Crear la tabla de tipos de incidentes
    cur.execute("CREATE TABLE IF NOT EXISTS tipos_incidentes ("
                "id_inci INTEGER PRIMARY KEY,"
                "nombre TEXT"
                ");")
    # Crear la tabla de empleados
    cur.execute("CREATE TABLE IF NOT EXISTS empleados ("
                "id_emp INTEGER PRIMARY KEY,"
                "nombre TEXT,"
                "nivel INTEGER,"
                "fecha_contrato TEXT"
                ");")

    cur.execute("CREATE TABLE IF NOT EXISTS tickets_emitidos("
                "id_ticket_emitido INTEGER PRIMARY KEY AUTOINCREMENT,"
                "id_cliente INTEGER,"
                "fecha_apertura TEXT,"
                "fecha_cierre TEXT,"
                "es_mantenimiento BOOLEAN,"
                "satisfaccion_cliente INTEGER,"
                "tipo_incidencia INTEGER,"
                "FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente),"
                "FOREIGN KEY (tipo_incidencia) REFERENCES tipos_incidencias(id_inci)"
                ");")

    cur.execute("CREATE TABLE IF NOT EXISTS  contactos_empleados("
                "id_contacto INTEGER PRIMARY KEY AUTOINCREMENT,"
                "id_ticket_emitido INTEGER,"
                "id_emp INTEGER,"
                "fecha TEXT,"
                "tiempo FLOAT,"
                "FOREIGN KEY (id_emp) REFERENCES empleados(id_emp),"
                "FOREIGN KEY (id_ticket_emitido) REFERENCES tickets_emitidos(id_ticket_emitido)"
                ");")

#Insertamos los datos en la base de datos

    # Insertar los datos de clientes
    if "clientes" in data:
        for cliente in data["clientes"]:
            cur.execute("INSERT OR IGNORE INTO clientes (id_cliente, nombre, telefono, provincia) " \
                        "VALUES ('%d', '%s', '%d', '%s')" %
                        (int(cliente["id_cli"]), cliente["nombre"], int(cliente["telefono"]), cliente["provincia"]))

    # Insertar los datos de empleados
    if "empleados" in data:
        for empleado in data["empleados"]:
            cur.execute("INSERT OR IGNORE INTO empleados (id_emp, nombre, nivel, fecha_contrato) " \
                        "VALUES ('%d', '%s', '%d', '%s')" %
                        (int(empleado["id_emp"]), empleado["nombre"], int(empleado["nivel"]),
                         empleado["fecha_contrato"]))

    # Insertar los datos de tipos_incidentes
    if "tipos_incidentes" in data:
        for tipo in data["tipos_incidentes"]:
            cur.execute("INSERT OR IGNORE INTO tipos_incidentes (id_inci, nombre) " \
                        "VALUES ('%d', '%s')" %
                        (int(tipo["id_inci"]), tipo["nombre"]))

    # Insertar los datos de tickets_emitidos y sus contactos
    if "tickets_emitidos" in data:
        for ticket in data["tickets_emitidos"]:
            cur.execute(
                "INSERT OR IGNORE INTO tickets_emitidos (id_cliente, fecha_apertura, fecha_cierre, es_mantenimiento, satisfaccion_cliente, tipo_incidencia) " \
                "VALUES ('%d', '%s', '%s', '%d', '%d', '%d')" %
                (int(ticket["cliente"]), ticket["fecha_apertura"], ticket["fecha_cierre"],
                 int(ticket["es_mantenimiento"]),
                 int(ticket["satisfaccion_cliente"]), int(ticket["tipo_incidencia"])))

            id_ticket_emitido = cur.lastrowid  # Obtener el ID del ticket recién insertado

            # Insertar los contactos con empleados
            if "contactos_con_empleados" in ticket:
                for contacto in ticket["contactos_con_empleados"]:
                    cur.execute("INSERT OR IGNORE INTO contactos_empleados (id_ticket_emitido, id_emp, fecha, tiempo) " \
                                "VALUES ('%d', '%d', '%s', '%.2f')" %
                                (id_ticket_emitido, int(contacto["id_emp"]), contacto["fecha"],
                                 float(contacto["tiempo"])))

    # Guardar los cambios y cerrar la conexión
    con.commit()
    """
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cur.fetchall()
    
        print("Tablas en la base de datos:")
        for tabla in tablas:
            print(tabla[0])
    """

    con.close()

  #realizar consultas con pandas
def analizarDatos():
    con = sqlite3.connect("incidentes.db")

    query_incidentes = "SELECT * FROM incidentes WHERE cliente IS NOT NULL;"
    query_contactos = "SELECT * FROM contactos_empleados WHERE id_incidente IN (SELECT id_incidente FROM incidentes);"

    df_incidentes = pd.read_sql_query(query_incidentes, con)
    df_contactos = pd.read_sql_query(query_contactos, con)

    #Numero de muestras totales.
    print("Numero de muestras totales: ", df_incidentes["id_incidente"].count())

    # Media y desviación estándar del total de incidentes en los que ha habido una valoración mayor o igual a 5 por parte del cliente.
    print("Media de incidentes con valoración mayor o igual a 5: ", df_incidentes[df_incidentes["satisfaccion_cliente"] >= 5]["id_incidente"].count())
    print("Desviación estándar de incidentes con valoración mayor o igual a 5: ", df_incidentes[df_incidentes["satisfaccion_cliente"] >= 5]["id_incidente"].std())

    # Media y desviación estándar del total del número de incidentes por cliente.
    print("Media de incidentes por cliente: ", df_incidentes.groupby("cliente").count().mean()["id_incidente"])
    print("Desviación estándar de incidentes por cliente: ", df_incidentes.groupby("cliente").count().std()["id_incidente"])

    # Media y desviación estándar del número de horas totales realizadas en cada incidente.
    print("Media de horas totales realizadas: ", df_contactos.groupby("id_incidente").sum()["tiempo"].mean())
    print("Desviación estándar de horas totales realizadas: ", df_contactos.groupby("id_incidente").sum()["tiempo"].std())

    # Valor mínimo y valor máximo del total de horas realizadas por los empleados.
    print("Valor mínimo de horas realizadas por empleado: ", df_contactos.groupby("id_emp").sum()["tiempo"].min())
    print("Valor máximo de horas realizadas por empleado: ", df_contactos.groupby("id_emp").sum()["tiempo"].max())

    # Valor mínimo y valor máximo del tiempo entre apertura y cierre de incidente.
    df_incidentes["fecha_apertura"] = pd.to_datetime(df_incidentes["fecha_apertura"])
    df_incidentes["fecha_cierre"] = pd.to_datetime(df_incidentes["fecha_cierre"])
    df_incidentes["tiempo_incidente"] = df_incidentes["fecha_cierre"] - df_incidentes["fecha_apertura"]

    # Valor mínimo y valor máximo del número de incidentes atendidos por cada empleado
    print("Valor mínimo de incidentes atendidos por empleado: ", df_contactos.groupby("id_emp").count()["id_incidente"].min())
    con.close()

ejercicio2ETL()
#analizarDatos()




