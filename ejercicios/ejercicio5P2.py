import json
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeClassifier, export_graphviz, plot_tree
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib
from datetime import datetime
import graphviz



with open('data_clasified.json') as f:
    data = json.load(f)


X = []
Y = []
for entry in data['tickets_emitidos']:
    cliente_id = entry['cliente']
    fecha_apertura = datetime.strptime(entry['fecha_apertura'], '%Y-%m-%d')
    fecha_cierre = datetime.strptime(entry['fecha_cierre'], '%Y-%m-%d')
    tiempo_resolucion = (fecha_cierre - fecha_apertura).days
    es_mantenimiento = entry['es_mantenimiento']
    tipo_incidencia = entry['tipo_incidencia']

    conn = sqlite3.connect('incidentes.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM tickets_emitidos WHERE id_cliente = ?', (cliente_id,))
    total_incidencias = cursor.fetchone()[0]

    X.append([
        total_incidencias,
        tiempo_resolucion,
        es_mantenimiento,
        tipo_incidencia
    ])
    Y.append(entry['es_critico'])


df = pd.DataFrame(X, columns=['total_incidencias', 'tiempo_resolucion', 'mantenimiento', 'tipo_incidente'])
df = pd.get_dummies(df, columns=['tipo_incidente'])
X = df.values

columnas_modelo = df.columns.tolist()

with open('columnas_modelo.json', 'w') as f:
    json.dump(columnas_modelo, f)


X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

def plot_linear_regression(feature_names):

    model = joblib.load('modelos/model_lr.pkl')

    predictions = model.predict(X)
    plt.hist(predictions, bins=20)
    plt.axvline(x=0.5, color='red', linestyle='--', label='Umbral (0.5)')
    plt.title("Distribución de Predicciones (Regresión Lineal)")
    plt.xlabel("Valor predicho")
    plt.ylabel("Frecuencia")
    plt.legend()
    plt.savefig("distribucion_lr.png")
    plt.close()

def plot_random_forest(feature_names):

    model = joblib.load('modelos/model_rf.pkl')

    plt.figure(figsize=(20, 10))
    estimator = model.estimators_[0]
    plot_tree(estimator,
              feature_names=feature_names,
              class_names=['No Crítico', 'Crítico'],
              filled=True,
              rounded=True)
    plt.title("Árbol Representativo (Random Forest)")
    plt.savefig("arbol_rf.png")
    plt.close()

def generate_tree_diagrams():
    model_dt = joblib.load('modelos/model_dt.pkl')
    dot_data = export_graphviz(model_dt, out_file=None,
                               feature_names=df.columns,
                               class_names=['No Crítico', 'Crítico'],
                               filled=True)
    graph = graphviz.Source(dot_data)
    graph.render("decision_tree")


def training():

    model_lr = LinearRegression()
    model_lr.fit(X_train, y_train)
    joblib.dump(model_lr, 'modelos/model_lr.pkl')

    model_rf = RandomForestClassifier(n_estimators=100)
    model_rf.fit(X_train, y_train)
    joblib.dump(model_rf, 'modelos/model_rf.pkl')

    model_dt = DecisionTreeClassifier(max_depth=3)
    model_dt.fit(X_train, y_train)
    joblib.dump(model_dt, 'modelos/model_dt.pkl')


training()
plot_linear_regression(df.columns)
plot_random_forest(df.columns)
generate_tree_diagrams()
