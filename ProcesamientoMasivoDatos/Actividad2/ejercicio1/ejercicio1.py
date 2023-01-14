#!/usr/bin/python3.9
from pyspark import SparkContext
import sys

def personaGastosSinTarjetaCredito(rdd):
    # Filtrar las entradas que sean con tarjeta de crédito
    no_credito_rdd = rdd.filter(lambda x: x.split(";")[1] != "Tarjeta de crédito")
    # Mapear cada entrada a (persona, gasto)
    no_credito_mapped = no_credito_rdd.map(lambda x: (x.split(";")[0], int(x.split(";")[2])))
    # Reducir por clave (persona) para sumar los gastos
    no_credito_reduced = no_credito_mapped.reduceByKey(lambda x, y: x + y)
    return no_credito_reduced

if __name__ == "__main__":
    if len(sys.argv) !=3:
        print("--Debes ingresar: dataset.txt pathSalida--")
        exit()
    dataset_path = str(sys.argv[1])
    result_path = str(sys.argv[2])
    # Crear SparkContext
    sc = SparkContext("local", "personaGastosSinTarjetaCredito")
    # Leer dataset
    rdd = sc.textFile(dataset_path)
    # Obtener lista de todas las personas
    all_people = rdd.map(lambda x: x.split(";")[0]).distinct().map(lambda x: (x, 0))
    # Hacer join con el resultado obtenido
    joined_result = personaGastosSinTarjetaCredito(rdd).union(all_people).reduceByKey(lambda x, y: x + y)
    # Reemplazar valores nulos con 0
    final_result = joined_result.mapValues(lambda x: x if x is not None else 0)
    # Formatear resultado como cadena
    formatted_result = final_result.map(lambda x: "{};{}".format(x[0], x[1]))
    # Guardar resultado en archivo
    formatted_result.saveAsTextFile(result_path)
    # Detener SparkContext
    sc.stop()
