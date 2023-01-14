#!/usr/bin/python3.9

from pyspark import SparkConf, SparkContext
import sys

def personaYMetodosDePago(input_file, output_file_mayor_1500, output_file_menor_1500):
    # Configurar el contexto de Spark
    conf = SparkConf().setAppName("personaYMetodosDePago")
    sc = SparkContext(conf = conf)

    # Cargar el dataset en un RDD
    rdd = sc.textFile(input_file)

    # Dividir cada entrada en una tupla con los campos "persona", "método_pago" y "dinero_gastado"
    rdd = rdd.map(lambda x: tuple(x.split(";")))

    # Dividir las compras en dos grupos: las que gastaron más de 1500 euros 
    # y las que gastaron menos de 1500 euros y que fueran con Tarjeta de crédito
    rdd_mayor_1500 = rdd.filter(lambda x: (x[1] == "Tarjeta de crédito" and int(x[2]) > 1500))
    rdd_menor_1500 = rdd.filter(lambda x: (x[1] == "Tarjeta de crédito" and int(x[2]) < 1500))

    # Seleccionar las entradas que no pagaron con tarjeta de crédito
    rdd_no_tarjeta = rdd.filter(lambda x: (x[1] != "Tarjeta de crédito"))

    # Asignar el valor 0 al campo "dinero_gastado" comparar con los rdd_mayor y rdd_menor para quitar coincidencias
    # Aplicar un distinct para coger valores únicos
    rdd_no_tarjeta = rdd_no_tarjeta.map(lambda x: (x[0], 0))
    rdd_no_tarjeta = rdd_no_tarjeta.subtractByKey(rdd_mayor_1500)
    rdd_no_tarjeta = rdd_no_tarjeta.subtractByKey(rdd_menor_1500)
    rdd_no_tarjeta = rdd_no_tarjeta.distinct()

    # Contar las compras por persona y utilizar combiner
    rdd_mayor_1500 = rdd_mayor_1500.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
    rdd_menor_1500 = rdd_menor_1500.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)

    # Juntamos las personas que no realizaron compras con tarjeta en ambas categorías
    rdd_mayor_1500 = rdd_mayor_1500.union(rdd_no_tarjeta)
    rdd_menor_1500 = rdd_menor_1500.union(rdd_no_tarjeta)

    # Formatear resultado como cadena
    rdd_mayor_1500 = rdd_mayor_1500.map(lambda x: "{};{}".format(x[0], x[1]))
    rdd_menor_1500 = rdd_menor_1500.map(lambda x: "{};{}".format(x[0], x[1]))

    # Guardar los resultados en los archivos comprasCreditoMayorDe1500 y comprasCreditoMenorDe1500
    rdd_mayor_1500.saveAsTextFile(output_file_mayor_1500)
    rdd_menor_1500.saveAsTextFile(output_file_menor_1500)

if __name__ == "__main__":
    if len(sys.argv) !=4:
        print("""Debes ingresar los parametros:
                dataset.txt
                pathcomprasCreditoMayor1500
                pathcomprasCreditoMenor1500""")
        exit()
    dataset_path = str(sys.argv[1])
    pathcomprasCreditoMayor1500 = str(sys.argv[2])
    pathcomprasCreditoMenor1500 = str(sys.argv[3])
    # uso de la función
    personaYMetodosDePago(dataset_path, pathcomprasCreditoMayor1500, pathcomprasCreditoMenor1500)

