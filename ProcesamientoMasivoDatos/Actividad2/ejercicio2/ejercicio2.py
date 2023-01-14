#!/usr/bin/python3.9

from pyspark import SparkContext
import sys

def CategoriaDeVideosMenosVista(rdd, result_path):
    # Mapear cada entrada a (categoria, visualizaciones)
    rdd = rdd.filter(lambda x: len(x.split("\t")) > 5)
    categoria_views = rdd.map(lambda x: (x.split("\t")[3], int(x.split("\t")[5])))
    # Reducir por clave (categoria) sumando las visualizaciones de cada vídeo
    categoria_views = categoria_views.reduceByKey(lambda x, y: x + y)
    # Obtener la entrada con menor valor
    categoria_menos_vista = categoria_views.min(lambda x: x[1])
    # Formatear resultado como cadena
    categoria_min_formatted = "{};{}".format(categoria_menos_vista[0], categoria_menos_vista[1])
    # Guardar el resultado en un archivo
    sc.parallelize([categoria_min_formatted]).saveAsTextFile(result_path)
    # Detener SparkContext
    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) !=3:
        print("Debes ingresar: pathEntrada pathSalida")
        exit()
    dataset_path = str(sys.argv[1])
    result_path = str(sys.argv[2])
    # Crear SparkContext
    sc = SparkContext("local", "CategoriaDeVideosMenosVista")
    # Leer dataset
    rdd = sc.wholeTextFiles(dataset_path)
    rdd = rdd.filter(lambda x: "log.txt" not in x[0]).flatMap(lambda x: x[1].split('\r\n')).filter(lambda x: x!='')
    # Ejecutar programa
    CategoriaDeVideosMenosVista(rdd, result_path)

