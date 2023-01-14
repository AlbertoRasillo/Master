#!/usr/bin/python3

import sys
# reducer
# Diccionario para almacenar la frecuencia de cada valor para cada clave
frecuencia = {}

# Recorre el stdin
for line in sys.stdin:
    # Separa la clave y el valor
    key, value = line.strip().split()

    # Aumenta en uno la frecuencia del valor para la clave
    if key in frecuencia:
        if value in frecuencia[key]:
            frecuencia[key][value] += 1
        else:
            frecuencia[key][value] = 1
    else:
        frecuencia[key] = {value: 1}

# Recorre el diccionario de frecuencias
for key in frecuencia:
    # Encuentra el valor con mayor frecuencia para la clave
    moda = max(frecuencia[key], key=frecuencia[key].get)

    # Muestra la moda para la clave
    print(f"{key}\t{moda}")


