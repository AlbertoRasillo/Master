#!/usr/bin/python3

import sys

# Función del mapper

# Recorre las líneas de entrada
for line in sys.stdin:
    # Salta la primera línea (cabecera)
    if line.startswith('"CITING","CITED"'):
        continue

    # Separa los campos y invierte la patente citante y la patente citada
    citing, cited = line.strip().split(',')
    print(cited + '\t' + citing)



