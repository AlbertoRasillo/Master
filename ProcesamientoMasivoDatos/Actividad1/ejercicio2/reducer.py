#!/usr/bin/python3

import sys
from collections import OrderedDict
# Función del reducer

# Creamos un diccionario para almacenar la lista de citing para cada cited
citations = {}

# Recorremos las líneas de la entrada estándar
for line in sys.stdin:
  # Separamos la línea en dos partes: cited y citing
  cited, citing = line.strip().split()

  # Si no existe una entrada para el cited en el diccionario, la creamos
  if cited not in citations:
    citations[cited] = []
  # Añadimos el citing a la lista del cited
  citations[cited].append(citing)

# Ordena el diccionario por valor
sorted_citations = sorted(citations, key=lambda x: x)

# Crea un nuevo diccionario ordenado a partir de la lista ordenada
ordered_citations = OrderedDict((key, citations[key]) for key in sorted_citations)

# Recorremos el diccionario de citaciones
for cited, citing_list in ordered_citations.items():
  # Ordenamos la lista de citing numericamente
  citing_list.sort()
  # Mostramos el resultado con el formato solicitado
  print(f"{cited} {','.join(citing_list)}")
