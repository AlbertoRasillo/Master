#!/usr/bin/python3

import sys

# Mapper

# Leemos la entrada desde sys.stdin
for line in sys.stdin:
    # Separamos el nombre y el gasto
    name, cost = line.strip().split()

    # Emitimos una clave-valor pair
    print(f"{name}\t{cost}")
