"""This script initializes plumber service."""

#########################################################
# Setting up the  context
#########################################################

#########################################################
# Imports
#########################################################
import django
from django.db import connection
from django.db.utils import OperationalError
import time


django.setup()

#########################################################
# 1. Waiting for PostgreSQL
#########################################################

print('-----------------------------------------------------')
print('1. Waiting for PostgreSQL')
for _ in range(60):
    try:
        connection.ensure_connection()
        break
    except OperationalError:
        time.sleep(1)
else:
    connection.ensure_connection()
connection.close()

print('-----------------------------------------------------')
print('2. Generate plumber.R file')
from spw.utils.plumber import (  # noqa
    spawn_r_plumber,
    write_plumber_file
)
write_plumber_file()

print('-----------------------------------------------------')
print('3. Spawn initial plumber process')
ports = [8282]
for index, port in enumerate(ports):
    print(f'Spawn plumber {index + 1} with port {port}')
    plumber_process = spawn_r_plumber(index=index + 1, port=port)
    if plumber_process:
        print(f'plumber process pid {plumber_process.pid}')
    else:
        raise RuntimeError('Cannot execute plumber process!')
