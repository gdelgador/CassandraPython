#Importación de librerias necesarias para conexión con Cassandra y gestión de fechas
import datetime

from cassandra.cluster import Cluster
from datetime import date

class Usuario:

    def __init__(self, DNI, nombre, calle, ciudad):
        self.DNI = DNI
        self.nombre = nombre
        self.calle = calle
        self.ciudad = ciudad
        pass

class Ejemplar:

    def __init__(self, nro, status):
        self.nro = nro
        self.status = status
        pass

class Libro:

    def __init__(self, ISBN, titulo, anio, temas):
        
        self.ISBN = ISBN
        self.titulo = titulo
        self.anio = anio
        self.temas = temas
        pass

class Autor:

    def __init__(self, cod, nombre, premios):
        self.cod = cod
        self.nombre = nombre
        self.premios = premios
        pass

class Editorial:

    def __init__(self, cod, nombre, direccion):
        self.cod = cod
        self.nombre = nombre
        self.direccion = direccion
        pass

class Pais:

    def __init__(self, cod, nombre):
        self.cod = cod
        self.nombre = nombre
        pass