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


class UsuarioEjemplar:

    def __init__(self, DNI, nro, fecha):
        self.Usuario_DNI = DNI
        self.Ejemplar_nro = nro
        self.fecha = fecha
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


class AutorLibro:

    def __init__(self, Autor_cod, ISBN) -> None:
        self.Autor_cod = Autor_cod
        self.ISBN = ISBN
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


def get_int(msg: str) -> int:
    """Solicita un número entero a usuario"""
    try:
        return int(input(msg))
    except:
        print("Dato invalido, vuelva a intentar...")
        return get_int(msg)
    pass


# Creacion de funciones para la insercion de datos
def insertTabla1():
    """Método de inserción a tabla1"""
    # solicitamos datos a usuario
    anio = get_int("Ingrese el anio: ")
    isbn = input("Ingrese el isbn: ").strip().upper()
    titulo = input("Ingrese el titulo: ").strip().upper()
    temas = set()
    tema = input("Introduzca un tema, vacio para parar: ").strip().upper()

    while tema != '':
        temas.add(tema)
        tema = input("Introduzca un tema, vacio para parar: ").strip().upper()

    # instancia de clase Libro
    libro = Libro(isbn, titulo, anio, temas)

    # insertamos a tablas
    insertStatement = session.prepare(
        "INSERT INTO tabla1(libro_anio, libro_isbn,libro_titulo, libro_temas) VALUES (?, ?, ?, ?)")
    session.execute(insertStatement, [libro.anio, libro.ISBN, libro.titulo, libro.temas])

    insertSoporte = session.prepare(
        "INSERT INTO SoporteLibro(libro_isbn, libro_titulo, libro_anio, libro_temas) VALUES (?, ?, ?, ?)")
    session.execute(insertSoporte, [libro.ISBN, libro.titulo, libro.anio, libro.temas])
    pass


def insertTabla5():
    """Método de inserción a tabla5"""
    # solicitamos dato a usuario
    ciudad = input("Ingrese la ciudad: ").strip().upper()
    calle = input("Ingrese la calle: ").strip().upper()
    nombre = input("Ingrese su nombre: ").strip().upper()
    dni = input("Ingrese su dni: ").strip().upper()

    # instancia de clase Usuario
    user = Usuario(dni, nombre, calle, ciudad)

    # insertamos a tablas
    insertUsuario = session.prepare(
        "INSERT INTO tabla5(usuario_ciudad, usuario_calle, usuario_nombre, usuario_dni) VALUES(?, ?, ?, ?)")
    session.execute(insertUsuario, [ciudad, calle, nombre, dni])

    insertSoporte = session.prepare(
        "INSERT INTO SoporteUsuario(usuario_dni, usuario_nombre, usuario_ciudad, usuario_calle) VALUES(?, ?, ?, ?)")
    session.execute(insertSoporte, [user.DNI, nombre, ciudad, calle])
    pass


def insertTabla7():
    """Método de inserción a tabla7"""

    autor_cod = get_int("Ingrese el autor cod: ")
    autor_nombre = input("Ingrese el autor nombre: ").strip().upper()
    premios = set()
    premio = input("Ingrese el premio: ").strip().upper()

    while premio != '':
        premios.add(premio)
        premio = input("Ingrese el premio: ").strip().upper()

    # instancia de clase Autor
    autor = Autor(autor_cod, autor_nombre, premios)

    # insertamos a Tablas
    #insertAutor = session.prepare("INSERT INTO ")

    insertPremio = session.prepare("INSERT INTO tabla7(premios_premio, autor_cod, autor_nombre) VALUES(?, ?, ?)")
    for premio in premios:
        session.execute(insertPremio, [premio, autor.cod, autor.nombre])
    pass


#Programa principal
#Conexión con Cassandra
OPCIONES_MENU = """
Introduzca un número para ejecutar una de las siguientes operaciones:
1. 

0. Cerrar aplicación
Ingrese su opcion: """


try:
    cluster = Cluster()
    session = cluster.connect('gonzalodelgado')

    # Sigue pidiendo operaciones hasta que se introduzca 0
    while True:
        respuesta = input(OPCIONES_MENU)
        if respuesta == '1':
            pass
        elif respuesta == '0':
            break
        else:
            print('Comando desconocido, vuelva a ingresar')
        pass

except Exception as e:
    print(e)
finally:
    #cerramos conexion
    print('Finzalizando programa')
    cluster.shutdown()
