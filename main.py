#Importación de librerias necesarias para conexión con Cassandra y gestión de fechas

from cassandra.cluster import Cluster
from datetime import date, datetime


class Usuario:
    """Clase Usuario"""
    def __init__(self, DNI, nombre, calle, ciudad):
        self.DNI = DNI
        self.nombre = nombre
        self.calle = calle
        self.ciudad = ciudad
        pass

class Ejemplar:
    """Clase Ejemplar"""
    def __init__(self, nro, status):
        self.nro = nro
        self.status = status
        pass


class UsuarioEjemplar:
    """Clase UsuarioEjemplar"""
    def __init__(self, DNI, nro, fecha):
        self.Usuario_DNI = DNI
        self.Ejemplar_nro = nro
        self.fecha = fecha
        pass


class Libro:
    """Clase Libro"""
    def __init__(self, ISBN, titulo, anio, temas):
        self.ISBN = ISBN
        self.titulo = titulo
        self.anio = anio
        self.temas = temas
        pass

    def __str__(self):
        return f"""
           libro: {self.ISBN}
           titulo: {self.titulo}
           anio: {self.anio}
           temas: {self.temas}
           """


class Autor:

    def __init__(self, cod, nombre, premios):
        self.cod = cod
        self.nombre = nombre
        self.premios = premios
        pass

class AutorLibro:
    """Clase AutorLibro"""
    def __init__(self, Autor_cod, ISBN) -> None:
        self.Autor_cod = Autor_cod
        self.ISBN = ISBN
        pass

class Editorial:
    """Clase Editorial"""
    def __init__(self, cod, nombre, direccion):
        self.cod = cod
        self.nombre = nombre
        self.direccion = direccion
        pass

class Pais:
    """Clase Pais"""
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

def get_date(msg: str) ->date:
    """Solicita una fecha a partir de un input"""
    try:
        fecha_str = input(msg)
        return datetime.strptime(fecha_str, '%Y-%m-%d').date()
    except:
        print('Dato invalido, vuelva a intentar ...')
        return get_date(msg)
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
    dni = input("Ingrese su dni: ").strip().upper()
    nombre = input("Ingrese su nombre: ").strip().upper()
    ciudad = input("Ingrese la ciudad: ").strip().upper()
    calle = input("Ingrese la calle: ").strip().upper()

    # instancia de clase Usuario
    user = Usuario(dni, nombre, calle, ciudad)

    # insertamos a tablas
    insertUsuario = session.prepare(
        "INSERT INTO tabla5(usuario_ciudad, usuario_calle, usuario_nombre, usuario_dni) VALUES(?, ?, ?, ?)")
    session.execute(insertUsuario, [user.ciudad, user.calle, user.nombre, user.DNI])

    insertSoporte = session.prepare(
        "INSERT INTO SoporteUsuario(usuario_dni, usuario_nombre, usuario_ciudad, usuario_calle) VALUES(?, ?, ?, ?)")
    session.execute(insertSoporte, [user.DNI, user.nombre, user.ciudad, user.calle])
    pass


def insertTabla7():
    """Método de inserción a tabla7"""
    autor_cod = get_int("Ingrese el autor cod: ")
    autor_nombre = input("Ingrese el autor nombre: ").strip().upper()
    premios = set()
    premio = input("Ingrese el premio, vacio para parar:: ").strip().upper()

    while premio != '':
        premios.add(premio)
        premio = input("Ingrese el premio, vacio para parar: ").strip().upper()

    # instancia de clase Autor
    autor = Autor(autor_cod, autor_nombre, premios)
    # insertamos a Tablas
    insertAutor = session.prepare("INSERT INTO SoporteAutor(autor_cod, autor_nombre, autor_premios) VALUES(?, ?, ?)")
    session.execute(insertAutor, [autor.cod, autor.nombre, autor.premios])

    insertPremio = session.prepare("INSERT INTO tabla7(premios_premio, autor_cod, autor_nombre) VALUES(?, ?, ?)")
    for autor_premio in autor.premios:
        session.execute(insertPremio, [autor_premio, autor.cod, autor.nombre])
    pass

def insert_es_prestado():
    """Método de inserción para relación es_prestado"""
    
    autor_cod = get_int("Ingrese el autor cod: ")
    ejemplar_nro = get_int("Ingrese el nro de ejemplar: ")
    fecha = get_date("Ingrese la fecha de prestamo en formato YYYY-MM-DD: ")

    
    pass

def actualizar_anio_publicacion_libro():
    """Actualiza anio de publicacion de un libro en base a su ISBN"""

    isbn = input("Ingrese el isbn: ").strip().upper()
    anio = get_int("Ingrese el anio publicación libro a actualizar: ")

    # Consultamos existencia de libro en tabla soporte
    libro = extraer_data_tabla_soporte_libro(libro_isbn=isbn)
    if not libro:
        print('Libro no encontrado, inserte valor en opción 1')
        return None
    # actualizamos valor anio
    libro.anio = anio
    
    # actualizamos en tabla soporte
    updateAnioLibroSoporte = session.prepare(
        "UPDATE soportelibro SET libro_anio = ? WHERE libro_isbn = ?"
                                             )
    session.execute(updateAnioLibroSoporte, [libro.anio, libro.ISBN])

    # Insertamos sobre tabla 1
    deleteTabla1 = session.prepare(
        "DELETE FROM tabla1 WHERE libro_anio= ? AND libro_isbn = ?"
    )
    session.execute(deleteTabla1, [libro.anio, libro.ISBN])

    insertStatement = session.prepare(
        "INSERT INTO tabla1(libro_anio, libro_isbn,libro_titulo, libro_temas) VALUES (?, ?, ?, ?)")
    session.execute(insertStatement, [libro.anio, libro.ISBN, libro.titulo, libro.temas])

    print(f'Se actualizaron tablas para isbn {libro.ISBN}')
    pass


def extraer_data_tabla_soporte_libro(libro_isbn:str):
    """Extrae información de tabla SoporteLibro de acuerdo a libro_isbn"""
    select = session.prepare("SELECT * FROM SoporteLibro WHERE libro_isbn = ?")
    # retornamos valores
    filas = session.execute(select, [libro_isbn, ])

    # solo debe haber un registro
    datos_retornar = [Libro(fila.libro_isbn, fila.libro_titulo, fila.libro_anio, fila.libro_temas) for fila in filas]
    return datos_retornar[0] if datos_retornar else None

#Programa principal
#Conexión con Cassandra
OPCIONES_MENU = """
Introduzca un número para ejecutar una de las siguientes operaciones:
1. Insertar un Libro
2. Insertar un Usuario
3. Insertar Premio Autor
4. Insertar relacion es_prestado
5. Actualizar anio publicación libro

0. Cerrar aplicación
Ingrese su opcion: """


try:
    cluster = Cluster()
    session = cluster.connect('gonzalodelgado')

    # Sigue pidiendo operaciones hasta que se introduzca 0
    while True:
        respuesta = input(OPCIONES_MENU)
        if respuesta == '1':
            insertTabla1()
            pass
        elif respuesta == '2':
            insertTabla5()
        elif respuesta == '3':
            insertTabla7()
        elif respuesta == '4':
            insert_es_prestado()
        elif respuesta == '5':
            actualizar_anio_publicacion_libro()
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
