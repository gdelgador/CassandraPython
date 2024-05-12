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

    def __str__(self):
        return f'Usuario(DNI: {self.DNI}, Nombre: {self.nombre}, Calle: {self.calle}, Ciudad: {self.ciudad})'


class Ejemplar:
    """Clase Ejemplar"""
    def __init__(self, nro, status):
        self.nro = nro
        self.status = status

    def __str__(self):
        return f'Ejemplar(Número: {self.nro}, Estado: {self.status})'

class UsuarioEjemplar(Usuario):
    """Clase UsuarioEjemplar"""
    def __init__(self, DNI, nombre, calle, ciudad, nro, fecha):
        super().__init__(DNI, nombre, calle, ciudad)
        self.Ejemplar_nro = nro
        self.fecha = fecha

    def __str__(self):
        return f'UsuarioEjemplar({super().__str__()}, Ejemplar Número: {self.Ejemplar_nro}, Fecha: {self.fecha})'


class Libro:
    """Clase Libro"""
    def __init__(self, ISBN, titulo, anio, temas):
        self.ISBN = ISBN
        self.titulo = titulo
        self.anio = anio
        self.temas = temas

    def __str__(self):
        return f'Libro(ISBN: {self.ISBN}, Título: {self.titulo}, Año: {self.anio}, Temas: {self.temas})'

class Autor:
    """Clase Autor"""
    def __init__(self, cod, nombre, premios):
        self.cod = cod
        self.nombre = nombre
        self.premios = premios
        pass

class AutorLibro:
    """Clase AutorLibro"""
    def __init__(self, Autor_cod, ISBN):
        self.Autor_cod = Autor_cod
        self.ISBN = ISBN

    def __str__(self):
        return f'AutorLibro(Autor_cod: {self.Autor_cod}, ISBN: {self.ISBN})'

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
    """Método de inserción a todas las tablas para relación es_prestado"""
    
    # solicito datos a usuario
    usuario_dni = input("Ingrese el dni del usuario: ").strip().upper()
    ejemplar_nro = get_int("Ingrese el nro de ejemplar: ")
    ejemplar_status = get_int("Ingrese el status de ejemplar: ")
    fecha = get_date("Ingrese la fecha de prestamo en formato YYYY-MM-DD: ")
    
    # traigo datos de tabla soporte
    user = extraer_data_tabla_soporte_usuario(dni=usuario_dni)
    
    if not user:
        print('No se encontro user, primero se debe insertar opcion 2')
        return None
    
    # Insetamos a tabla8 correspondiente
    insertTabla8 = session.prepare(
        "INSERT INTO tabla8(fecha, ejemplar_nro, usuario_dni, ejemplar_status, usuario_nombre, usuario_ciudad, usuario_calle) VALUES(?, ?, ?, ?, ?, ?, ?)"
        )
    session.execute(insertTabla8, [fecha, ejemplar_nro, user.DNI, ejemplar_status, user.nombre, user.ciudad, user.calle])
    
    # insertamos sobre tabla 6
    updateTabla6 = session.prepare(
        "UPDATE tabla6 SET prestamos = prestamos + 1 WHERE ejemplar_nro = ? and usuario_dni = ?"
        )
    session.execute(updateTabla6, [ejemplar_nro, user.DNI])
    
    pass


def insert_corresponde_es_prestado():
    """Método de inserción a todas las tablas para relación corresponde_es_prestado"""
    
    libro_isbn = input("Ingrese el autor cod: ").strip().upper()
    usuario_dni = input("Ingrese el dni del usuario: ").strip().upper()
    
    ejemplar_nro = get_int("Ingrese el nro de ejemplar: ")
    fecha = get_date("Ingrese la fecha de prestamo en formato YYYY-MM-DD: ")
    
    # VALIDAR
    ejemplar_status = input("Ingrese el status del ejemplar: ").strip().upper()
    
    # traigo datos de tabla soporte
    user = extraer_data_tabla_soporte_usuario(dni=usuario_dni)
    libro = extraer_data_tabla_soporte_libro(libro_isbn=libro_isbn)
    
    if not user:
        print('No se encontro user, primero se debe insertar opcion 2')
        return None
    
    if not libro:
        print('No se encontro libro, primero se debe insertar opcion 1')
        return None
    
    # Insetamos a tabla2 correspondiente
    
    insertTabla2 = session.prepare(
        "INSERT INTO tabla2(libro_titulo, libro_isbn, ejemplar_nro, ejemplar_status) VALUES(?, ?, ?, ?)"
        )
    session.execute(insertTabla2, [libro.titulo, libro.ISBN, ejemplar_nro, ejemplar_status])
    
    # Insetamos a tabla4 correspondiente
    insertTabla4 = session.prepare(
        "INSERT INTO tabla4(libro_titulo,usuario_dni,libro_isbn,ejemplar_nro,usuario_nombre,usuario_ciudad,usuario_calle,) VALUES(?, ?, ?, ?, ?,?,?)"
        )
    session.execute(insertTabla4, [libro.titulo, user.DNI, libro.ISBN, ejemplar_nro, user.nombre, user.ciudad, user.calle])
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
    
    # actualizamos en tabla soporte
    updateAnioLibroSoporte = session.prepare(
        "UPDATE soportelibro SET libro_anio = ? WHERE libro_isbn = ?"
                                             )
    session.execute(updateAnioLibroSoporte, [anio, libro.ISBN])

    # Insertamos sobre tabla 1
    deleteTabla1 = session.prepare(
        "DELETE FROM tabla1 WHERE libro_anio= ? AND libro_isbn = ?"
    )
    session.execute(deleteTabla1, [libro.anio, libro.ISBN])

    insertStatement = session.prepare(
        "INSERT INTO tabla1(libro_anio, libro_isbn,libro_titulo, libro_temas) VALUES (?, ?, ?, ?)")
    session.execute(insertStatement, [anio, libro.ISBN, libro.titulo, libro.temas])

    print(f'Se actualizaron tablas libro')
    libro_actualizado = extraer_data_tabla_soporte_libro(libro_isbn=isbn)
    print(libro_actualizado)
    pass


def eliminar_autor_por_premio():
    """Elimina Autores de tabla de acuerdo a su premio"""
    
    premio = input("Ingrese el premio a buscar: ").strip().upper()

    if not premio:
        print('No se ingreso valor premio ...')
        return None

    listado_cod_autores = extraer_autores_por_premio(premio=premio)

    if not listado_cod_autores:
        print('No se retornaron valores de tabla ...')
        return None

    # Borramos datos de tabla
    deleteTabla7 = session.prepare(
        "DELETE FROM tabla7 WHERE premios_premio= ? AND autor_cod in ?"
    )
    session.execute(deleteTabla7, [premio, listado_cod_autores])
    
    deleteSoporteAutor = session.prepare("DELETE FROM SoporteAutor autor_cod IN ?")
    session.execute(deleteSoporteAutor, [listado_cod_autores,])
    
    print(f'Se eliminaron autores cod [{listado_cod_autores}] de tabla ')
    pass

def extraer_autores_por_premio(premio:str):
    """Extrae el cod_autor de la tabla 7 según el premio ingresado"""

    select = session.prepare("SELECT autor_cod FROM tabla7 WHERE premios_premio = ?")
    filas = session.execute(select, [premio, ])
    # a lista
    datos_retornar = [fila.autor_cod for fila in filas]
    return datos_retornar if datos_retornar else None

def extraer_data_tabla_soporte_libro(libro_isbn:str):
    """Extrae información de tabla SoporteLibro de acuerdo a libro_isbn"""
    select = session.prepare("SELECT * FROM SoporteLibro WHERE libro_isbn = ?")
    # retornamos valores
    filas = session.execute(select, [libro_isbn, ])

    # solo debe haber un registro
    datos_retornar = [Libro(fila.libro_isbn, fila.libro_titulo, fila.libro_anio, fila.libro_temas) for fila in filas]
    return datos_retornar[0] if datos_retornar else None

def extraer_data_tabla_soporte_usuario(dni:str):
    """Extrae información de tabla SoporteUsuario de acuerdo a usuario_dni"""
    select = session.prepare("SELECT * FROM SoporteUsuario WHERE usuario_dni = ?")
    # retornamos valores
    filas = session.execute(select, [dni, ])

    # solo debe haber un registro
    datos_retornar = [Usuario(fila.usuario_dni, fila.usuario_nombre, fila.usuario_ciudad, fila.usuario_calle) for fila in filas]
    return datos_retornar[0] if datos_retornar else None

def consultar_libros_segun_anio():
    """
    Obtener toda la información de libros publicados en un año en concreto.
    """
    anio = get_int('Ingrese el año de publicación a buscar: ')
    
    select = session.prepare('SELECT * FROM tabla1 WHERE libro_anio=?')
    filas = session.execute(select, [anio, ])
    
    datos_retornar = [Libro(ISBN=fila.libro_isbn,
                            titulo=fila.libro_titulo,
                            anio=fila.libro_anio,
                            temas=fila.libro_temas) for fila in filas]
    
    if not datos_retornar:
        print('No hay datos que retornar ..')
        return None
    
    for libro in datos_retornar:
        print(libro)


def consultar_libros_titulo():
    """Obtener toda la información de los ejemplares de un libro según el título de este."""
    titulo = input("Ingrese el título del libro: ").strip().upper()

    select = session.prepare("SELECT * FROM tabla2 WHERE libro_titulo = ?")
    filas = session.execute(select, [titulo, ])

    datos_retornar = [(fila.libro_titulo,  fila.libro_isbn, fila.ejemplar_nro, fila.ejemplar_status) for fila in filas]
    
    if not datos_retornar:
        print("No se encontraron ejemplares para el libro ingresado.")
        return None
    
    for item in datos_retornar:
        print('='*50)
        print(f'Libro titulo: {item[0]}')
        print(f'Libro isbn: {item[1]}')
        print(f'Ejemplar nro: {item[2]}')
        print(f'Ejemplar status: {item[3]}')
    pass

def consultar_usuario_segun_libro():
    """Obtener los usuarios que han tomado prestado el ejemplar de un libro según el título de un libro"""
    titulo_libro = input("Ingrese el título del libro: ").strip().upper()

    select = session.prepare("SELECT * FROM tabla4 WHERE libro_titulo = ?")
    filas = session.execute(select, [titulo_libro, ])

    datos_retornar = [(fila.libro_titulo, fila.usuario_dni, fila.libro_isbn, fila.ejemplar_nro, fila.usuario_nombre, fila.usuario_ciudad, fila.usuario_calle) for fila in filas]

    if not datos_retornar:
        print("No se encontraron usuarios para el libro especificado.")
        return None
    
    for item in datos_retornar:
        print('='*50)
        print(f"""
              Libro: {item[0]},
              Usuario DNI: {item[1]}, 
              Libro ISBN: {item[2]}, 
              Ejemplar Nro: {item[3]},
              Usuario Nombre: {item[4]},
              Usuario Ciudad: {item[5]},
              Usuario Calle: {item[6]}
              """)

    pass

def consultar_usuario_por_ciudad():
    """Obtener información de usuarios según ciudad"""
    ciudad = input("Ingrese la ciudad: ").strip().upper()

    select = session.prepare("SELECT * FROM tabla5 WHERE usuario_ciudad = ?")
    filas = session.execute(select, [ciudad, ])

    datos_retornar = [(fila.usuario_ciudad,fila.usuario_calle,fila.usuario_nombre,fila.usuario_dni) for fila in filas]

    if not datos_retornar:
        print("No se encontraron usuarios en la ciudad especificada.")
        return None
    
    for item in datos_retornar:
        print('='*50)
        print(f"""
              Ciudad: {item[0]},
              Calle: {item[1]}, 
              Nombre: {item[2]}, 
              DNI: {item[3]}
              """)

    pass

def consultar_autores_por_premio():
    """Obtener la información de los autores que hayan ganado un premio específico"""
    
    premio = input('Ingrese el premio a buscar: ').strip().upper()
    
    select  = session.prepare("SELECT * FROM tabla7 WHERE premios_premio = ?")
    filas = session.execute(select, [premio, ])
    
    datos_retornar = [(fila.premios_premio,fila.autor_cod,fila.autor_nombre) for fila in filas]
    
    if not datos_retornar:
        print("No se encontraron datos para el premio ingresado.")
        return None
    
    for item in datos_retornar:
        print('='*50)
        print(f"""
              Premio: {item[0]},
              Cod Autor: {item[1]}, 
              Autor Nombre: {item[2]}
              """)
    
    
    pass


def consultar_segun_fecha_usuario_ejemplar():
    """Buscar según la fecha de préstamo los ejemplares prestados y el usuario que lo tomó prestado."""
    
    fecha = get_date("Ingrese la fecha de prestamo en formato YYYY-MM-DD: ")
    
    select  = session.prepare("SELECT * FROM tabla8 WHERE fecha = ?")
    filas = session.execute(select, [fecha, ])
    datos_retornar = [UsuarioEjemplar(DNI=fila.usuario_dni,
                                      nombre=fila.usuario_nombre,
                                      calle=fila.usuario_calle,
                                      ciudad=fila.usuario_ciudad,
                                      nro=fila.ejemplar_nro,
                                      fecha=fila.fecha 
                                      )
                      for fila in filas]
    
    
    if not datos_retornar:
        print("No se encontraron para la fecha ingresada.")
        return None
    
    for item in datos_retornar:
        print('='*50)
        print(item)
    pass

#Programa principal
#Conexión con Cassandra
OPCIONES_MENU = """
Introduzca un número para ejecutar una de las siguientes operaciones:
1. Insertar un Libro
2. Insertar un Usuario
3. Insertar Premio Autor
4. Insertar relacion es_prestado
5. Insertar relacion prestamo_es_prestado
6. Actualizar anio publicación libro
7. Eliminar autores según premio
8. Obtener información de los libros publicados en un año en concreto
9. Obtener toda la información de los ejemplares de un libro según el título de este.
10. Obtener los usuarios que han tomado prestado el ejemplar de un libro según el título de un libro.
11. Obtener información de usuarios según ciudad
12. Obtener la información de los autores que hayan ganado un premio específico
13. Buscar según la fecha de préstamo los ejemplares prestados y el usuario que lo tomó prestado.

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
        
        # relacion prestamos 
        elif respuesta == '4':
            insert_es_prestado()
        elif respuesta == '5':
            insert_corresponde_es_prestado()
            
        # actualizacion eliminado de datos
        elif respuesta == '6':
            actualizar_anio_publicacion_libro()
        elif respuesta == '7':
            eliminar_autor_por_premio()
        
        # consultas de seleccion
        elif respuesta == '8':
            consultar_libros_segun_anio()
        elif respuesta == '9':
            consultar_libros_titulo()
        elif respuesta == '10':
            consultar_usuario_segun_libro()
        elif respuesta == '11':
            consultar_usuario_por_ciudad()
        elif respuesta == '12':
            consultar_autores_por_premio()
        elif respuesta == '13':
            consultar_segun_fecha_usuario_ejemplar()
            
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
