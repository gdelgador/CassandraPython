CREATE KEYSPACE IF NOT EXISTS gonzalodelgado
WITH durable_writes = true
AND replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};

-- tabla1
DROP TABLE IF EXISTS gonzalodelgado.tabla1;
CREATE TABLE IF NOT EXISTS gonzalodelgado.tabla1 (
	libro_anio int,
	libro_isbn text,
	libro_titulo text,
	libro_temas SET<text>,
	PRIMARY KEY (libro_anio, libro_isbn)
) WITH CLUSTERING ORDER BY ( libro_isbn ASC );

-- tabla2
DROP TABLE IF EXISTS gonzalodelgado.tabla2;
CREATE TABLE IF NOT EXISTS gonzalodelgado.tabla2 (
	libro_titulo text,
	libro_isbn text,
	ejemplar_nro int,
	ejemplar_status text,
	PRIMARY KEY (libro_titulo, libro_isbn, ejemplar_nro)
) WITH CLUSTERING ORDER BY ( libro_isbn ASC, ejemplar_nro ASC );

-- tabla3
DROP TABLE IF EXISTS gonzalodelgado.tabla3;
CREATE TABLE IF NOT EXISTS gonzalodelgado.tabla3 (
	autor_nombre text,
	autor_cod int,
	libro_isbn text,
	libro_titulo int,
	libro_anio int,
	libro_temas SET<text>,
	PRIMARY KEY (autor_nombre, autor_cod, libro_isbn)
) WITH CLUSTERING ORDER BY ( autor_cod ASC,libro_isbn ASC );


--tabla 4
DROP TABLE IF EXISTS gonzalodelgado.tabla4;
CREATE TABLE IF NOT EXISTS gonzalodelgado.tabla4(
	libro_titulo text,
	usuario_dni text,
	libro_isbn text,
	ejemplar_nro int,
	usuario_nombre text,
	usuario_ciudad text,
	usuario_calle text,
	PRIMARY KEY (libro_titulo, usuario_dni, libro_isbn, ejemplar_nro)
) WITH CLUSTERING ORDER BY (usuario_dni ASC,libro_isbn ASC, ejemplar_nro ASC );

-- tabla5
DROP TABLE IF EXISTS gonzalodelgado.tabla5;
CREATE TABLE IF NOT EXISTS gonzalodelgado.tabla5(
	-- validar optimizacion
	usuario_ciudad text,
	usuario_calle text,
	usuario_nombre text,
	usuario_dni text,
	PRIMARY KEY (usuario_ciudad, usuario_dni)
) WITH CLUSTERING ORDER BY ( usuario_dni ASC);


-- tabla6
DROP TABLE IF EXISTS gonzalodelgado.tabla6;
CREATE TABLE IF NOT EXISTS gonzalodelgado.tabla6(
	ejemplar_nro int,
	usuario_dni text,
	cantidad counter,
	PRIMARY KEY (ejemplar_nro, usuario_dni)
) WITH CLUSTERING ORDER BY ( usuario_dni ASC);

-- tabla7
DROP TABLE IF EXISTS gonzalodelgado.tabla7;
CREATE TABLE IF NOT EXISTS gonzalodelgado.tabla7(
	premios_premio text,
	autor_cod int,
	autor_nombre text,
	PRIMARY KEY (premios_premio, autor_cod)
) WITH CLUSTERING ORDER BY ( autor_cod ASC);

-- tabla8
DROP TABLE IF EXISTS gonzalodelgado.tabla8;
CREATE TABLE IF NOT EXISTS gonzalodelgado.tabla8(
	fecha date,
	ejemplar_nro int,
	usuario_dni text,
	ejemplar_status text,
	usuario_nombre text,
	usuario_ciudad text,
	usuario_calle text,
	PRIMARY KEY (fecha, ejemplar_nro, usuario_dni)
) WITH CLUSTERING ORDER BY ( ejemplar_nro ASC, usuario_dni ASC);



--- creacion de tablas soporte
DROP TABLE IF EXISTS gonzalodelgado.SoporteLibro;
CREATE TABLE IF NOT EXISTS gonzalodelgado.SoporteLibro(
	libro_isbn text,
	libro_titulo text,
	libro_anio int,
	libro_temas SET<text>,
	PRIMARY KEY (libro_isbn)
);

DROP TABLE IF EXISTS gonzalodelgado.SoporteUsuario;
CREATE TABLE IF NOT EXISTS gonzalodelgado.SoporteUsuario(
	usuario_dni text,
	usuario_nombre text,
	usuario_ciudad text,
	usuario_calle text,
	PRIMARY KEY (usuario_dni)
);

DROP TABLE IF EXISTS gonzalodelgado.SoporteAutor;
CREATE TABLE IF NOT EXISTS gonzalodelgado.SoporteAutor(
	Autor_cod int,
	Autor_nombre text,
	Autor_premios SET<text>,
	PRIMARY KEY (Autor_cod)
);



--- ejercicio 3
DROP TABLE IF EXISTS gonzalodelgado.aire;
CREATE TABLE  IF NOT EXISTS gonzalodelgado.aire(
	estacion text,
	titulo text ,
	latitud text,
	longitud text,
	fecha text,
	periodo text,
	so2 text,
	no text,
	no2 text,
	co text,
	pm10 text,
	o3 text,
	dd text, 
	vv text, 
	tmp text, 
	hr text,
	prb text, 
	rs text, 
	ll text, 
	ben text, 
	tol text, 
	mxil text, 
	pm25 text,
	primary key (estacion, fecha, periodo)
);
