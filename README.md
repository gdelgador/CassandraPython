# CassandraPython
Desarrollo Pc2 curso Sistemas de Almacenamiento y Gestión Big Data - Viu 



comando carga dsbulk

hacer cd hacia carpeta bin dsbulk

- en windowns colocar

.\dsbulk.cmd load -url "C:\\Users\\Gonzalo\\Desktop\\repos\\CassandraPython\\data\\calidadAire.csv" -k gonzalodelgado -t aire --schema.mapping "Estacion=estacion, Titulo=titulo, latitud=latitud, longitud=longitud, Fecha=fecha, Periodo=periodo, SO2=so2, NO=no, NO2=no2, CO=co, PM10=pm10, O3=o3, dd=dd, vv=vv, TMP=tmp, HR=hr, PRB=prb, RS=rs, LL=ll, BEN=ben, TOL=tol, MXIL=mxil, PM25=pm25"