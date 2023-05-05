import mysql.connector as mysql
from mysql.connector import errorcode


# noinspection PyGlobalUndefined

# Funcion encarcada de conectarse mysql
def create_conexion(user: str, passwd: str, host: str, port: str):
    global cursor, cnx
    print("Intentando conectar con mysql: ", end='')
    try:
        cnx = mysql.connect(
            host=host,
            port=port,
            user=user,
            passwd=passwd
        )
        cursor = cnx.cursor()
    except mysql.Error() as err:
        print(err.msg)
    else:
        print("Conexion exitosa")

    try:
        print("Selecionando la base de datos {}: ".format(db_name), end='')
        cursor.execute("USE {}".format(db_name))
    except mysql.Error as err:
        if err.errno == 1049:
            print("La base de datos no existe")
    else:
        print("OK")


# Funcion para crear una base de datos
def create_db():
    try:
        print("Creando la base de datos {}: ".format(db_name), end='')
        cursor.execute(
            "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(db_name))
        cursor.execute("USE {}".format(db_name))
    except mysql.Error as err:
        if err == errorcode.ER_DB_CREATE_EXISTS:
            print("Ya existe")
        else:
            print("Error al crear la base de datos {}: ".format(err))
    else:
        print("OK")


# Funcion para elminar una base de datos
def drop_db():
    verificar = input(
        "Escribe S si estas seguro de eliminar esta base de datos {}: ".format(db_name))

    if verificar == "s" or verificar == "S":
        try:
            print("Borrando la bases de datos {}: ".format(db_name), end='')
            cursor.execute("DROP DATABASE {}".format(db_name))
        except mysql.Error as err:
            print(err.msg)
        else:
            print("OK")
    else:
        print("Abortando la operacion")


# Funcion para crear tablas
def create_table(tables):
    for table_name in tables:
        table_description = tables[table_name]
        try:
            print("Creando la tabla {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("la tabla ya existe")
            else:
                print(err.msg)
        else:
            print("OK")


# def drop_table():


# Funcion para insertar datos
def insert_data(command_sql, dataset):
    try:
        print("Intentando agregar datos: ", end='')
        cursor.execute(command_sql, dataset)
    except mysql.Error as err:
        print(err.msg)
    else:
        print('OK')
        cnx.commit()


# Funcion para mostra las tablas de la base de datos selecionada
def show_table():
    try:
        print("Mostrando tablas de la base de datos {}".format(db_name))
        cursor.execute("SHOW TABLES")
        for tables in cursor:
            print(tables)
    except mysql.Error as err:
        print(err)


# Funcion para mostrar los atributos de la tabla seleccionada
def show_atributes(name_table):
    try:
        print("Mostrando los atributos de la tabla {}".format(name_table))
        cursor.execute("DESCRIBE {}".format(name_table))
        for atributtes in cursor:
            print(atributtes)
    except mysql.Error as err:
        print(err.msg)


# Funcion para mostrar los datos de la tabla seleccionada
def show_info(db_table):
    try:
        print("Mostrando la informacion de la tabla {}".format(db_table))
        cursor.execute("SELECT * FROM {}".format(db_table))
        for info in cursor:
            print(info)
    except mysql.Error as err:
        print(err.msg)


# Test 1
def test():
    global db_name
    db_name = "clinicas_oftamologicas"
    # Conectando con mysql
    create_conexion("root", "0000", "localhost", "3306")
    drop_db()
    create_db()
    # Creacion de tablas
    tables = {"paciente": (
        """
        CREATE TABLE IF NOT EXISTS paciente(
            dni VARCHAR(12) NOT NULL,
            nombre VARCHAR(255) NOT NULL,
            fecha_nacimiento DATE NOT NULL,
            direccion VARCHAR(255) NOT NULL,
            CONSTRAINT pk_paciente PRIMARY KEY(dni)
        )
        """
    ), "tratamiento": ("""
        CREATE TABLE IF NOT EXISTS tratamiento(
            id_tratamiento INT(10) NOT NULL,
            nombre VARCHAR(70) NOT NULL,
            fecha_inicio DATE NOT NULL,
            CONSTRAINT pk_tratamiento PRIMARY KEY(id_tratamiento)
        )
    """), "medico": ("""
        CREATE TABLE IF NOT EXISTS medico(
            num_colegiado VARCHAR(9) NOT NULL,
            nombre VARCHAR(70) NOT NULL,
            fecha_nacimiento DATE NOT NULL,
            CONSTRAINT pk_tratamiento PRIMARY KEY(num_colegiado)
        )
    """), "periodo": ("""
        CREATE TABLE IF NOT EXISTS perido(
            fecha_inicio DATE NOT NULL,
            fecha_fin DATE NOT NULL,
            CONSTRAINT pk_tratamiento PRIMARY KEY(fecha_inicio, fecha_fin)
        )
    """

                      ), "clinica": ("""
        CREATE TABLE IF NOT EXISTS clinica(
            codigo_postal INT(8) NOT NULL,
            numero INT(4) NOT NULL,
            calle VARCHAR(4) NOT NULL,
            ciudad VARCHAR(100) NOT NULL,
            telefono VARCHAR(10) NOT NULL,
            CONSTRAINT pk_clinicas PRIMARY KEY(codigo_postal, numero, calle(4))
        )
    """
                                     ), "prueba": ("""
        CREATE TABLE IF NOT EXISTS prueba(
            codigo_prueba INT(8) NOT NULL,
            nombre VARCHAR(255) NOT NULL,
            fecha DATE NOT NULL,
            hora TIME NOT NULL,
            tipo VARCHAR(255) NOT NULL,
            descripcion VARCHAR(255) NOT NULL,
            CONSTRAINT pk_prueba PRIMARY KEY(codigo_prueba)
        )
    """
                                                   )}

    create_table(tables)

    # Creacion de secuencias para ingresar datos
    add_paciente = (
        "INSERT INTO paciente"
        "(dni, nombre, fecha_nacimiento, direccion)"
        "VALUES (%(dni)s, %(nombre)s, %(fecha_nacimiento)s, %(direccion)s)"
    )

    add_tratamiento = """
        INSERT INTO tratamiento(id_tratamiento, nombre, fecha_inicio)
        VALUES (%(id_tratamiento)s, %(nombre)s, %(fecha_inicio)s)
        """

    add_medico = """
        INSERT INTO medico(num_colegiado, nombre, fecha_nacimiento)
        VALUES(%(num_colegiado)s, %(nombre)s, %(fecha_nacimiento)s)
    """

    add_periodo = """
        INSERT INTO periodo(fecha_inico, fecha_fin)
        VALUES(%(fecha_inicio)s, %(fecha_fin)s)
    """

    add_clinica = """
        INSERT INTO clinica(codigo_postal, numero, calle, ciudad, telefono)
        VALUES (%(codigo_postal)s, %(numero)s, %(calle)s, %(ciudad)s, %(telefono)s)
    """

    add_prueba = """
        INSERT INTO (codigo_prueba, nombre, fecha, hora, tipo, descripcion)
        VALUES (%(codigo_prueba)s, %(nombre)s, %(nombre)s, %(fecha)s, %(hora)s, %(tipo)s, %(descripcion)s)
    """

    # Datos a almacenar

    data_paciente = []
    data = {
        'dni': "109565",
        'nombre': "Maria Jose Rico Pabon",
        'fecha_nacimiento': "1988/03/01",
        'direccion': "Cl 15 # 4-51 Barrio Aeropuerto"
    }
    data_paciente.append(data)

    data = {
        'dni': "109065",
        'nombre': "Pedro Jose Ricon Perez",
        'fecha_nacimiento': "1980/05/22",
        'direccion': "Cl 15 # 4-51 Barrio Aeropuerto"
    }
    data_paciente.append(data)

    data = {
        'dni': "109268",
        'nombre': "Pablo Jose Garcia Perez",
        'fecha_nacimiento': "1970/06/12",
        'direccion': "Cl 15 # 4-51 Barrio Aeropuerto"
    }
    data_paciente.append(data)

    data = {
        'dni': "109265",
        'nombre': "Jose Villamizar Torres",
        'fecha_nacimiento': "1960/08/08",
        'direccion': "Cl 15 # 4-51 Barrio Aeropuerto"
    }
    data_paciente.append(data)

    data = {
        'dni': "109485",
        'nombre': "Fernando Jose Rincon Chavez",
        'fecha_nacimiento': "2000/04/30",
        'direccion': "Cl 15 # 4-51 Barrio Aeropuerto"
    }
    data_paciente.append(data)

    data = {
        'dni': "256532",
        'nombre': "Juan Jose Rico Pabon",
        'fecha_nacimiento': "1980/05/10",
        'direccion': "Cl 15 # 4-51 Barrio Aeropuerto"
    }
    data_paciente.append(data)

    data = {
        'dni': "88161340",
        'nombre': "Pedro Jose Ricon Perez",
        'fecha_nacimiento': "1980/05/22",
        'direccion': "Cl 15 # 4-51 Barrio Aeropuerto"
    }
    data_paciente.append(data)

    for data in data_paciente:
        insert_data(add_paciente, data)

    # show_table()
    # show_atributes("paciente")
    show_info("paciente")

    cursor.close()
    cnx.close()


test()
