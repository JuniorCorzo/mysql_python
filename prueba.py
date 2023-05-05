import mysql.connector as mysql
from mysql.connector import errorcode


# noinspection PyGlobalUndefined


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


def create_db():
    try:
        print("Creando la base de datos {}: ".format(db_name), end='')
        cursor.execute(
            "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(db_name))
    except mysql.Error as err:
        if err == errorcode.ER_DB_CREATE_EXISTS:
            print("Ya existe")
        else:
            print("Error al crear la base de datos {}: ".format(err))
    else:
        print("OK")


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


def insert_data(command_sql, dataset):
    try:
        print("Intentando agregar datos: ", end='')
        cursor.execute(command_sql, dataset)
    except mysql.Error as err:
        print(err.msg)
    else:
        print('OK')
        cnx.commit()


def show_table():
    try:
        print("Mostrando tablas de la base de datos {}".format(db_name))
        cursor.execute("SHOW TABLES")
        for tables in cursor:
            print(tables)
    except mysql.Error as err:
        print(err)


def show_atributes(name_table):
    try:
        print("Mostrando los atributos de la tabla {}".format(name_table))
        cursor.execute("DESCRIBE {}".format(name_table))
        for atributtes in cursor:
            print(atributtes)
    except mysql.Error as err:
        print(err.msg)


def show_info(db_table):
    try:
        print("Mostrando la informacion de la tabla {}".format(db_table))
        cursor.execute("SELECT * FROM {}".format(db_table))
        for info in cursor:
            print(info)
    except mysql.Error as err:
        print(err.msg)


def test():
    global db_name
    db_name = "clinicas_oftamologicas"

    create_conexion("root", "0000", "localhost", "3306")
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

    create_db()
    create_table(tables)
    add_paciente = (
        "INSERT INTO paciente"
        "(dni, nombre, fecha_nacimiento, direccion)"
        "VALUES (%(dni)s, %(nombre)s, %(fecha_nacimiento)s, %(direccion)s)"
    )

    data_paciente = {
        'dni': "109565",
        'nombre': "Maria Jose Rico Pabon",
        'fecha_nacimiento': "1988/03/01",
        'direccion': "Cl 15 # 4-51 Barrio Aeropuerto"
    }

    insert_data(add_paciente, data_paciente)
    show_table()
    show_atributes("paciente")
    show_info("paciente")

    cursor.close()
    cnx.close()


test()
