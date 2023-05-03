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
    db_name = "db_python"

    create_conexion("root", "0000", "localhost", "3306")
    tables = {}
    tables["prueba"] = """
        CREATE TABLE IF NOT EXISTS prueba (
            id_prueba int(11) NOT NULL,
            nombre varchar(255) NOT NULL,
            PRIMARY KEY(id_prueba)
        )
    """
    tables["encargado"] = """
        CREATE TABLE IF NOT EXISTS encargado(
            id_encargado int(11) NOT NULL,
            nombre varchar(255) NOT NULL,
            apellido varchar(255) NOT NULL,
            PRIMARY KEY(id_encargado)
        )
    """
    create_db()
    create_table(tables)
    add_prueba = (
        "INSERT INTO prueba"
        "(id_prueba, nombre)"
        "VALUES (%(id_prueba)s, %(nombre)s)"
    )
    add_encargado = (
        """
        INSERT INTO encargado(id_encargado, nombre, apellido)
        VALUES (%(id_encargado)s, %(nombre)s, %(apellido)s)
        """
    )

    data_prueba = {
        'id_prueba': 3140,
        'nombre': "Angel Corzo"
    }

    data_encargado = {
        "id_encargado": "0001",
        "nombre": "Angel",
        "apellido": "Corzo"
    }

    insert_data(add_prueba, data_prueba)
    insert_data(add_encargado, data_encargado)
    # drop_db()
    show_table()
    show_atributes("prueba")
    show_info("prueba")

    cursor.close()
    cnx.close()


test()
