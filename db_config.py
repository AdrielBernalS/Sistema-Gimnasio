"""
Módulo de Configuración de Base de Datos
Administra las conexiones para SQLite y MySQL de forma transparente.
OPTIMIZADO: Connection pooling para MySQL, sin monkey-patching.
"""

import os
import sqlite3
import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector import pooling

# Variable global para almacenar la configuración
_config = {
    'DB_TYPE': 'sqlite',
    'DB_PATH': 'sistema.db',
    'MYSQL_HOST': 'localhost',
    'MYSQL_PORT': 3306,
    'MYSQL_DATABASE': 'sistema',
    'MYSQL_USER': 'root',
    'MYSQL_PASSWORD': '',
    'MYSQL_CHARSET': 'utf8mb4'
}

# Pool de conexiones MySQL (se crea una sola vez)
_mysql_pool = None


def load_config():
    """Carga la configuración desde variables de entorno o archivo .env"""
    global _config, _mysql_pool

    _config['DB_TYPE'] = os.getenv('DB_TYPE', 'sqlite').lower()
    _config['DB_PATH'] = os.getenv('DB_PATH', 'sistema.db')
    _config['MYSQL_HOST'] = os.getenv('MYSQL_HOST', 'localhost')
    _config['MYSQL_PORT'] = int(os.getenv('MYSQL_PORT', 3306))
    _config['MYSQL_DATABASE'] = os.getenv('MYSQL_DATABASE', 'sistema')
    _config['MYSQL_USER'] = os.getenv('MYSQL_USER', 'root')
    _config['MYSQL_PASSWORD'] = os.getenv('MYSQL_PASSWORD', '')
    _config['MYSQL_CHARSET'] = os.getenv('MYSQL_CHARSET', 'utf8mb4')

    # Resetear el pool si la config cambia
    _mysql_pool = None

    return _config


def get_config():
    """Obtiene la configuración actual"""
    return _config


def set_config(db_type=None, db_path=None, mysql_host=None, mysql_port=None,
               mysql_database=None, mysql_user=None, mysql_password=None, mysql_charset=None):
    """Establece la configuración de base de datos"""
    global _config, _mysql_pool

    if db_type:
        _config['DB_TYPE'] = db_type.lower()
    if db_path:
        _config['DB_PATH'] = db_path
    if mysql_host:
        _config['MYSQL_HOST'] = mysql_host
    if mysql_port:
        _config['MYSQL_PORT'] = mysql_port
    if mysql_database:
        _config['MYSQL_DATABASE'] = mysql_database
    if mysql_user:
        _config['MYSQL_USER'] = mysql_user
    if mysql_password:
        _config['MYSQL_PASSWORD'] = mysql_password
    if mysql_charset:
        _config['MYSQL_CHARSET'] = mysql_charset

    # Resetear el pool al cambiar config
    _mysql_pool = None

    return _config


def _get_mysql_pool():
    """
    Crea o retorna el pool de conexiones MySQL.
    Se crea UNA SOLA VEZ y se reutiliza en todas las requests.
    """
    global _mysql_pool

    if _mysql_pool is None:
        pool_size = int(os.getenv('MYSQL_POOL_SIZE', 5))
        _mysql_pool = pooling.MySQLConnectionPool(
            pool_name="gimnasio_pool",
            pool_size=pool_size,
            pool_reset_session=True,
            host=_config['MYSQL_HOST'],
            port=_config['MYSQL_PORT'],
            database=_config['MYSQL_DATABASE'],
            user=_config['MYSQL_USER'],
            password=_config['MYSQL_PASSWORD'],
            charset=_config['MYSQL_CHARSET'],
            collation='utf8mb4_unicode_ci',
        )

    return _mysql_pool


def get_connection():
    """
    Factory de conexiones.
    MySQL: obtiene conexión del pool (rápido, reutilizable).
    SQLite: crea conexión directa (igual que antes).
    """
    if _config['DB_TYPE'] == 'mysql':
        try:
            conn = _get_mysql_pool().get_connection()
        except MySQLError:
            # Fallback: conexión directa si el pool falla (ej: pool lleno)
            conn = _get_mysql_connection_direct()

        # Configurar timezone sin monkey-patching
        try:
            cur = conn.cursor()
            cur.execute("SET time_zone = 'America/Lima'")
            cur.close()
        except Exception:
            pass

        # Wrapper liviano: sobreescribe cursor() para usar dictionary=True por defecto
        # Sin monkey-patching de execute — solo el kwarg dictionary
        _original_cursor = conn.cursor

        def dict_cursor(*args, **kwargs):
            kwargs.setdefault('dictionary', True)
            return _original_cursor(*args, **kwargs)

        conn.cursor = dict_cursor
        return conn
    else:
        return _get_sqlite_connection()


def _get_sqlite_connection():
    """Crea una conexión SQLite"""
    conn = sqlite3.connect(_config['DB_PATH'], timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _get_mysql_connection_direct():
    """Crea una conexión MySQL directa (fallback sin pool)"""
    try:
        return mysql.connector.connect(
            host=_config['MYSQL_HOST'],
            port=_config['MYSQL_PORT'],
            database=_config['MYSQL_DATABASE'],
            user=_config['MYSQL_USER'],
            password=_config['MYSQL_PASSWORD'],
            charset=_config['MYSQL_CHARSET'],
            collation='utf8mb4_unicode_ci'
        )
    except MySQLError as e:
        print(f"Error de conexión MySQL: {e}")
        raise


def get_db_type():
    """Retorna el tipo de base de datos actual"""
    return _config['DB_TYPE']


def is_sqlite():
    """Verifica si la BD es SQLite"""
    return _config['DB_TYPE'] == 'sqlite'


def is_mysql():
    """Verifica si la BD es MySQL"""
    return _config['DB_TYPE'] == 'mysql'


def _crear_indices_mysql():
    """
    Crea índices de rendimiento en MySQL de forma segura.
    Verifica INFORMATION_SCHEMA antes de crear — nunca falla si ya existen.
    Se llama separado del script SQL para evitar "Commands out of sync".
    """
    indices = [
        ('clientes',           'idx_clientes_dni',         'dni'),
        ('clientes',           'idx_clientes_activo',      'activo'),
        ('clientes',           'idx_clientes_vencimiento', 'fecha_vencimiento'),
        ('clientes',           'idx_clientes_plan',        'plan_id'),
        ('pagos',              'idx_pagos_cliente',        'cliente_id'),
        ('pagos',              'idx_pagos_fecha',          'fecha_pago'),
        ('pagos',              'idx_pagos_estado',         'estado'),
        ('historial_membresia','idx_historial_cliente',    'cliente_id'),
        ('historial_membresia','idx_historial_fechas',     'fecha_inicio'),
        ('notificaciones',     'idx_notif_leida',          'leida'),
        ('notificaciones',     'idx_notif_usuario',        'usuario_id'),
        ('notificaciones',     'idx_notif_fecha',          'fecha_creacion'),
        ('accesos',            'idx_accesos_cliente',      'cliente_id'),
        ('accesos',            'idx_accesos_fecha',        'fecha_hora_entrada'),
        ('ventas',             'idx_ventas_fecha',         'fecha_venta'),
        ('ventas',             'idx_ventas_usuario',       'usuario_id'),
        ('detalle_ventas',     'idx_detalle_venta',        'venta_id'),
        ('productos',          'idx_productos_categoria',  'categoria'),
        ('productos',          'idx_productos_estado',     'estado'),
        ('usuarios',           'idx_usuarios_username',    'username'),
        ('usuarios',           'idx_usuarios_estado',      'estado'),
        ('intentos_login',     'idx_intentos_ip',          'ip_address'),
    ]

    try:
        conn = _get_mysql_connection_direct()
        for tabla, nombre, columna in indices:
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = %s AND INDEX_NAME = %s",
                    (tabla, nombre)
                )
                (existe,) = cur.fetchone()
                cur.close()
                if not existe:
                    cur2 = conn.cursor()
                    cur2.execute(f"CREATE INDEX {nombre} ON {tabla}({columna})")
                    cur2.close()
                    conn.commit()
            except MySQLError as e:
                pass  # índice ya existe u otro error menor — ignorar
        conn.close()
    except Exception as e:
        print(f"Aviso índices: {e}")


def close_connection(conn):
    """
    Cierra una conexión de forma segura.
    En MySQL con pool, devuelve la conexión al pool en lugar de cerrarla.
    """
    if conn:
        try:
            conn.close()
        except Exception:
            pass


def init_database():
    """
    Inicializa la base de datos según el tipo configurado.
    """
    if _config['DB_TYPE'] == 'mysql':
        return _init_mysql_database()
    else:
        return _init_sqlite_database()


def _init_sqlite_database():
    """Inicializa la base de datos SQLite"""
    db_path = _config['DB_PATH']
    sql_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sistema.sql')

    if not os.path.exists(sql_file):
        print(f"ADVERTENCIA: No se encontró el archivo {sql_file}")
        return False

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_script = f.read()

    cursor.executescript(sql_script)
    conn.commit()
    conn.close()

    print(f"Base de datos SQLite inicializada: {db_path}")
    return True


def _init_mysql_database():
    """Inicializa la base de datos MySQL"""
    sql_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sistema.sql')

    if not os.path.exists(sql_file):
        print(f"ADVERTENCIA: No se encontró el archivo {sql_file}")
        return False

    try:
        # Conectar sin BD para crearla si no existe
        conn = mysql.connector.connect(
            host=_config['MYSQL_HOST'],
            port=_config['MYSQL_PORT'],
            user=_config['MYSQL_USER'],
            password=_config['MYSQL_PASSWORD'],
            charset=_config['MYSQL_CHARSET']
        )
        cursor = conn.cursor()
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {_config['MYSQL_DATABASE']} "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cursor.close()
        conn.close()

        # Conectar a la BD y ejecutar el script
        conn = mysql.connector.connect(
            host=_config['MYSQL_HOST'],
            port=_config['MYSQL_PORT'],
            database=_config['MYSQL_DATABASE'],
            user=_config['MYSQL_USER'],
            password=_config['MYSQL_PASSWORD'],
            charset=_config['MYSQL_CHARSET']
        )
        cursor = conn.cursor()

        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        for statement in sql_script.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    cursor.execute(statement)
                except MySQLError as e:
                    if 'Duplicate column' not in str(e):
                        print(f"Warning: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        # Crear índices por separado (evita "Commands out of sync")
        _crear_indices_mysql()

        print(f"Base de datos MySQL inicializada: {_config['MYSQL_DATABASE']}")
        return True

    except MySQLError as e:
        print(f"Error al inicializar MySQL: {e}")
        return False


# Inicializar configuración al importar
load_config()