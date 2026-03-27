"""
Helper de Base de Datos para DAOs
Proporciona funciones helper para compatibilidad entre SQLite y MySQL.
OPTIMIZADO: Conversión de parámetros centralizada, sin duplicación de lógica.
"""

from db_config import get_connection, get_db_type, close_connection, is_sqlite, is_mysql
import sqlite3
from datetime import datetime, timedelta, timezone


def get_db_connection():
    """Obtiene una conexión a la base de datos"""
    return get_connection()


def get_db_type():
    """Retorna el tipo de base de datos ('sqlite' o 'mysql')"""
    from db_config import get_config
    return get_config()['DB_TYPE']


def _adapt_query(query):
    """Convierte placeholders ? a %s para MySQL de forma centralizada."""
    if is_mysql() and '?' in query:
        return query.replace('?', '%s')
    return query


def execute_query(query, params=None, fetch=True, commit=True):
    """
    Ejecuta una consulta de forma transparente para SQLite/MySQL.

    Args:
        query: Consulta SQL
        params: Parámetros de la consulta
        fetch: Si True, retorna los resultados
        commit: Si True, hace commit de los cambios

    Returns:
        Lista de diccionarios (si fetch=True) o número de filas afectadas
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        query = _adapt_query(query)

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        is_select = query.strip().upper().startswith(('SELECT', 'WITH', 'SHOW', 'PRAGMA'))

        if fetch or is_select:
            results = cursor.fetchall()
            if commit and not is_select:
                conn.commit()

            if is_sqlite():
                return [dict(row) for row in results]

            # MySQL con dictionary=True ya devuelve dicts
            if results and not isinstance(results[0], dict):
                columns = [col[0] for col in cursor.description] if cursor.description else []
                return [dict(zip(columns, row)) for row in results]
            return results
        else:
            if commit:
                conn.commit()
            return cursor.rowcount if cursor.rowcount else 0

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        close_connection(conn)


def execute_many(query, params_list):
    """
    Ejecuta una consulta múltiples veces con diferentes parámetros.

    Args:
        query: Consulta SQL
        params_list: Lista de tuplas con parámetros

    Returns:
        Número de filas afectadas
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        query = _adapt_query(query)
        cursor.executemany(query, params_list)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        close_connection(conn)


def get_last_row_id(cursor):
    """Obtiene el ID de la última fila insertada"""
    return cursor.lastrowid


def row_factory(cursor, row):
    """Factory para convertir filas a diccionarios"""
    return dict(zip([col[0] for col in cursor.description], row))


def get_table_names():
    """Obtiene todos los nombres de las tablas"""
    if is_sqlite():
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    else:
        query = "SHOW TABLES"

    results = execute_query(query)
    return [list(row.values())[0] for row in results]


def table_exists(table_name):
    """Verifica si una tabla existe"""
    if is_sqlite():
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
        results = execute_query(query, (table_name,))
    else:
        query = "SHOW TABLES LIKE %s"
        results = execute_query(query, (table_name,))

    return len(results) > 0


def column_exists(table_name, column_name):
    """Verifica si una columna existe en una tabla"""
    if is_sqlite():
        query = f"PRAGMA table_info({table_name})"
        results = execute_query(query)
        return any(row['name'] == column_name for row in results)
    else:
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s AND COLUMN_NAME = %s"
        results = execute_query(query, (table_name, column_name))
        return len(results) > 0


def add_column_if_not_exists(table_name, column_name, column_definition):
    """
    Agrega una columna a una tabla si no existe.
    """
    if not column_exists(table_name, column_name):
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
        execute_query(query, commit=True)


def get_current_timestamp():
    """Retorna la función de timestamp actual (UTC del servidor)"""
    if is_sqlite():
        return "datetime('now', 'localtime')"
    else:
        return "NOW()"


def get_current_timestamp_peru():
    """
    Retorna la fecha y hora actual en zona horaria de Perú (America/Lima).

    Para MySQL: retorna una expresión SQL con CONVERT_TZ segura para f-strings y parámetros.
    Para SQLite: retorna el valor literal con comillas, listo para insertar en SQL.

    IMPORTANTE: El valor retornado es una EXPRESIÓN SQL, no un string Python crudo.
    - En f-strings de SQL: úsalo directamente → f"DATE_FORMAT({get_current_timestamp_peru()}, '%Y-%m')"
    - Como parámetro de INSERT/UPDATE: usa get_current_timestamp_peru_value() en su lugar.
    """
    if is_mysql():
        # Expresión SQL que MySQL evalúa en el servidor, sin riesgo de syntax error
        return "CONVERT_TZ(NOW(), 'UTC', 'America/Lima')"
    else:
        # SQLite: calcular en Python y retornar literal con comillas
        peru_tz = timezone(timedelta(hours=-5))
        ahora_peru = datetime.now(timezone.utc).astimezone(peru_tz)
        return f"'{ahora_peru.strftime('%Y-%m-%d %H:%M:%S')}'"


def get_current_timestamp_peru_value():
    """
    Retorna el valor Python (string) de la fecha/hora actual en Perú.
    Usar SOLO como parámetro en cursor.execute() / execute_query(), nunca en f-strings SQL.

    Ejemplo correcto:
        execute_query("INSERT INTO pagos (fecha) VALUES (%s)", (get_current_timestamp_peru_value(),))
    """
    peru_tz = timezone(timedelta(hours=-5))
    ahora_peru = datetime.now(timezone.utc).astimezone(peru_tz)
    return ahora_peru.strftime('%Y-%m-%d %H:%M:%S')


def get_current_date_peru():
    """
    Retorna la fecha actual en zona horaria de Perú (America/Lima).
    IMPORTANTE: Usar esta función en lugar de CURDATE() para todas las consultas
    que necesiten comparar con la fecha actual en hora peruana.
    
    NOTA: Esta función incluye comillas simples externas, úsala SOLO con parámetros
    (cursor.execute con %s), NO en f-strings SQL.
    """
    if is_mysql():
        # Para MySQL: usar CONVERT_TZ para convertir la fecha actual de UTC a Perú
        return "DATE(CONVERT_TZ(NOW(),'UTC','America/Lima'))"
    else:
        # Para SQLite: calcular la fecha actual en zona horaria peruana
        peru_tz = timezone(timedelta(hours=-5))
        ahora_peru = datetime.now(timezone.utc).astimezone(peru_tz)
        return f"'{ahora_peru.strftime('%Y-%m-%d')}'"


def get_current_date_expression():
    """
    Retorna la expresión SQL para la fecha actual en zona horaria de Perú.
    Seguro para usar en f-strings SQL sin necesidad de comillas externas.
    
    Para MySQL: DATE(CONVERT_TZ(NOW(),'UTC','America/Lima'))
    Para SQLite: date('now', 'localtime')
    """
    if is_mysql():
        # Sin comillas simples externas, listo para f-strings
        return "DATE(CONVERT_TZ(NOW(),'UTC','America/Lima'))"
    else:
        return "date('now', 'localtime')"


def get_date_function(date_string=None):
    """Retorna la función de fecha apropiada para la base de datos actual."""
    if is_sqlite():
        if date_string:
            return f"datetime('{date_string}')"
        return "datetime('now', 'localtime')"
    else:
        if date_string:
            return f"STR_TO_DATE('{date_string}', '%Y-%m-%d %H:%i:%s')"
        return "NOW()"


def get_date_format(column, format_string='%Y-%m-%d'):
    """Retorna la función de formato de fecha apropiada."""
    if is_sqlite():
        return column
    else:
        return f"DATE_FORMAT({column}, '{format_string}')"


def get_limit_clause(limit, offset=None):
    """Retorna la cláusula LIMIT apropiada."""
    if offset is not None:
        return f"LIMIT {offset}, {limit}"
    return f"LIMIT {limit}"


def get_concat_function(*args):
    """Retorna la función de concatenación apropiada."""
    if is_sqlite():
        return " || ".join(args)
    else:
        return "CONCAT(" + ", ".join(args) + ")"


def get_coalesce_function(value, default='NULL'):
    """Retorna la función COALESCE apropiada."""
    if is_sqlite():
        return f"COALESCE({value}, {default})"
    else:
        return f"IFNULL({value}, {default})"


def get_date_sub(date, days, interval_type='DAY'):
    """Retorna la función para restar tiempo a una fecha."""
    if is_sqlite():
        return f"datetime('{date}', '-{days} days')"
    else:
        return f"DATE_SUB({date}, INTERVAL {days} {interval_type})"


def get_date_add(date, days, interval_type='DAY'):
    """Retorna la función para sumar tiempo a una fecha."""
    if is_sqlite():
        return f"datetime('{date}', '+{days} days')"
    else:
        return f"DATE_ADD({date}, INTERVAL {days} {interval_type})"


def get_year_function(date_column):
    """Retorna la función para obtener el año de una fecha"""
    if is_sqlite():
        return f"strftime('%Y', {date_column})"
    else:
        return f"YEAR({date_column})"


def get_month_function(date_column):
    """Retorna la función para obtener el mes de una fecha"""
    if is_sqlite():
        return f"strftime('%m', {date_column})"
    else:
        return f"MONTH({date_column})"


def get_day_function(date_column):
    """Retorna la función para obtener el día de una fecha"""
    if is_sqlite():
        return f"strftime('%d', {date_column})"
    else:
        return f"DAY({date_column})"


def get_current_month_expression():
    """
    Retorna la expresión SQL para obtener el mes actual en formato 'YYYY-MM'.
    Seguro para usar en f-strings SQL sin necesidad de comillas externas.
    
    Para MySQL: YEAR(NOW()) * 100 + MONTH(NOW()) o DATE_FORMAT con CONVERT_TZ
    Para SQLite: strftime('%Y-%m', 'now', 'localtime')
    """
    if is_mysql():
        # Usar CONVERT_TZ para obtener la fecha actual en Perú, luego extraer el mes
        return "DATE_FORMAT(CONVERT_TZ(NOW(), 'UTC', 'America/Lima'), '%Y-%m')"
    else:
        return "strftime('%Y-%m', 'now', 'localtime')"


def get_current_year_month():
    """
    Retorna el año y mes actual como string 'YYYY-MM' para usar como parámetro.
    Útil para comparaciones DIRECTAS con DATE_FORMAT sin necesidad de expresión SQL.
    """
    from datetime import datetime, timezone, timedelta
    peru_tz = timezone(timedelta(hours=-5))
    ahora_peru = datetime.now(timezone.utc).astimezone(peru_tz)
    return ahora_peru.strftime('%Y-%m')


# Inicializar configuración al importar
from db_config import load_config
load_config()