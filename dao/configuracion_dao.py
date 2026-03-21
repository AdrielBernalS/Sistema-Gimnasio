"""
DAO de Configuracion
Data Access Object para operaciones de base de datos de Configuraciones.
"""

import sqlite3

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql

class ConfiguracionDAO:
    """Clase para acceder a datos de Configuraciones"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        """Obtiene una conexión a la base de datos"""
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn

    def _row_to_dict(self, cursor, row):
        """Convierte una fila a diccionario compatible con SQLite y MySQL"""
        if row is None:
            return None
        if is_sqlite():
            return dict(row)
        else:
            columns = [col[0] for col in cursor.description]
            return dict(zip(columns, row))
    
    def obtener_actual(self):
        """Obtiene la configuración actual"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM configuraciones ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()
        
        # Para MySQL con dictionary=True, row ya es un dict
        # Para SQLite, necesitamos convertirlo
        if is_sqlite():
            result = dict(row) if row else None
        else:
            # MySQL: el cursor con dictionary=True ya devuelve dict
            result = row
        
        conn.close()
        return result
    
    def obtener_color_primario(self):
        """Obtiene el color primario"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT color_primario FROM configuraciones ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()
        conn.close()
        return (list(row.values())[0] if isinstance(row, dict) else row[0]) if row else '#2563eb'
    
    def obtener_color_secundario(self):
        """Obtiene el color secundario"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT color_secundario FROM configuraciones ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()
        conn.close()
        return (list(row.values())[0] if isinstance(row, dict) else row[0]) if row else '#64748b'
    
    def obtener_color_acento(self):
        """Obtiene el color de acento"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT color_acento FROM configuraciones ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()
        conn.close()
        return (list(row.values())[0] if isinstance(row, dict) else row[0]) if row else '#10b981'
    
    def obtener_nombre_empresa(self):
        """Obtiene el nombre de la empresa"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT empresa_nombre FROM configuraciones ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()
        conn.close()
        return (list(row.values())[0] if isinstance(row, dict) else row[0]) if row else ''
    
    def obtener_logo_empresa(self):
        """Obtiene el logo de la empresa"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT empresa_logo FROM configuraciones ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()
        conn.close()
        return (list(row.values())[0] if isinstance(row, dict) else row[0]) if row else None
    
    def crear(self, config):
        """Crea una nueva configuración"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO configuraciones (
                empresa_nombre, empresa_logo, color_primario, color_secundario,
                color_acento, whatsapp_numero, whatsapp_token,
                planes_habilitados, funcionalidades_habilitadas, configuracion_completada
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            config.empresa_nombre, config.empresa_logo, config.color_primario,
            config.color_secundario, config.color_acento, config.whatsapp_numero,
            config.whatsapp_token, config.planes_habilitados,
            config.funcionalidades_habilitadas, config.configuracion_completada
        ))
        config_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return config_id
    
    def actualizar(self, datos):
        """Actualiza la configuración actual"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Primero obtener el ID más reciente
        cursor.execute('SELECT id FROM configuraciones ORDER BY id DESC LIMIT 1')
        result = cursor.fetchone()
        
        if result:
            config_id = result['id'] if isinstance(result, dict) else result[0]
            
            # Ahora hacer el UPDATE con el ID conocido
            cursor.execute('''
                UPDATE configuraciones SET
                    empresa_nombre = %s, empresa_logo = %s, color_primario = %s,
                    color_secundario = %s, color_acento = %s, whatsapp_numero = %s,
                    whatsapp_token = %s, planes_habilitados = %s,
                    funcionalidades_habilitadas = %s, fecha_modificacion = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (
                datos.get('empresa_nombre'), datos.get('empresa_logo'),
                datos.get('color_primario'), datos.get('color_secundario'),
                datos.get('color_acento'), datos.get('whatsapp_numero'),
                datos.get('whatsapp_token'), datos.get('planes_habilitados'),
                datos.get('funcionalidades_habilitadas'),
                config_id
            ))
        
        conn.commit()
        conn.close()
        return True