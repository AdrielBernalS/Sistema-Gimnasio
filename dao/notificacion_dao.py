"""
DAO para gestionar notificaciones
"""

import sqlite3
import json
from datetime import datetime, timedelta

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql, get_current_timestamp_peru_value

class NotificacionDAO:
    @staticmethod
    def crear_notificacion(tipo, titulo, mensaje, cliente_id=None, usuario_id=None):
        """Crea una nueva notificación"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Obtener timestamp en hora peruana como valor Python (no expresión SQL)
            fecha_creacion = get_current_timestamp_peru_value()
            
            cursor.execute('''
                INSERT INTO notificaciones (tipo, titulo, mensaje, cliente_id, usuario_id, leida, fecha_creacion)
                VALUES (%s, %s, %s, %s, %s, 0, %s)
            ''', (tipo, titulo, mensaje, cliente_id, usuario_id, fecha_creacion))
            
            notificacion_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            return notificacion_id
        except Exception as e:
            print(f"Error creando notificación: {e}")
            return None

    @staticmethod
    def obtener_no_leidas(usuario_id=None, limit=20):
        """Obtiene notificaciones no leídas"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if usuario_id:
                cursor.execute('''
                    SELECT n.*, c.nombre_completo as cliente_nombre
                    FROM notificaciones n
                    LEFT JOIN clientes c ON n.cliente_id = c.id
                    WHERE n.leida = 0 
                    AND (n.usuario_id IS NULL OR n.usuario_id = %s)
                    ORDER BY n.fecha_creacion DESC
                    LIMIT %s
                ''', (usuario_id, limit))
            else:
                cursor.execute('''
                    SELECT n.*, c.nombre_completo as cliente_nombre
                    FROM notificaciones n
                    LEFT JOIN clientes c ON n.cliente_id = c.id
                    WHERE n.leida = 0 
                    AND n.usuario_id IS NULL
                    ORDER BY n.fecha_creacion DESC
                    LIMIT %s
                ''', (limit,))
            
            notificaciones = []
            for row in cursor.fetchall():
                notif = dict(row)
                notif['fecha_creacion'] = notif['fecha_creacion']
                notificaciones.append(notif)
            
            conn.close()
            return notificaciones
        except Exception as e:
            print(f"Error obteniendo notificaciones: {e}")
            return []

    @staticmethod
    def obtener_todas(usuario_id=None, limit=50):
        """Obtiene todas las notificaciones"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if usuario_id:
                cursor.execute('''
                    SELECT n.*, c.nombre_completo as cliente_nombre
                    FROM notificaciones n
                    LEFT JOIN clientes c ON n.cliente_id = c.id
                    WHERE n.usuario_id IS NULL OR n.usuario_id = %s
                    ORDER BY n.fecha_creacion DESC
                    LIMIT %s
                ''', (usuario_id, limit))
            else:
                cursor.execute('''
                    SELECT n.*, c.nombre_completo as cliente_nombre
                    FROM notificaciones n
                    LEFT JOIN clientes c ON n.cliente_id = c.id
                    WHERE n.usuario_id IS NULL
                    ORDER BY n.fecha_creacion DESC
                    LIMIT %s
                ''', (limit,))
            
            notificaciones = []
            for row in cursor.fetchall():
                notif = dict(row)
                notificaciones.append(notif)
            
            conn.close()
            return notificaciones
        except Exception as e:
            print(f"Error obteniendo todas las notificaciones: {e}")
            return []

    @staticmethod
    def marcar_como_leida(notificacion_id):
        """Marca una notificación como leída"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE notificaciones 
                SET leida = 1 
                WHERE id = %s
            ''', (notificacion_id,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error marcando notificación como leída: {e}")
            return False

    @staticmethod
    def marcar_todas_como_leidas(usuario_id=None):
        """Marca todas las notificaciones como leídas"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if usuario_id:
                cursor.execute('''
                    UPDATE notificaciones 
                    SET leida = 1 
                    WHERE leida = 0 
                    AND (usuario_id IS NULL OR usuario_id = %s)
                ''', (usuario_id,))
            else:
                cursor.execute('''
                    UPDATE notificaciones 
                    SET leida = 1 
                    WHERE leida = 0 
                    AND usuario_id IS NULL
                ''')
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error marcando todas las notificaciones como leídas: {e}")
            return False

    @staticmethod
    def eliminar_antiguas(dias=30):
        """Elimina notificaciones antiguas"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            fecha_limite = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                DELETE FROM notificaciones 
                WHERE fecha_creacion < %s
            ''', (fecha_limite,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error eliminando notificaciones antiguas: {e}")
            return False

    @staticmethod
    def contar_no_leidas(usuario_id=None):
        """Cuenta las notificaciones no leídas"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if usuario_id:
                cursor.execute('''
                    SELECT COUNT(*) 
                    FROM notificaciones 
                    WHERE leida = 0 
                    AND (usuario_id IS NULL OR usuario_id = %s)
                ''', (usuario_id,))
            else:
                cursor.execute('''
                    SELECT COUNT(*) 
                    FROM notificaciones 
                    WHERE leida = 0 
                    AND usuario_id IS NULL
                ''')
            
            count = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
            conn.close()
            return count
        except Exception as e:
            print(f"Error contando notificaciones no leídas: {e}")
            return 0