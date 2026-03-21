"""
DAO de HistorialMembresia
Data Access Object para operaciones de base de datos del Historial de Membresías.
VERSIÓN SIMPLIFICADA - SIN DEPENDENCIAS EXTERNAS
Usa solo la librería estándar de Python para manejar zona horaria de Perú (UTC-5)
"""

import sqlite3
from models import HistorialMembresia
from datetime import datetime, timedelta, timezone

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql

class HistorialMembresiaDAO:
    """Clase para acceder a datos del Historial de Membresías"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
        # Zona horaria de Perú (UTC-5)
        self.peru_tz = timezone(timedelta(hours=-5))
    
    def _get_connection(self):
        """Obtiene una conexión a la base de datos"""
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row    
        return conn
    
    def _get_fecha_actual_peru(self):
        """Obtiene la fecha y hora actual en zona horaria de Perú (UTC-5)"""
        # Obtener fecha/hora actual en UTC
        ahora_utc = datetime.now(timezone.utc)
        # Convertir a hora de Perú (UTC-5)
        ahora_peru = ahora_utc.astimezone(self.peru_tz)
        # Retornar como string
        return ahora_peru.strftime('%Y-%m-%d %H:%M:%S')
    
    def obtener_todos(self):
        """Obtiene todo el historial de membresías"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT h.*, c.nombre_completo as cliente_nombre, c.dni as cliente_dni,
                   p.nombre as plan_nombre, p.codigo as plan_codigo,
                   u.nombre_completo as usuario_nombre
            FROM historial_membresia h
            JOIN clientes c ON h.cliente_id = c.id
            JOIN planes_membresia p ON h.plan_id = p.id
            LEFT JOIN usuarios u ON h.usuario_id = u.id
            ORDER BY h.fecha_inicio DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_por_id(self, historial_id):
        """Obtiene un historial por su ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT h.*, c.nombre_completo as cliente_nombre, p.nombre as plan_nombre,
                   u.nombre_completo as usuario_nombre
            FROM historial_membresia h
            JOIN clientes c ON h.cliente_id = c.id
            JOIN planes_membresia p ON h.plan_id = p.id
            LEFT JOIN usuarios u ON h.usuario_id = u.id
            WHERE h.id = %s
        ''', (historial_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def obtener_por_cliente(self, cliente_id):
        """Obtiene el historial de un cliente"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT h.*, p.nombre as plan_nombre, p.codigo as plan_codigo,
                   u.nombre_completo as usuario_nombre
            FROM historial_membresia h
            JOIN planes_membresia p ON h.plan_id = p.id
            LEFT JOIN usuarios u ON h.usuario_id = u.id
            WHERE h.cliente_id = %s
            ORDER BY h.id DESC
        ''', (cliente_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_activas(self):
        """Obtiene membresías activas"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT h.*, c.nombre_completo as cliente_nombre, p.nombre as plan_nombre,
                   u.nombre_completo as usuario_nombre
            FROM historial_membresia h
            JOIN clientes c ON h.cliente_id = c.id
            JOIN planes_membresia p ON h.plan_id = p.id
            LEFT JOIN usuarios u ON h.usuario_id = u.id
            WHERE h.estado = 'activa'
            ORDER BY h.fecha_fin ASC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_por_vencer(self, dias=7):
        """Obtiene membresías por vencer en los próximos días"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT h.*, c.nombre_completo as cliente_nombre, c.telefono, 
                   p.nombre as plan_nombre, u.nombre_completo as usuario_nombre
            FROM historial_membresia h
            JOIN clientes c ON h.cliente_id = c.id
            JOIN planes_membresia p ON h.plan_id = p.id
            LEFT JOIN usuarios u ON h.usuario_id = u.id
            WHERE h.estado = 'activa'
            AND h.fecha_fin <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
            ORDER BY h.fecha_fin ASC
        ''', (dias,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def crear(self, historial):
        """Crea un nuevo registro de historial con fecha_registro en zona horaria de Perú"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener la fecha actual en zona horaria de Perú (UTC-5)
        fecha_registro_peru = self._get_fecha_actual_peru()
        
        cursor.execute('''
            INSERT INTO historial_membresia (cliente_id, plan_id, fecha_inicio, fecha_fin, 
                                             monto_pagado, metodo_pago, estado, observaciones,
                                             usuario_id, fecha_registro)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)  
        ''', (
            historial.cliente_id, historial.plan_id, historial.fecha_inicio,
            historial.fecha_fin, historial.monto_pagado, historial.metodo_pago,
            historial.estado if historial.estado else 'activa',
            historial.observaciones,
            historial.usuario_id,
            fecha_registro_peru  # Usar fecha en zona horaria de Perú
        ))
        historial_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return historial_id
    
    def crear_from_dict(self, data):
        """Crea un historial desde un diccionario"""
        historial = HistorialMembresia.from_dict(data)
        return self.crear(historial)
    
    def actualizar_estado(self, historial_id, estado, usuario_id=None):
        """Actualiza el estado de una membresía"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if usuario_id:
            cursor.execute('''
                UPDATE historial_membresia 
                SET estado = %s, usuario_actualizacion = %s
                WHERE id = %s
            ''', (estado, usuario_id, historial_id))
        else:
            cursor.execute('UPDATE historial_membresia SET estado = %s WHERE id = %s', 
                          (estado, historial_id))
        
        conn.commit()
        conn.close()
        return True
    
    def finalizar(self, historial_id, usuario_id=None):
        """Finaliza una membresía (la marca como terminada)"""
        return self.actualizar_estado(historial_id, 'terminada', usuario_id)
    
    def cancelar(self, historial_id, observacion=None, usuario_id=None):
        """Cancela una membresía"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if usuario_id:
            cursor.execute('''
                UPDATE historial_membresia 
                SET estado = %s, observaciones = %s, usuario_actualizacion = %s
                WHERE id = %s
            ''', ('cancelada', observacion, usuario_id, historial_id))
        else:
            cursor.execute('''
                UPDATE historial_membresia 
                SET estado = %s, observaciones = %s 
                WHERE id = %s
            ''', ('cancelada', observacion, historial_id))
        
        conn.commit()
        conn.close()
        return True
    
    def obtener_ultima_membresia(self, cliente_id):
        """Obtiene la última membresía activa de un cliente"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT h.*, p.nombre as plan_nombre, u.nombre_completo as usuario_nombre
            FROM historial_membresia h
            JOIN planes_membresia p ON h.plan_id = p.id
            LEFT JOIN usuarios u ON h.usuario_id = u.id
            WHERE h.cliente_id = %s AND h.estado = 'activa'
            ORDER BY h.fecha_inicio DESC
            LIMIT 1
        ''', (cliente_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def contar_membresias_mes(self, año=None, mes=None):
        """Cuenta las membresías adquiridas en un mes"""
        if año is None or mes is None:
            # Usar la fecha actual de Perú
            ahora_utc = datetime.now(timezone.utc)
            ahora_peru = ahora_utc.astimezone(self.peru_tz)
            if año is None:
                año = ahora_peru.year
            if mes is None:
                mes = ahora_peru.month
        
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM historial_membresia
            WHERE YEAR(fecha_registro) = %s
            AND MONTH(fecha_registro) = %s
        ''', (str(año), str(mes).zfill(2)))
        count = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
        conn.close()
        return count