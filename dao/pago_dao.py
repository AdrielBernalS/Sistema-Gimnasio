"""
DAO de Pago
Data Access Object para operaciones de base de datos de Pagos.
"""

import sqlite3

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql, get_current_timestamp_peru, get_current_timestamp_peru_value

class PagoDAO:
    """Clase para acceder a datos de Pagos"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        """Obtiene una conexión a la base de datos"""
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    
    def obtener_por_cliente(self, cliente_id):
        """Obtiene todos los pagos de un cliente"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM pagos WHERE cliente_id = %s ORDER BY fecha_pago DESC', (cliente_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    
    def crear_from_dict(self, data):
        """Crea un pago desde un diccionario"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener timestamp en hora peruana (valor Python, no expresión SQL)
        fecha_pago = get_current_timestamp_peru_value()
        
        cursor.execute('''
        INSERT INTO pagos (cliente_id, plan_id, monto, metodo_pago, 
                          usuario_registro, estado, fecha_pago)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    ''', (
        data.get('cliente_id'),
        data.get('plan_id'),
        data.get('monto'),
        data.get('metodo_pago'),
        data.get('usuario_registro'),
        data.get('estado', 'pendiente'),
        fecha_pago
    ))
        pago_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return pago_id
    
    def obtener_total_mes(self, año=None, mes=None):
        """Obtiene el total de ingresos del mes (pagos de membresías + ventas de productos)"""
        from datetime import datetime
        if año is None:
            año = datetime.now().year
        if mes is None:
            mes = datetime.now().month

        conn = self._get_connection()
        cursor = conn.cursor()

        # Total de pagos de membresías completados
        cursor.execute('''
            SELECT COALESCE(SUM(monto), 0) as total
            FROM pagos
            WHERE estado = 'completado'
            AND YEAR(fecha_pago)  = %s
            AND MONTH(fecha_pago) = %s
        ''', (año, mes))
        total_pagos = float((lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone()) or 0)

        # Total de ventas de productos completadas
        cursor.execute('''
            SELECT COALESCE(SUM(total), 0) as total
            FROM ventas
            WHERE estado = 'completado'
            AND YEAR(fecha_venta)  = %s
            AND MONTH(fecha_venta) = %s
        ''', (año, mes))
        total_ventas = float((lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone()) or 0)

        conn.close()
        return total_pagos + total_ventas
    
    
    def obtener_ingresos_mensuales(self):
        """Obtiene ingresos mensuales de los últimos 6 meses"""

        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(f'''
            SELECT 
                DATE_FORMAT(mes_date, '%Y-%m') as mes_periodo,
                LPAD(MONTH(mes_date), 2, '0') as mes_numero,
                COALESCE(SUM(p.monto), 0) as ingresos_pagos,
                COALESCE(SUM(v.total), 0) as ingresos_ventas,
                COALESCE(SUM(p.monto), 0) + COALESCE(SUM(v.total), 0) as ingresos_totales
            FROM (
                SELECT DATE_SUB(LAST_DAY({get_current_timestamp_peru()}), INTERVAL (5 - n) MONTH) as mes_date
                FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) nums
            ) meses
            LEFT JOIN pagos p 
                ON DATE_FORMAT(p.fecha_pago, '%Y-%m') = DATE_FORMAT(mes_date, '%Y-%m')
                AND p.estado = 'completado'
            LEFT JOIN ventas v 
                ON DATE_FORMAT(v.fecha_venta, '%Y-%m') = DATE_FORMAT(mes_date, '%Y-%m')
                AND v.estado = 'completado'
            GROUP BY mes_date
            ORDER BY mes_date
        ''')

        data = cursor.fetchall()
        conn.close()

        return [dict(row) for row in data]