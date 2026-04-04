"""
DAO de Pareja Promocion
Data Access Object para operaciones de base de datos de Parejas en promociones 2x1.
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from db_helper import get_db_connection, is_sqlite, is_mysql
from models import ParejaPromocion


# Función para obtener la fecha y hora actual de Perú (UTC-5)
def get_current_datetime_peru():
    """Retorna la fecha y hora actual ajustada a la zona horaria de Perú (UTC-5)"""
    peru_tz = timezone(timedelta(hours=-5))
    ahora_peru = datetime.now(timezone.utc).astimezone(peru_tz)
    return ahora_peru


def get_current_date_peru():
    """Retorna la fecha actual ajustada a la zona horaria de Perú (UTC-5)"""
    peru_tz = timezone(timedelta(hours=-5))
    ahora_peru = datetime.now(timezone.utc).astimezone(peru_tz)
    return ahora_peru.date()


class ParejaPromocionDAO:
    """Clase para acceder a datos de Parejas en Promociones 2x1"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    def obtener_todos(self):
        """Obtiene todas las parejas en promociones"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM parejas_promocion ORDER BY fecha_creacion DESC')
        rows = cursor.fetchall()
        conn.close()
        
        if is_sqlite():
            return [dict(row) for row in rows]
        return rows
    
    def obtener_por_id(self, pareja_id):
        """Obtiene una pareja en promoción por su ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM parejas_promocion WHERE id = %s', (pareja_id,))
        row = cursor.fetchone()
        conn.close()
        
        if is_sqlite():
            return dict(row) if row else None
        return row
    
    def obtener_por_promocion(self, promocion_id):
        """Obtiene todas las parejas de una promoción específica"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM parejas_promocion WHERE promocion_id = %s AND activo = 1 ORDER BY fecha_creacion DESC', (promocion_id,))
        rows = cursor.fetchall()
        conn.close()
        
        if is_sqlite():
            return [dict(row) for row in rows]
        return rows
    
    def obtener_por_cliente(self, cliente_id):
        """Obtiene todas las parejas donde el cliente es principal o secundario"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM parejas_promocion 
            WHERE (cliente_principal_id = %s OR cliente_secundario_id = %s) 
            AND activo = 1
            ORDER BY fecha_creacion DESC
        ''', (cliente_id, cliente_id))
        rows = cursor.fetchall()
        conn.close()
        
        if is_sqlite():
            return [dict(row) for row in rows]
        return rows
    
    def obtener_por_cliente_principal(self, cliente_principal_id):
        """Obtiene todas las parejas donde el cliente es el principal"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM parejas_promocion 
            WHERE cliente_principal_id = %s AND activo = 1
            ORDER BY fecha_creacion DESC
        ''', (cliente_principal_id,))
        rows = cursor.fetchall()
        conn.close()
        
        if is_sqlite():
            return [dict(row) for row in rows]
        return rows
    
    def obtener_pareja_activa_cliente(self, cliente_id):
        """
        Obtiene la pareja activa de un cliente (ya sea principal o secundario).
        Retorna None si no existe una pareja activa.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM parejas_promocion 
            WHERE (cliente_principal_id = %s OR cliente_secundario_id = %s) 
            AND activo = 1 AND separada = 0
            ORDER BY fecha_creacion DESC
            LIMIT 1
        ''', (cliente_id, cliente_id))
        row = cursor.fetchone()
        conn.close()
        
        if is_sqlite():
            return dict(row) if row else None
        return row
    
    def crear(self, pareja_promocion):
        """Crea una nueva pareja en promoción 2x1"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            fecha_creacion = get_current_datetime_peru().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                INSERT INTO parejas_promocion 
                (promocion_id, cliente_principal_id, cliente_secundario_id, precio_total,
                 fecha_creacion, fecha_vencimiento, activo, separada)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                pareja_promocion.promocion_id,
                pareja_promocion.cliente_principal_id,
                pareja_promocion.cliente_secundario_id,
                pareja_promocion.precio_total,
                fecha_creacion,
                pareja_promocion.fecha_vencimiento,
                pareja_promocion.activo if pareja_promocion.activo is not None else 1,
                0  # separada = 0 por defecto
            ))
            pareja_id = cursor.lastrowid
            conn.commit()
            return pareja_id
        finally:
            conn.close()
    
    def crear_from_dict(self, data):
        """Crea una pareja en promoción desde un diccionario"""
        pareja = ParejaPromocion.from_dict(data)
        return self.crear(pareja)
    
    def separar_pareja(self, pareja_id):
        """
        Separa una pareja en promoción 2x1.
        Marca la pareja como separada y actualiza la fecha de separación.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            fecha_separacion = get_current_datetime_peru().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                UPDATE parejas_promocion 
                SET separada = 1, fecha_separacion = %s
                WHERE id = %s
            ''', (fecha_separacion, pareja_id))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    def separar_pareja_por_cliente(self, cliente_id):
        """
        Separa la pareja activa de un cliente.
        """
        pareja = self.obtener_pareja_activa_cliente(cliente_id)
        if pareja:
            return self.separar_pareja(pareja['id'])
        return False
    
    def desactivar_pareja(self, pareja_id):
        """Desactiva una pareja en promoción (soft delete)"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE parejas_promocion 
                SET activo = 0
                WHERE id = %s
            ''', (pareja_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    def actualizar_vencimiento(self, pareja_id, nueva_fecha_vencimiento):
        """Actualiza la fecha de vencimiento de una pareja en promoción"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE parejas_promocion 
                SET fecha_vencimiento = %s
                WHERE id = %s
            ''', (nueva_fecha_vencimiento, pareja_id))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    def obtener_parejas_vencidas(self):
        """
        Obtiene todas las parejas cuya fecha de vencimiento ha pasado.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        fecha_actual = get_current_date_peru()
        
        cursor.execute('''
            SELECT * FROM parejas_promocion 
            WHERE activo = 1 
            AND separada = 0 
            AND DATE(fecha_vencimiento) < %s
            ORDER BY fecha_vencimiento ASC
        ''', (fecha_actual,))
        rows = cursor.fetchall()
        conn.close()
        
        if is_sqlite():
            return [dict(row) for row in rows]
        return rows
    
    def separar_parejas_vencidas(self):
        """
        Separa automáticamente todas las parejas cuya fecha de vencimiento ha pasado.
        Retorna el número de parejas separadas.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            fecha_actual = get_current_date_peru()
            fecha_separacion = get_current_datetime_peru().strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute('''
                UPDATE parejas_promocion 
                SET separada = 1, fecha_separacion = %s
                WHERE activo = 1 
                AND separada = 0 
                AND DATE(fecha_vencimiento) < %s
            ''', (fecha_separacion, fecha_actual))
            
            separadas = cursor.rowcount
            conn.commit()
            
            if separadas > 0:
                print(f"[ParejaPromocion] Se separaron {separadas} pareja(s) con vencimiento vencido")
            
            return separadas
        finally:
            conn.close()
    
    def obtener_detalles_completos(self, pareja_id):
        """
        Obtiene los detalles completos de una pareja en promoción,
        incluyendo información de los clientes y la promoción.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        query = '''
            SELECT 
                pp.id as pareja_id,
                pp.promocion_id,
                pp.cliente_principal_id,
                pp.cliente_secundario_id,
                pp.precio_total,
                pp.fecha_creacion,
                pp.fecha_vencimiento,
                pp.activo,
                pp.separada,
                pp.fecha_separacion,
                p.nombre as promocion_nombre,
                p.tipo_promocion,
                p.precio_2x1,
                c1.dni as cliente_principal_dni,
                c1.nombre_completo as cliente_principal_nombre,
                c1.telefono as cliente_principal_telefono,
                c2.dni as cliente_secundario_dni,
                c2.nombre_completo as cliente_secundario_nombre,
                c2.telefono as cliente_secundario_telefono
            FROM parejas_promocion pp
            INNER JOIN promociones p ON pp.promocion_id = p.id
            INNER JOIN clientes c1 ON pp.cliente_principal_id = c1.id
            INNER JOIN clientes c2 ON pp.cliente_secundario_id = c2.id
            WHERE pp.id = %s
        '''
        
        cursor.execute(query, (pareja_id,))
        row = cursor.fetchone()
        conn.close()
        
        if is_sqlite():
            return dict(row) if row else None
        return row