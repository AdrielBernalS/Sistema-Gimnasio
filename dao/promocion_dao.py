"""
DAO de Promoción
Data Access Object para operaciones de base de datos de Promociones.
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from db_helper import get_db_connection, is_sqlite, is_mysql
from models import Promocion


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


class PromocionDAO:
    """Clase para acceder a datos de Promociones"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    def obtener_todos(self):
        """Obtiene todas las promociones"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM promociones ORDER BY fecha_inicio DESC')
        rows = cursor.fetchall()
        conn.close()
        
        if is_sqlite():
            return [dict(row) for row in rows]
        return rows
    
    def obtener_por_id(self, promocion_id):
        """Obtiene una promoción por su ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM promociones WHERE id = %s', (promocion_id,))
        row = cursor.fetchone()
        conn.close()
        
        if is_sqlite():
            return dict(row) if row else None
        return row
    
    def obtener_por_plan(self, plan_id):
        """Obtiene todas las promociones de un plan"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM promociones WHERE plan_id = %s ORDER BY fecha_inicio DESC', (plan_id,))
        rows = cursor.fetchall()
        conn.close()
        
        if is_sqlite():
            return [dict(row) for row in rows]
        return rows
    
    def obtener_vigentes_por_plan(self, plan_id, sexo_cliente=None):
        """
        Obtiene las promociones vigentes para un plan específico.
        Filtra por fecha actual (solo fecha sin hora) y sexo del cliente si es aplicable.
        Las fechas funcionan desde 00:00 del día inicio hasta 23:59 del día fin.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener la fecha actual en hora de Perú (solo fecha, sin hora)
        fecha_actual = get_current_date_peru()
        
        # Consulta base: promociones activas, dentro del rango de fechas
        # La comparación de fechas es solo por fecha (DATE), no por DATETIME
        # fecha_inicio <= fecha_actual (desde 00:00 del día inicio)
        # fecha_fin >= fecha_actual (hasta 23:59 del día fin)
        query = '''
            SELECT * FROM promociones 
            WHERE plan_id = %s 
            AND activo = 1 
            AND DATE(fecha_inicio) <= %s 
            AND DATE(fecha_fin) >= %s
        '''
        params = [plan_id, fecha_actual, fecha_actual]
        
        # Si se proporciona sexo del cliente, filtrar por sexo aplicable
        if sexo_cliente:
            query += ' AND (sexo_aplicable = %s OR sexo_aplicable = %s)'
            params.extend([sexo_cliente, 'todos'])
        else:
            query += ' AND sexo_aplicable = %s'
            params.append('todos')
        
        query += ' LIMIT 1'  # Solo la primera promoción vigente
        
        cursor.execute(query, params)
        row = cursor.fetchone()
        conn.close()
        
        if is_sqlite():
            return dict(row) if row else None
        return row
    
    def obtener_promocion_principal(self, plan_id):
        """
        Obtiene la promoción principal para mostrar en la tarjeta del plan.
        No filtra por sexo_aplicable, para mostrar la promoción aunque sea solo para hombres o mujeres.
        Retorna la primera promoción vigente encontrada.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        fecha_actual = get_current_date_peru()
        
        query = '''
            SELECT * FROM promociones 
            WHERE plan_id = %s 
            AND activo = 1 
            AND DATE(fecha_inicio) <= %s 
            AND DATE(fecha_fin) >= %s
            ORDER BY 
                CASE sexo_aplicable 
                    WHEN 'todos' THEN 1 
                    WHEN 'masculino' THEN 2 
                    WHEN 'femenino' THEN 3 
                END
            LIMIT 1
        '''
        
        cursor.execute(query, (plan_id, fecha_actual, fecha_actual))
        row = cursor.fetchone()
        conn.close()
        
        if is_sqlite():
            return dict(row) if row else None
        return row
    
    def crear(self, promocion):
        """Crea una nueva promoción"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            # Usar get_current_datetime_peru() para evitar el problema de timezone con MySQL
            fecha_creacion = get_current_datetime_peru().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                INSERT INTO promociones 
                (plan_id, nombre, descripcion, porcentaje_descuento, monto_descuento,
                 fecha_inicio, fecha_fin, sexo_aplicable, activo, usuario_id, fecha_creacion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                promocion.plan_id,
                promocion.nombre,
                promocion.descripcion,
                promocion.porcentaje_descuento,
                promocion.monto_descuento,
                promocion.fecha_inicio,
                promocion.fecha_fin,
                promocion.sexo_aplicable or 'todos',
                promocion.activo if promocion.activo is not None else 1,
                promocion.usuario_id,
                fecha_creacion
            ))
            promocion_id = cursor.lastrowid
            conn.commit()
            return promocion_id
        finally:
            conn.close()
    
    def crear_from_dict(self, data):
        """Crea una promoción desde un diccionario"""
        promocion = Promocion.from_dict(data)
        return self.crear(promocion)
    
    def actualizar(self, promocion_id, datos):
        """Actualiza una promoción"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # Construir la consulta UPDATE dinámicamente
            campos = []
            valores = []
            for clave, valor in datos.items():
                campos.append(f'{clave} = %s')
                valores.append(valor)
            
            valores.append(promocion_id)
            
            query = f"UPDATE promociones SET {', '.join(campos)} WHERE id = %s"
            cursor.execute(query, valores)
            conn.commit()
        finally:
            conn.close()
    
    def eliminar(self, promocion_id):
        """Elimina una promoción (soft delete)"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE promociones SET activo = 2 WHERE id = %s', (promocion_id,))
            conn.commit()
        finally:
            conn.close()
    
    def eliminar_promociones_vencidas(self):
        """
        Elimina automáticamente las promociones cuya fecha_fin haya pasado.
        Marca como activo = 2 (eliminado) las promociones vencidas.
        Retorna el número de promociones eliminadas.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            fecha_actual = get_current_date_peru()
            
            # Marcar como eliminadas las promociones cuya fecha_fin sea menor a la fecha actual
            # y que aún estén activas (activo = 1)
            cursor.execute('''
                UPDATE promociones 
                SET activo = 2 
                WHERE activo = 1 
                AND DATE(fecha_fin) < %s
            ''', (fecha_actual,))
            
            eliminadas = cursor.rowcount
            conn.commit()
            return eliminadas
        finally:
            conn.close()
    
    def eliminar_permanente(self, promocion_id):
        """Elimina permanentemente una promoción"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM promociones WHERE id = %s', (promocion_id,))
            conn.commit()
        finally:
            conn.close()
    
    def calcular_precio_con_descuento(self, plan_id, precio_original, sexo_cliente=None):
        """
        Calcula el precio con descuento aplicando la promoción vigente.
        Retorna: (precio_final, descuento_aplicado, promocion_info)
        """
        promocion = self.obtener_vigentes_por_plan(plan_id, sexo_cliente)
        
        if not promocion:
            return precio_original, 0, None
        
        descuento = 0
        
        # Si hay porcentaje de descuento, aplicarlo
        if promocion.get('porcentaje_descuento'):
            descuento = float(precio_original) * (float(promocion['porcentaje_descuento']) / 100)
        # Si hay monto fijo de descuento, usarlo
        elif promocion.get('monto_descuento'):
            descuento = float(promocion['monto_descuento'])
        
        precio_final = float(precio_original) - descuento
        
        # Asegurar que el precio no sea negativo
        if precio_final < 0:
            precio_final = 0
        
        return precio_final, descuento, promocion
    
    def existe_promocion_superpuesta(self, plan_id, fecha_inicio, fecha_fin, sexo_aplicable, promo_id_actual=None):
        """
        Verifica si existe una promoción superpuesta para el mismo plan.
        
        Args:
            plan_id: ID del plan
            fecha_inicio: Fecha de inicio de la promoción
            fecha_fin: Fecha de fin de la promoción
            sexo_aplicable: Sexo aplicable ('todos', 'masculino', 'femenino')
            promo_id_actual: ID de la promoción actual (para excluir al editar)
        
        Returns:
            True si existe una promoción superpuesta, False en caso contrario
        
        Reglas de superposición:
            - 'todos' se superpone con 'todos', 'masculino' y 'femenino'
            - 'masculino' se superpone con 'todos' y 'masculino'
            - 'femenino' se superpone con 'todos' y 'femenino'
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Convertir fechas a formato DATE para comparación
            # Manejar tanto 'YYYY-MM-DD' como 'YYYY-MM-DD HH:MM:SS'
            if isinstance(fecha_inicio, str):
                # Si tiene hora, tomar solo la parte de fecha
                fecha_inicio_str = fecha_inicio.split(' ')[0]
                fecha_inicio_dt = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()
            else:
                fecha_inicio_dt = fecha_inicio
            
            if isinstance(fecha_fin, str):
                # Si tiene hora, tomar solo la parte de fecha
                fecha_fin_str = fecha_fin.split(' ')[0]
                fecha_fin_dt = datetime.strptime(fecha_fin_str, '%Y-%m-%d').date()
            else:
                fecha_fin_dt = fecha_fin
            
            # Determinar los sexos conflictivos según el sexo_aplicable
            if sexo_aplicable == 'todos':
                sexos_conflictivos = ['todos', 'masculino', 'femenino']
            elif sexo_aplicable == 'masculino':
                sexos_conflictivos = ['todos', 'masculino']
            elif sexo_aplicable == 'femenino':
                sexos_conflictivos = ['todos', 'femenino']
            else:
                sexos_conflictivos = [sexo_aplicable]
            
            # Crear placeholders para la consulta IN
            placeholders = ', '.join(['%s'] * len(sexos_conflictivos))
            
            query = f'''
                SELECT COUNT(*) as total FROM promociones 
                WHERE plan_id = %s 
                AND activo = 1
                AND DATE(fecha_inicio) <= %s 
                AND DATE(fecha_fin) >= %s
                AND sexo_aplicable IN ({placeholders})
            '''
            
            params = [plan_id, fecha_fin_dt, fecha_inicio_dt] + sexos_conflictivos
            
            # Excluir la promoción actual si se está editando
            if promo_id_actual:
                query += ' AND id != %s'
                params.append(promo_id_actual)
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            # Manejar ambos casos: resultado como dict (sqlite_row_factory) o como tuple
            if result:
                # Verificar si es un diccionario (sqlite con row_factory o MySQL con dict cursor)
                if isinstance(result, dict):
                    return result['total'] > 0
                # Si es una tupla, acceder por índice
                else:
                    return result[0] > 0
            return False
        
        finally:
            conn.close()
    
    def buscar(self, query):
        """Busca promociones por nombre o descripción"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM promociones 
            WHERE nombre LIKE %s OR descripcion LIKE %s
            ORDER BY fecha_inicio DESC
        ''', (f'%{query}%', f'%{query}%'))
        rows = cursor.fetchall()
        conn.close()
        
        if is_sqlite():
            return [dict(row) for row in rows]
        return rows