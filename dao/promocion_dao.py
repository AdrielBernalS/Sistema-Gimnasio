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
    
    def obtener_vigentes_por_plan(self, plan_id, sexo_cliente=None, turno_cliente=None, segmento_cliente=None):
        """
        Obtiene las promociones vigentes para un plan específico.
        Filtra por fecha actual (solo fecha sin hora) y sexo del cliente si es aplicable.
        También filtra por turno del cliente si es aplicable.
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
        
        # Si se proporciona turno del cliente, filtrar por turno aplicable
        # Validar que turno_cliente sea un valor válido (no un objeto o string inválido)
        valores_validos_turno = ('manana', 'tarde', 'todos', 'nocturno')
        turno_valido = turno_cliente if turno_cliente and turno_cliente in valores_validos_turno else None
        
        if turno_valido:
            query += ' AND (turno_aplicable = %s OR turno_aplicable = %s)'
            params.extend([turno_valido, 'todos'])
        else:
            query += ' AND (turno_aplicable = %s OR turno_aplicable IS NULL OR turno_aplicable = %s)'
            params.extend(['todos', 'todos'])
        
        # Si se proporciona segmento del cliente, filtrar por segmento aplicable
        # Validar que segmento_cliente sea un valor válido
        valores_validos_segmento = ('todos', 'madre_padre', 'joven', 'adulto', 'adulto_mayor', 
                                    'nino', 'adolescente', 'estudiante', 'empresarial')
        segmento_valido = segmento_cliente if segmento_cliente and segmento_cliente in valores_validos_segmento else None
        
        if segmento_valido:
            query += ' AND (segmento_promocion = %s OR segmento_promocion = %s)'
            params.extend([segmento_valido, 'todos'])
        else:
            query += ' AND (segmento_promocion = %s OR segmento_promocion IS NULL OR segmento_promocion = %s)'
            params.extend(['todos', 'todos'])
            
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
                 fecha_inicio, fecha_fin, sexo_aplicable, turno_aplicable, segmento_promocion,
                 activo, usuario_id, fecha_creacion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                promocion.plan_id,
                promocion.nombre,
                promocion.descripcion,
                promocion.porcentaje_descuento,
                promocion.monto_descuento,
                promocion.fecha_inicio,
                promocion.fecha_fin,
                promocion.sexo_aplicable or 'todos',
                getattr(promocion, 'turno_aplicable', 'todos') or 'todos',
                getattr(promocion, 'segmento_promocion', 'todos') or 'todos',
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
        También limpia los segmentos de clientes que quedaron sin promoción.
        Retorna el número de promociones eliminadas.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            fecha_actual = get_current_date_peru()
            
            # Marcar como eliminadas las promociones vencidas
            cursor.execute('''
                UPDATE promociones 
                SET activo = 2 
                WHERE activo = 1 
                AND DATE(fecha_fin) < %s
            ''', (fecha_actual,))
            
            eliminadas = cursor.rowcount
            conn.commit()
            
            # Limpiar segmentos de clientes que ya no tienen promoción vigente
            if eliminadas > 0:
                self.limpiar_segmentos_clientes_sin_promocion()
            
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
    
    def calcular_precio_con_descuento(self, plan_id, precio_original, sexo_cliente=None, turno_cliente=None, segmento_cliente=None):
        """
        Calcula el precio con descuento aplicando la promoción vigente.
        Retorna: (precio_final, descuento_aplicado, promocion_info)
        """
        promocion = self.obtener_vigentes_por_plan(plan_id, sexo_cliente, turno_cliente, segmento_cliente)
        
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
    
    def existe_promocion_superpuesta(self, plan_id, fecha_inicio, fecha_fin, sexo_aplicable, promo_id_actual=None, turno_aplicable='todos', segmento_promocion='todos'):
        """
        Verifica si existe una promoción superpuesta para el mismo plan.
        
        REGLAS DE SUPERPOSICIÓN:
        - Dos promociones son conflictivas SOLO si aplican al MISMO segmento.
        - Segmentos diferentes NO son conflictivos (ej: Joven vs Adulto pueden coexistir)
        - Si una promoción tiene segmento 'todos', entra en conflicto con TODOS los segmentos.
        - Si tiene sexo 'todos', entra en conflicto con todos los sexos.
        - Si tiene turno 'todos', entra en conflicto con todos los turnos.
        
        Args:
            plan_id: ID del plan
            fecha_inicio: Fecha de inicio de la promoción
            fecha_fin: Fecha de fin de la promoción
            sexo_aplicable: Sexo aplicable ('todos', 'masculino', 'femenino')
            promo_id_actual: ID de la promoción actual (para excluir al editar)
            turno_aplicable: Turno aplicable ('todos', 'manana', 'tarde')
            segmento_promocion: Segmento aplicable ('todos' o valor específico)
        
        Returns:
            True si existe una promoción superpuesta, False en caso contrario
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Convertir fechas a formato DATE para comparación
            if isinstance(fecha_inicio, str):
                fecha_inicio_str = fecha_inicio.split(' ')[0]
                fecha_inicio_dt = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()
            else:
                fecha_inicio_dt = fecha_inicio
            
            if isinstance(fecha_fin, str):
                fecha_fin_str = fecha_fin.split(' ')[0]
                fecha_fin_dt = datetime.strptime(fecha_fin_str, '%Y-%m-%d').date()
            else:
                fecha_fin_dt = fecha_fin
            
            # Construir consulta base
            query = '''
                SELECT COUNT(*) as total FROM promociones 
                WHERE plan_id = %s 
                AND activo = 1
                AND DATE(fecha_inicio) <= %s 
                AND DATE(fecha_fin) >= %s
            '''
            params = [plan_id, fecha_fin_dt, fecha_inicio_dt]
            
            # ========================================
            # VALIDACIÓN DE SEXO (sin cambios)
            # ========================================
            if sexo_aplicable == 'todos':
                # 'todos' entra en conflicto con 'todos', 'masculino' y 'femenino'
                query += ' AND sexo_aplicable IN (%s, %s, %s)'
                params.extend(['todos', 'masculino', 'femenino'])
            elif sexo_aplicable == 'masculino':
                query += ' AND sexo_aplicable IN (%s, %s)'
                params.extend(['todos', 'masculino'])
            elif sexo_aplicable == 'femenino':
                query += ' AND sexo_aplicable IN (%s, %s)'
                params.extend(['todos', 'femenino'])
            
            # ========================================
            # VALIDACIÓN DE TURNO (sin cambios)
            # ========================================
            if turno_aplicable == 'todos':
                query += ' AND turno_aplicable IN (%s, %s, %s)'
                params.extend(['todos', 'manana', 'tarde'])
            elif turno_aplicable == 'manana':
                query += ' AND turno_aplicable IN (%s, %s)'
                params.extend(['todos', 'manana'])
            elif turno_aplicable == 'tarde':
                query += ' AND turno_aplicable IN (%s, %s)'
                params.extend(['todos', 'tarde'])
            
            # ========================================
            # NUEVA VALIDACIÓN DE SEGMENTO
            # Dos promociones SOLO son conflictivas si aplican al MISMO segmento
            # ========================================
            if segmento_promocion == 'todos':
                # 'todos' entra en conflicto con TODOS los segmentos
                # (una promo 'todos' no puede coexistir con ninguna otra)
                query += ' AND (segmento_promocion = %s OR segmento_promocion IS NULL)'
                params.append('todos')
                # Nota: No agregamos más condiciones porque 'todos' bloquea todo
            else:
                # Segmento específico: solo entra en conflicto con:
                # 1. Otra promo del MISMO segmento
                # 2. O una promo con segmento 'todos'
                query += ' AND (segmento_promocion = %s OR segmento_promocion = %s OR segmento_promocion IS NULL)'
                params.extend([segmento_promocion, 'todos'])
            
            # Excluir la promoción actual si se está editando
            if promo_id_actual:
                query += ' AND id != %s'
                params.append(promo_id_actual)
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            # Manejar resultado (dict o tuple)
            if result:
                if isinstance(result, dict):
                    return result['total'] > 0
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

    def limpiar_segmentos_clientes_sin_promocion(self):
        """
        Limpia el campo segmento_promocion de los clientes cuya promoción asociada
        ya no está vigente (fecha_fin pasada o promoción eliminada).
        Retorna el número de clientes actualizados.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Clientes que tienen un segmento asignado (diferente de 'No Asignado')
            # y que NO tienen una promoción vigente para ese segmento
            cursor.execute('''
                UPDATE clientes c
                SET c.segmento = 'No Asignado'
                WHERE c.segmento IS NOT NULL 
                AND c.segmento != 'No Asignado'
                AND NOT EXISTS (
                    SELECT 1 FROM promociones p
                    WHERE p.segmento_promocion = c.segmento
                    AND p.activo = 1
                    AND DATE(p.fecha_fin) >= CURDATE()
                    AND DATE(p.fecha_inicio) <= CURDATE()
                    AND p.plan_id = c.plan_id
                )
            ''')
            
            actualizados = cursor.rowcount
            conn.commit()
            
            if actualizados > 0:
                print(f"[Promociones] Se limpiaron {actualizados} cliente(s) sin promoción vigente")
            
            return actualizados
            
        except Exception as e:
            print(f"Error al limpiar segmentos de clientes: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()