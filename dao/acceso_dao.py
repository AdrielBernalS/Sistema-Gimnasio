"""
DAO de Acceso
Data Access Object para operaciones de base de datos de Accesos.
"""

import sqlite3

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql, get_current_timestamp_peru, get_current_timestamp_peru_value, get_current_date_peru, get_current_date_expression

class AccesoDAO:
    """Clase para acceder a datos de Accesos"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        """Obtiene una conexión a la base de datos"""
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    def obtener_todos(self):
        """Obtiene todos los accesos"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT a.*, c.nombre_completo as cliente_nombre
            FROM accesos a
            LEFT JOIN clientes c ON a.cliente_id = c.id
            ORDER BY a.fecha_hora_entrada DESC
            LIMIT 100
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_hoy(self, fecha=None):
        """Obtiene los accesos de hoy con información del cliente y plan"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if fecha:
                cursor.execute('''
                    SELECT 
                        a.id,
                        a.cliente_id,
                        a.tipo,
                        a.dni as acceso_dni,
                        a.metodo_acceso,
                        a.fecha_hora_entrada,
                        c.nombre_completo,
                        c.dni as cliente_dni,
                        p.nombre as plan_nombre,
                        i.nombre as invitado_nombre,
                        i.dni as invitado_dni
                    FROM accesos a
                    LEFT JOIN clientes c ON a.cliente_id = c.id AND a.tipo IN ('cliente', NULL)
                    LEFT JOIN planes_membresia p ON c.plan_id = p.id
                    LEFT JOIN invitados i ON a.cliente_id = i.id AND a.tipo = 'invitado'
                    WHERE DATE(a.fecha_hora_entrada) = DATE(%s)
                    ORDER BY a.fecha_hora_entrada ASC
                ''', (fecha,))
            else:
                cursor.execute(f'''
                    SELECT 
                        a.id,
                        a.cliente_id,
                        a.tipo,
                        a.dni as acceso_dni,
                        a.metodo_acceso,
                        a.fecha_hora_entrada,
                        c.nombre_completo,
                        c.dni as cliente_dni,
                        p.nombre as plan_nombre,
                        i.nombre as invitado_nombre,
                        i.dni as invitado_dni
                    FROM accesos a
                    LEFT JOIN clientes c ON a.cliente_id = c.id AND a.tipo IN ('cliente', NULL)
                    LEFT JOIN planes_membresia p ON c.plan_id = p.id
                    LEFT JOIN invitados i ON a.cliente_id = i.id AND a.tipo = 'invitado'
                    WHERE DATE(a.fecha_hora_entrada) = {get_current_date_expression()}
                    ORDER BY a.fecha_hora_entrada ASC
                ''')
            
            rows = cursor.fetchall()
            
            # Transformar los datos para el frontend
            resultado = []
            for row in rows:
                row_dict = dict(row)
                
                # Determinar si es un cliente o un invitado
                es_invitado = row_dict.get('tipo') == 'invitado'
                
                # Obtener nombre y DNI correctos según el tipo
                if es_invitado:
                    row_dict['cliente_nombre'] = row_dict.get('invitado_nombre') or 'Invitado'
                    row_dict['cliente_dni'] = row_dict.get('invitado_dni') or row_dict.get('acceso_dni') or '--'
                    row_dict['plan_nombre'] = 'Invitado'
                else:
                    row_dict['cliente_nombre'] = row_dict.get('nombre_completo') or 'Cliente'
                    row_dict['cliente_dni'] = row_dict.get('cliente_dni') or row_dict.get('acceso_dni') or '--'
                
                # Formatear la fecha y hora para visualización
                if row_dict.get('fecha_hora_entrada'):
                    try:
                        from datetime import datetime
                        fhe = row_dict['fecha_hora_entrada']
                        if hasattr(fhe, 'strftime'):  # datetime object de MySQL
                            fecha_dt = fhe
                        else:
                            fecha_dt = datetime.strptime(str(fhe), '%Y-%m-%d %H:%M:%S')
                        row_dict['fecha_hora'] = fecha_dt.strftime('%d/%m/%Y - %H:%M')
                        row_dict['fecha_hora_entrada'] = fecha_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        row_dict['fecha_hora'] = str(row_dict['fecha_hora_entrada'])
                else:
                    row_dict['fecha_hora'] = '--'
                
                resultado.append(row_dict)
            
            return resultado
            
        finally:
            conn.close()
    
    def obtener_por_cliente(self, cliente_id):
        """Obtiene los accesos de un cliente"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM accesos 
            WHERE cliente_id = %s 
            ORDER BY fecha_hora_entrada DESC
            LIMIT 50
        ''', (cliente_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def _parsear_duracion(self, duracion_str):
        """
        Parsea una cadena de duración y devuelve un diccionario con tipo y cantidad.
        Ejemplos: "1 mes" -> {'tipo': 'meses', 'cantidad': 1}
                  "7 días" -> {'tipo': 'dias', 'cantidad': 7}
                  "2 horas" -> {'tipo': 'horas', 'cantidad': 2}
        """
        if not duracion_str:
            return {'tipo': 'meses', 'cantidad': 1}
        
        duracion_str = duracion_str.lower().strip()
        
        import re
        # Buscar número y unidad
        match = re.search(r'(\d+)\s*(hora|horas|día|dias|días|mes|meses|semana|semanas)', duracion_str)
        
        if match:
            cantidad = int(match.group(1))
            unidad = match.group(2)
            
            if 'hora' in unidad:
                return {'tipo': 'horas', 'cantidad': cantidad}
            elif 'día' in unidad or 'dia' in unidad:
                return {'tipo': 'dias', 'cantidad': cantidad}
            elif 'semana' in unidad:
                return {'tipo': 'dias', 'cantidad': cantidad * 7}
            elif 'mes' in unidad:
                return {'tipo': 'meses', 'cantidad': cantidad}
        
        # Si no se puede parsear, intentar extraer solo el número
        match_num = re.search(r'(\d+)', duracion_str)
        if match_num:
            return {'tipo': 'dias', 'cantidad': int(match_num.group(1))}
        
        return {'tipo': 'meses', 'cantidad': 1}
    
    def _calcular_fecha_vencimiento_sql(self, duracion):
        """
        Calcula la fecha de vencimiento en formato SQLITE.
        Devuelve una expresión SQL como: "DATE_ADD(NOW(), INTERVAL 7 DAY)"
        """
        info = self._parsear_duracion(duracion)
        tipo = info['tipo']
        cantidad = info['cantidad']
        
        if tipo == 'horas':
            return f"DATE_ADD({get_current_timestamp_peru()}, INTERVAL {cantidad} HOUR)"
        elif tipo == 'dias':
            return f"DATE_ADD({get_current_timestamp_peru()}, INTERVAL {cantidad} DAY)"
        elif tipo == 'meses':
            return f"DATE_ADD({get_current_timestamp_peru()}, INTERVAL {cantidad} MONTH)"
        else:
            return f"DATE_ADD({get_current_timestamp_peru()}, INTERVAL 30 DAY)"
    
    def registrar_entrada(self, cliente_id=None, dni=None, tipo='cliente', metodo='manual',usuario_id=None):
        """Registra una entrada"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener timestamp en hora peruana
        fecha_entrada = get_current_timestamp_peru_value()

        cursor.execute('''
            INSERT INTO accesos (cliente_id, tipo, dni, metodo_acceso, fecha_hora_entrada, usuario_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (cliente_id, tipo, dni, metodo, fecha_entrada, usuario_id))

        acceso_id = cursor.lastrowid

        # Si es un cliente registrado y su plan tiene QR deshabilitado,
        # actualizar fecha_inicio Y fecha_vencimiento del cliente
        if cliente_id is not None and tipo != 'invitado':
            cursor.execute('''
                SELECT p.qr_habilitado, p.duracion FROM clientes c
                JOIN planes_membresia p ON c.plan_id = p.id
                WHERE c.id = %s
            ''', (cliente_id,))
            plan_info = cursor.fetchone()
            if plan_info and plan_info['qr_habilitado'] == 0:
                # Calcular fecha_vencimiento basada en la duración del plan
                fecha_vencimiento = self._calcular_fecha_vencimiento_sql(plan_info['duracion'])
                
                # Obtener timestamp en hora peruana para fecha_inicio
                fecha_actual_peru = get_current_timestamp_peru_value()
                
                # Actualizar fecha_inicio y fecha_vencimiento del cliente
                cursor.execute(f'''
                    UPDATE clientes 
                    SET fecha_inicio = %s, 
                        fecha_vencimiento = {fecha_vencimiento}
                    WHERE id = %s
                ''', (fecha_actual_peru, cliente_id,))
        
        conn.commit()
        conn.close()
        return acceso_id
    
    def contar_entradas_hoy(self):
        """Cuenta las entradas de hoy"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT COUNT(*) FROM accesos
            WHERE DATE(fecha_hora_entrada) = {get_current_date_expression()}
        ''')
        count = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
        conn.close()
        return count
    

    def obtener_clientes_de_hoy(self):
        """
        Obtiene la lista de clientes únicos que han accedido al gimnasio hoy.
        Retorna una lista de diccionarios con: id, nombre_completo, dni, tipo
        Útil para poblar el desplegable de ventas.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Consultar clientes únicos que accedieron hoy (excluyendo invitados para ventas)
            # Un cliente puede haber entrado múltiples veces, pero solo queremos que aparezca una vez
            if is_sqlite():
                # SQLite
                cursor.execute('''
                    SELECT DISTINCT
                        c.id,
                        c.nombre_completo,
                        c.dni,
                        'cliente' as tipo
                    FROM accesos a
                    INNER JOIN clientes c ON a.cliente_id = c.id
                    WHERE DATE(a.fecha_hora_entrada) = DATE('now', 'localtime')
                    AND a.tipo IN ('cliente', NULL)
                    ORDER BY c.nombre_completo ASC
                ''')
            else:
                # MySQL
                cursor.execute(f'''
                    SELECT DISTINCT
                        c.id,
                        c.nombre_completo,
                        c.dni,
                        'cliente' as tipo
                    FROM accesos a
                    INNER JOIN clientes c ON a.cliente_id = c.id
                    WHERE DATE(a.fecha_hora_entrada) = {get_current_date_expression()}
                    AND a.tipo IN ('cliente', NULL)
                    ORDER BY c.nombre_completo ASC
                ''')

            rows = cursor.fetchall()
            clientes = []

            for row in rows:
                row_dict = dict(row)
                # Asegurar que el nombre no sea None
                if not row_dict.get('nombre_completo'):
                    row_dict['nombre_completo'] = 'Cliente sin nombre'
                # Asegurar que el DNI no sea None
                if not row_dict.get('dni'):
                    row_dict['dni'] = '--'
                clientes.append(row_dict)

            return clientes

        finally:
            conn.close()