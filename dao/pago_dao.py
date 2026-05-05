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
    
    def obtener_total_mes(self, año=None, mes=None, funcionalidades=None):
        from datetime import datetime
        if año is None:
            año = datetime.now().year
        if mes is None:
            mes = datetime.now().month

        conn = self._get_connection()
        cursor = conn.cursor()
        
        total = 0.0
        
        pagos_habilitados = True
        ventas_habilitadas = True
        
        if funcionalidades:
            if isinstance(funcionalidades, str):
                import json
                try:
                    funcionalidades = json.loads(funcionalidades)
                except:
                    funcionalidades = []
            pagos_habilitados = 'pagos' in funcionalidades
            ventas_habilitadas = 'productos' in funcionalidades or 'ventas' in funcionalidades
        
        if pagos_habilitados:
            cursor.execute('''
                SELECT COALESCE(SUM(monto), 0) as total
                FROM pagos
                WHERE estado = 'completado'
                AND YEAR(fecha_pago) = %s
                AND MONTH(fecha_pago) = %s
            ''', (año, mes))
            total_pagos = float((lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone()) or 0)
            total += total_pagos
        
        if ventas_habilitadas:
            cursor.execute('''
                SELECT COALESCE(SUM(total), 0) as total
                FROM ventas
                WHERE estado = 'completado'
                AND YEAR(fecha_venta) = %s
                AND MONTH(fecha_venta) = %s
            ''', (año, mes))
            total_ventas = float((lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone()) or 0)
            total += total_ventas
        
        conn.close()
        return total
        
    
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

    def _row_value(self, row, key):
        """Helper para leer un valor de una fila (dict o tuple)"""
        if isinstance(row, dict):
            return row.get(key)
        return row[0]

    def actualizar_monto_detalle(self, detalle_id, nuevo_monto):
        """
        Actualiza el monto de un abono parcial en pagos_detalle.

        Si tras la edición la suma de abonos del período cubre el monto
        del plan, completa el pago automáticamente:
          - Crea un registro en 'pagos' con estado 'completado'
          - Linkea pago_id en todos los pagos_detalle del historial
          - Marca el historial_membresia como 'activa'

        Retorna un dict:
          {
            'actualizado': bool,
            'pago_completo': bool,
            'pago_id': int | None,
            'cliente_id': int | None
          }
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # ── 1. Actualizar el monto del abono ──────────────────────────────
            cursor.execute(
                'UPDATE pagos_detalle SET monto = %s WHERE id = %s',
                (nuevo_monto, detalle_id)
            )
            if cursor.rowcount == 0:
                conn.close()
                return {'actualizado': False, 'pago_completo': False,
                        'pago_id': None, 'cliente_id': None}

            # ── 2. Obtener info del detalle + historial ───────────────────────
            cursor.execute('''
                SELECT pd.historial_id,
                       pd.cliente_id,
                       pd.pago_id,
                       hm.monto_pagado,
                       hm.plan_id,
                       hm.usuario_id
                FROM pagos_detalle pd
                JOIN historial_membresia hm ON pd.historial_id = hm.id
                WHERE pd.id = %s
            ''', (detalle_id,))
            row = cursor.fetchone()

            if not row:
                conn.commit()
                conn.close()
                return {'actualizado': True, 'pago_completo': False,
                        'pago_id': None, 'cliente_id': None}

            row = dict(row)
            historial_id     = row['historial_id']
            cliente_id       = row['cliente_id']
            pago_id_existente = row['pago_id']
            monto_plan       = float(row['monto_pagado'])
            plan_id          = row['plan_id']
            usuario_registro = row['usuario_id']

            # ── 3. Si ya estaba linkeado a un pago completo, solo guardamos ──
            if pago_id_existente is not None:
                conn.commit()
                conn.close()
                return {'actualizado': True, 'pago_completo': False,
                        'pago_id': pago_id_existente, 'cliente_id': cliente_id}

            # ── 4. Sumar todos los abonos pendientes del mismo historial ──────
            cursor.execute('''
                SELECT COALESCE(SUM(monto), 0) AS total_abonado
                FROM pagos_detalle
                WHERE historial_id = %s AND pago_id IS NULL
            ''', (historial_id,))
            fila = cursor.fetchone()
            total_abonado = float(
                list(dict(fila).values())[0] if isinstance(fila, dict) else fila[0]
            ) if fila else 0.0

            # ── 5. Si la suma cubre el monto → completar el pago ─────────────
            if total_abonado >= monto_plan - 0.01:  # tolerancia 1 centavo

                fecha_pago = get_current_timestamp_peru_value()

                # Determinar método: si hay varios distintos → 'mixto'
                cursor.execute('''
                    SELECT DISTINCT metodo_pago
                    FROM pagos_detalle
                    WHERE historial_id = %s AND pago_id IS NULL
                ''', (historial_id,))
                metodos = [
                    (list(dict(r).values())[0] if isinstance(r, dict) else r[0])
                    for r in cursor.fetchall()
                ]
                metodo_final = metodos[0] if len(metodos) == 1 else 'mixto'

                # Crear registro en pagos con estado 'completado'
                cursor.execute('''
                    INSERT INTO pagos
                        (cliente_id, plan_id, monto, metodo_pago,
                         fecha_pago, estado, usuario_registro)
                    VALUES (%s, %s, %s, %s, %s, 'completado', %s)
                ''', (cliente_id, plan_id, monto_plan,
                      metodo_final, fecha_pago, usuario_registro))
                nuevo_pago_id = cursor.lastrowid

                # Linkear todos los detalles pendientes de este historial
                cursor.execute('''
                    UPDATE pagos_detalle
                    SET pago_id = %s
                    WHERE historial_id = %s AND pago_id IS NULL
                ''', (nuevo_pago_id, historial_id))

                # Marcar el historial como activo/completado
                cursor.execute('''
                    UPDATE historial_membresia
                    SET estado = 'activa'
                    WHERE id = %s
                ''', (historial_id,))

                conn.commit()
                conn.close()
                return {
                    'actualizado': True,
                    'pago_completo': True,
                    'pago_id': nuevo_pago_id,
                    'cliente_id': cliente_id
                }

            # ── 6. Pago aún incompleto, solo guardar ─────────────────────────
            conn.commit()
            conn.close()
            return {'actualizado': True, 'pago_completo': False,
                    'pago_id': None, 'cliente_id': cliente_id}

        except Exception as e:
            conn.rollback()
            conn.close()
            raise e