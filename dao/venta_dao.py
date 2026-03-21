"""
DAO de Venta
Data Access Object para operaciones de base de datos de Ventas.
"""

import sqlite3
from datetime import datetime

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql


def _normalizar_fecha(fecha_str):
    """Convierte fecha ISO 8601 a formato MySQL datetime"""
    if not fecha_str:
        return None
    from datetime import datetime
    # Manejar formato ISO: 2026-03-04T22:46:56.243Z
    if 'T' in str(fecha_str):
        try:
            dt = datetime.strptime(str(fecha_str)[:19], '%Y-%m-%dT%H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
    return fecha_str

class VentaDAO:
    """Clase para acceder a datos de Ventas"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    def _generar_codigo(self):
        """Genera un código único para la venta"""
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f'VEN-{timestamp}'
    
    def obtener_todos(self):
        """Obtiene todas las ventas NO eliminadas con conteo de productos"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                v.*,
                (SELECT COUNT(*) FROM detalle_ventas dv WHERE dv.venta_id = v.id) as productos_count
            FROM ventas v
            WHERE v.estado != 'eliminado' OR v.estado IS NULL
            ORDER BY v.fecha_venta DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_por_id(self, venta_id):
        """Obtiene una venta por su ID (incluyendo eliminadas)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM ventas WHERE id = %s', (venta_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def eliminar_logico(self, venta_id):
        """Eliminación lógica de una venta"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE ventas 
            SET estado = 'eliminado',
                fecha_modificacion = NOW()
            WHERE id = %s
        ''', (venta_id,))
        conn.commit()
        conn.close()
        return True
    
    def crear(self, venta):
        """Crea una nueva venta"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Generar código si no existe
        codigo = getattr(venta, 'codigo', None) or self._generar_codigo()
        
        # Los datos del cliente ya vienen en el objeto
        cliente_dni = getattr(venta, 'cliente_dni', None)
        cliente_nombre = getattr(venta, 'cliente_nombre', None)
        fecha_venta = _normalizar_fecha(getattr(venta, 'fecha_venta', None)) or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('''
            INSERT INTO ventas (codigo, total, metodo_pago, 
                               cliente_dni, cliente_nombre, fecha_venta, estado, usuario_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
        ''', (
            codigo, venta.total, venta.metodo_pago,
            cliente_dni, cliente_nombre, fecha_venta,
            getattr(venta, 'estado', 'completado'),venta.usuario_id
        ))
        venta_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return venta_id
    
    def crear_con_detalle(self, venta, detalles):
        """Crea una venta con sus detalles"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Generar código único
        codigo = self._generar_codigo()
        
        # Manejar tanto objeto Venta como diccionario
        if hasattr(venta, 'cliente_nombre'):  # Es un objeto Venta
            cliente_nombre = venta.cliente_nombre
            cliente_dni = venta.cliente_dni
            total = venta.total
            metodo_pago = venta.metodo_pago
            fecha_venta = _normalizar_fecha(getattr(venta, 'fecha_venta', None)) or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            usuario_id = getattr(venta, 'usuario_id', None)  # <-- OBTENER usuario_id del objeto Venta
        else:  # Es un diccionario
            cliente_nombre = venta.get('cliente_nombre')
            cliente_dni = venta.get('cliente_dni')
            total = venta.get('total', 0)
            metodo_pago = venta.get('metodo_pago', 'efectivo')
            fecha_venta = _normalizar_fecha(venta.get('fecha_venta')) or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            usuario_id = venta.get('usuario_id')  # <-- OBTENER usuario_id del diccionario
        
        # Insertar venta CON usuario_id
        cursor.execute('''
            INSERT INTO ventas (codigo, total, metodo_pago, 
                            cliente_dni, cliente_nombre, fecha_venta, estado, usuario_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            codigo, 
            total, 
            metodo_pago,
            cliente_dni, 
            cliente_nombre, 
            fecha_venta,
            'completado',
            usuario_id  # <-- AGREGAR usuario_id aquí
        ))
        venta_id = cursor.lastrowid
        
        # Insertar detalles
        for item in detalles:
            cursor.execute('''
                INSERT INTO detalle_ventas (venta_id, producto_id, cantidad, precio_unitario, subtotal)
                VALUES (%s, %s, %s, %s, %s)
            ''', (
                venta_id, 
                item.get('producto_id'), 
                item.get('cantidad', 1), 
                item.get('precio_unitario', 0), 
                item.get('subtotal', 0)
            ))
            
            # Actualizar stock
            cantidad = item.get('cantidad', 1)
            if cantidad > 0:
                cursor.execute('UPDATE productos SET stock = stock - %s WHERE id = %s', 
                            (cantidad, item.get('producto_id')))
        
        conn.commit()
        conn.close()
        return venta_id
    
    def agregar_detalle(self, venta_id, producto_id, cantidad, precio_unitario, subtotal):
        """Agrega un producto a la venta"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO detalle_ventas (venta_id, producto_id, cantidad, precio_unitario, subtotal)
            VALUES (%s, %s, %s, %s, %s)
        ''', (venta_id, producto_id, cantidad, precio_unitario, subtotal))
        conn.commit()
        conn.close()
        return True
    
    def obtener_detalle(self, venta_id):
        """Obtiene el detalle de una venta"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT dv.*, p.nombre as producto_nombre
            FROM detalle_ventas dv
            JOIN productos p ON dv.producto_id = p.id
            WHERE dv.venta_id = %s
        ''', (venta_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_total_dia(self, fecha=None):
        """Obtiene el total de ventas del día"""
        from datetime import datetime
        if fecha is None:
            fecha = datetime.now().strftime('%Y-%m-%d')
        
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COALESCE(SUM(total), 0) as total, COUNT(*) as cantidad
            FROM ventas
            WHERE estado = 'completado'
            AND DATE(fecha_venta) = %s
        ''', (fecha,))
        result = cursor.fetchone()
        conn.close()
        return {'total': (list(result.values())[0] if isinstance(result, dict) else result[0]), 'cantidad': result[1]}

# Agrega estos métodos a la clase VentaDAO en venta_dao.py:

def crear_from_dict(self, data):
    """Crea una venta desde un diccionario"""
    conn = self._get_connection()
    cursor = conn.cursor()
    
    # Generar código único
    codigo = self._generar_codigo()
    
    # Obtener fecha de venta o usar la actual
    fecha_venta = _normalizar_fecha(data.get('fecha_venta')) or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Insertar venta
    cursor.execute('''
        INSERT INTO ventas (codigo, total, metodo_pago, 
                           cliente_dni, cliente_nombre, fecha_venta, estado)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    ''', (
        codigo,
        data.get('total', 0),
        data.get('metodo_pago', 'efectivo'),
        data.get('cliente_dni'),
        data.get('cliente_nombre'),
        fecha_venta,
        'completado'
    ))
    venta_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return venta_id

def obtener_total_mes(self):
    """Obtiene el total de ventas del mes actual"""
    from datetime import datetime
    mes_actual = datetime.now().strftime('%Y-%m')
    
    conn = self._get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COALESCE(SUM(total), 0) 
        FROM ventas 
        WHERE estado = 'completado'
        AND DATE_FORMAT(fecha_venta, '%Y-%m') = %s
    ''', (mes_actual,))
    result = cursor.fetchone()
    conn.close()
    return (list(result.values())[0] if isinstance(result, dict) else result[0]) if result else 0