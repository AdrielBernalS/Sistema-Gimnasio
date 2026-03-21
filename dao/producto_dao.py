"""
DAO de Producto
Data Access Object para operaciones de base de datos de Productos.
"""

import sqlite3
from datetime import datetime, timedelta

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql

class ProductoDAO:
    """Clase para acceder a datos de Productos"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        """Obtiene una conexión a la base de datos"""
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    def _get_peru_time(self):
        """Obtiene la fecha y hora actual de Perú (UTC-5)"""
        # Obtener UTC y restar 5 horas para Perú
        utc_now = datetime.utcnow()
        peru_time = utc_now - timedelta(hours=5)
        return peru_time
    
    def _validar_stock_minimo(self, stock, stock_minimo):
        """Valida que el stock mínimo no sea mayor que el stock actual"""
        if stock_minimo is None:
            return True
        if stock is None:
            return True
        return stock_minimo <= stock
    
    def obtener_todos(self, solo_activos=True):
        """Obtiene todos los productos"""
        conn = self._get_connection()
        cursor = conn.cursor()
        if solo_activos:
            cursor.execute('SELECT * FROM productos WHERE estado = %s ORDER BY nombre', ('activo',))
        else:
            cursor.execute('SELECT * FROM productos ORDER BY nombre')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_por_id(self, producto_id):
        """Obtiene un producto por su ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            'SELECT * FROM productos WHERE id = %s',
            (producto_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def obtener_por_categoria(self, categoria):
        """Obtiene productos por categoría"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM productos WHERE categoria = %s AND estado = %s', (categoria, 'activo'))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_con_stock_bajo(self):
        """Obtiene productos con stock bajo (siempre vacío ya que stock_minimo = 0)"""
        return [] 
    
    def crear(self, producto):
        """Crea un nuevo producto con stock mínimo automáticamente en 0"""
        # Forzar stock mínimo a 0
        producto.stock_minimo = 0
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener fecha y hora de Perú
        peru_time = self._get_peru_time()
        fecha_creacion = peru_time.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('''
            INSERT INTO productos (nombre, descripcion, categoria, precio, stock, stock_minimo, 
                                fecha_registro, fecha_actualizacion, estado, usuario_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            producto.nombre, producto.descripcion, producto.categoria,
            producto.precio, producto.stock, producto.stock_minimo,
            fecha_creacion, fecha_creacion, 'activo', producto.usuario_id
        ))
        producto_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return producto_id
    
    def crear_from_dict(self, data):
        """Crea un producto desde un diccionario"""
        from models import Producto
        producto = Producto.from_dict(data)
        return self.crear(producto)
    
    def actualizar(self, producto_id, datos):
        """Actualiza un producto - elimina stock_minimo si viene en datos"""
        # Eliminar stock_minimo de los datos si existe
        if 'stock_minimo' in datos:
            datos['stock_minimo'] = 0  # Forzar a 0 si se intenta modificar
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener fecha y hora de Perú
        peru_time = self._get_peru_time()
        fecha_actualizacion = peru_time.strftime('%Y-%m-%d %H:%M:%S')
        
        campos = []
        valores = []
        for key, value in datos.items():
            if key not in ['id', 'fecha_registro']:
                campos.append(f'{key} = %s')
                valores.append(value)
        
        # Agregar fecha de actualización
        campos.append('fecha_actualizacion = %s')
        valores.append(fecha_actualizacion)
        
        if campos:
            valores.append(producto_id)
            query = f"UPDATE productos SET {', '.join(campos)} WHERE id = %s"
            cursor.execute(query, valores)
            conn.commit()
        
        conn.close()
        return True
        
    def actualizar_stock(self, producto_id, nuevo_stock):
        """Actualiza solo el stock de un producto"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener fecha y hora de Perú
        peru_time = self._get_peru_time()
        fecha_actualizacion = peru_time.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('''
            UPDATE productos SET stock = %s, fecha_actualizacion = %s
            WHERE id = %s
        ''', (nuevo_stock, fecha_actualizacion, producto_id))
        conn.commit()
        conn.close()
        return True
    
    def actualizar_stock_y_minimo(self, producto_id, nuevo_stock, nuevo_stock_minimo=None):
        """Actualiza stock y stock mínimo con validación"""
        # Validar stock mínimo
        if nuevo_stock_minimo is not None and nuevo_stock_minimo > nuevo_stock:
            raise ValueError(f"El stock mínimo ({nuevo_stock_minimo}) no puede ser mayor que el stock actual ({nuevo_stock})")
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener fecha y hora de Perú
        peru_time = self._get_peru_time()
        fecha_actualizacion = peru_time.strftime('%Y-%m-%d %H:%M:%S')
        
        if nuevo_stock_minimo is not None:
            cursor.execute('''
                UPDATE productos SET stock = %s, stock_minimo = %s, fecha_actualizacion = %s
                WHERE id = %s
            ''', (nuevo_stock, nuevo_stock_minimo, fecha_actualizacion, producto_id))
        else:
            cursor.execute('''
                UPDATE productos SET stock = %s, fecha_actualizacion = %s
                WHERE id = %s
            ''', (nuevo_stock, fecha_actualizacion, producto_id))
        
        conn.commit()
        conn.close()
        return True
    
    def eliminar(self, producto_id):
        """Elimina un producto (soft delete - cambia estado a inactivo)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener fecha y hora de Perú para la eliminación
        peru_time = self._get_peru_time()
        fecha_eliminacion = peru_time.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('''
            UPDATE productos SET estado = 'eliminado', fecha_actualizacion = %s
            WHERE id = %s
        ''', (fecha_eliminacion, producto_id))
        conn.commit()
        conn.close()
        return True
    
    def obtener_categorias(self):
        """Obtiene todas las categorías existentes"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT categoria FROM productos WHERE categoria IS NOT NULL ORDER BY categoria')
        rows = cursor.fetchall()
        conn.close()
        return [(list(row.values())[0] if isinstance(row, dict) else row[0]) for row in rows]
    
    def obtener_total_inventario(self):
        """Obtiene el valor total del inventario"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT SUM(stock * precio) FROM productos WHERE estado = %s', ('activo',))
        total = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone()) or 0
        conn.close()
        return total
    
    def forzar_ajustar_stock_minimo(self, producto_id):
        """
        Ajusta automáticamente el stock mínimo si es mayor que el stock actual.
        Esto es útil para corregir datos inconsistentes.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener el producto
        cursor.execute('SELECT stock, stock_minimo FROM productos WHERE id = %s', (producto_id,))
        producto = cursor.fetchone()
        
        if producto:
            stock = producto['stock']
            stock_minimo = producto['stock_minimo']
            
            # Si stock mínimo es mayor que stock, ajustarlo
            if stock_minimo > stock:
                nuevo_stock_minimo = stock
                cursor.execute('''
                    UPDATE productos SET stock_minimo = %s, fecha_actualizacion = %s
                    WHERE id = %s
                ''', (nuevo_stock_minimo, self._get_peru_time().strftime('%Y-%m-%d %H:%M:%S'), producto_id))
                conn.commit()
                conn.close()
                return True, f"Stock mínimo ajustado de {stock_minimo} a {stock}"
        
        conn.close()
        return False, "No se necesitó ajuste"
    
    def obtener_historial_entradas(self, producto_id):
        """
        Obtiene el historial de entradas de inventario para un producto específico.

        Args:
            producto_id: ID del producto para filtrar las entradas

        Returns:
            Lista de diccionarios con las entradas de inventario del producto
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        query = '''
            SELECT e.id, e.cantidad, e.costo_unitario, e.usuario_registro, e.observaciones, e.fecha_entrada
            FROM entradas_inventario e
            WHERE e.producto_id = %s
            ORDER BY e.fecha_entrada DESC
        '''
        cursor.execute(query, (producto_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]