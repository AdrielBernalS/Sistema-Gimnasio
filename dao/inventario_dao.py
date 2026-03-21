import sqlite3

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql

class InventarioDAO:
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path

    def _get_connection(self):
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn

    def registrar_entrada(self, data):
        """Registra una nueva entrada y actualiza el stock del producto"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # 1. Insertar el registro de entrada
            cursor.execute('''
                INSERT INTO entradas_inventario 
                (producto_id, cantidad, costo_unitario, usuario_registro, observaciones, fecha_entrada)
                VALUES (%s, %s, %s, %s, %s, NOW())
            ''', (
                data['producto_id'], data['cantidad'], 
                data.get('costo_unitario'), data.get('usuario'), 
                data.get('observaciones')
            ))

            # 2. Actualizar el stock actual en la tabla productos
            cursor.execute('''
                UPDATE productos 
                SET stock = stock + %s 
                WHERE id = %s
            ''', (data['cantidad'], data['producto_id']))

            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def obtener_historial_entradas(self):
        """Obtiene el historial de todas las entradas realizadas"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT e.*, p.nombre as producto_nombre
            FROM entradas_inventario e
            JOIN productos p ON e.producto_id = p.id
            ORDER BY e.fecha_entrada DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]