"""
DAO de PlanMembresia
Data Access Object para operaciones de base de datos de Planes de Membresía.
"""

import sqlite3

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql

class PlanDAO:
    """Clase para acceder a datos de Planes de Membresía"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        """Obtiene una conexión a la base de datos"""
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    def obtener_todos(self):
        """Obtiene todos los planes (excluye los eliminados con estado 2)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM planes_membresia WHERE habilitado < 2 ORDER BY precio')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_habilitados(self):
        """Obtiene solo los planes habilitados"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM planes_membresia WHERE habilitado = 1 ORDER BY precio')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_por_codigo(self, codigo):
        """Obtiene un plan por su código"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM planes_membresia WHERE codigo = %s', (codigo,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def obtener_por_id(self, plan_id):
        """Obtiene un plan por su ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM planes_membresia WHERE id = %s', (plan_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def crear(self, plan):
        """Crea un nuevo plan"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO planes_membresia (codigo, nombre, descripcion, precio, duracion, 
                                         qr_habilitado, permite_aplazamiento, permite_invitados, 
                                         cantidad_invitados, habilitado, envia_whatsapp,usuario_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        ''', (
            plan.codigo, plan.nombre, plan.descripcion,
            plan.precio, plan.duracion, plan.qr_habilitado,
            plan.permite_aplazamiento, plan.permite_invitados,
            plan.cantidad_invitados, plan.habilitado, plan.envia_whatsapp, plan.usuario_id
        ))
        plan_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return plan_id
    
    def actualizar(self, plan_id, datos):
        """Actualiza un plan"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        campos = []
        valores = []
        for key, value in datos.items():
            if key not in ['id']:
                campos.append(f'{key} = %s')
                valores.append(value)
        
        if campos:
            valores.append(plan_id)
            query = f"UPDATE planes_membresia SET {', '.join(campos)} WHERE id = %s"
            cursor.execute(query, valores)
            conn.commit()
        
        conn.close()
        return True
    
    def toggle_habilitado(self, plan_id):
        """Activa o desactiva un plan (solo si no está eliminado)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE planes_membresia 
            SET habilitado = CASE WHEN habilitado = 1 THEN 0 ELSE 1 END
            WHERE id = %s AND habilitado IN (0, 1)
        ''', (plan_id,))
        conn.commit()
        conn.close()
        return True
    
    def crear_from_dict(self, data):
        """Crea un plan desde un diccionario"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO planes_membresia (codigo, nombre, descripcion, precio, duracion, 
                                         qr_habilitado, permite_aplazamiento, permite_invitados, 
                                         cantidad_invitados, habilitado, envia_whatsapp, usuario_id,
                                         limite_semanal)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            data.get('codigo'),
            data.get('nombre'),
            data.get('descripcion'),
            data.get('precio'),
            data.get('duracion'),
            data.get('qr_habilitado', 1),
            data.get('permite_aplazamiento', 1),
            data.get('permite_invitados', 1),
            data.get('cantidad_invitados', 0),
            data.get('habilitado', 1),
            data.get('envia_whatsapp', 1),
            data.get('usuario_id'),
            data.get('limite_semanal', 7)
        ))
        plan_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return plan_id
    
    def eliminar(self, plan_id):
        """Elimina lógicamente un plan (lo marca como eliminado con estado 2)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE planes_membresia SET habilitado = 2 WHERE id = %s', (plan_id,))
        conn.commit()
        conn.close()
        return True
    
    def existe_codigo(self, codigo, exclude_id=None):
        """Verifica si ya existe un plan con ese código"""
        conn = self._get_connection()
        cursor = conn.cursor()
        if exclude_id:
            cursor.execute('SELECT id FROM planes_membresia WHERE codigo = %s AND id != %s', (codigo, exclude_id))
        else:
            cursor.execute('SELECT id FROM planes_membresia WHERE codigo = %s', (codigo,))
        row = cursor.fetchone()
        conn.close()
        return row is not None
    
    def contar_clientes(self, plan_id):
        """Cuenta cuántos clientes activos tienen este plan"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM clientes WHERE plan_id = %s AND activo = 1', (plan_id,))
        count = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
        conn.close()
        return count