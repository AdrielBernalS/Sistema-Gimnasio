"""
DAO de Invitado
Data Access Object para operaciones de base de datos de Invitados.
"""

import sqlite3
from models import Invitado

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql, get_current_date_peru, get_current_date_expression

class InvitadoDAO:
    """Clase para acceder a datos de Invitados"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    
    def obtener_por_id(self, invitado_id):
        """Obtiene un invitado por su ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT i.*, c.nombre_completo as cliente_titular
            FROM invitados i
            LEFT JOIN clientes c ON i.cliente_titular_id = c.id
            WHERE i.id = %s
        ''', (invitado_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def obtener_por_cliente(self, cliente_titular_id):
        """Obtiene los invitados de un cliente titular"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT i.*, c.nombre_completo as cliente_titular, u.nombre_completo as usuario_nombre
            FROM invitados i
            LEFT JOIN clientes c ON i.cliente_titular_id = c.id
            LEFT JOIN usuarios u ON i.usuario_id = u.id
            WHERE i.cliente_titular_id = %s AND i.estado != 'eliminado'
            ORDER BY i.fecha_visita DESC, i.id DESC
        ''', (cliente_titular_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def obtener_todos(self):
        """Obtiene todos los invitados"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT i.*, c.nombre_completo as cliente_titular, u.nombre_completo as usuario_nombre
            FROM invitados i
            LEFT JOIN clientes c ON i.cliente_titular_id = c.id
            LEFT JOIN usuarios u ON i.usuario_id = u.id
            WHERE i.estado != 'eliminado'
            ORDER BY i.fecha_visita DESC, i.id DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    
    def obtener_hoy(self):
        """Obtiene los invitados de hoy"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT i.*, c.nombre_completo as cliente_titular
            FROM invitados i
            LEFT JOIN clientes c ON i.cliente_titular_id = c.id
            WHERE i.fecha_visita = {get_current_date_expression()} AND i.estado != 'eliminado'
            ORDER BY i.id DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_pendientes(self):
        """Obtiene invitados pendientes (sin registrar salida)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT i.*, c.nombre_completo as cliente_titular
            FROM invitados i
            LEFT JOIN clientes c ON i.cliente_titular_id = c.id
            WHERE i.estado = 'activo'
            ORDER BY i.fecha_visita DESC, i.id DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def crear(self, invitado):
        """Crea un nuevo invitado"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO invitados (cliente_titular_id, nombre, dni, telefono, 
                                  fecha_visita, estado, usuario_id)
            VALUES (%s, %s, %s, %s, %s, %s,%s)
        ''', (
            invitado.cliente_titular_id, invitado.nombre, invitado.dni,
            invitado.telefono, invitado.fecha_visita,
            invitado.estado if invitado.estado else 'activo', invitado.usuario_id
        ))
        invitado_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return invitado_id
    
    def crear_from_dict(self, data):
        """Crea un invitado desde un diccionario"""
        # Asegurar que tenga usuario_id
        if 'usuario_id' not in data or data['usuario_id'] is None:
            # Intentar obtener de la sesión si está disponible
            try:
                from flask import session
                data['usuario_id'] = session.get('usuario_id', 1)
            except:
                # Si no se puede obtener de la sesión, usar 1 (sistema)
                data['usuario_id'] = 1
                
        invitado = Invitado.from_dict(data)
        return self.crear(invitado)
    
    def registrar_salida(self, invitado_id):
        """Registra la salida de un invitado"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE invitados 
            SET hora_salida = CURRENT_TIME, estado = 'completado'
            WHERE id = %s
        ''', (invitado_id,))
        conn.commit()
        conn.close()
        return True
    
    def actualizar(self, invitado_id, datos):
        """Actualiza un invitado"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        campos = []
        valores = []
        for key, value in datos.items():
            if key not in ['id']:
                campos.append(f'{key} = %s')
                valores.append(value)
        
        if campos:
            valores.append(invitado_id)
            query = f"UPDATE invitados SET {', '.join(campos)} WHERE id = %s"
            cursor.execute(query, valores)
            conn.commit()
        
        conn.close()
        return True
    
    def eliminar(self, invitado_id):
        """Elimina un invitado (eliminación lógica)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE invitados SET estado = 'eliminado' WHERE id = %s", (invitado_id,))
        conn.commit()
        conn.close()
        return True
    
    def contar_hoy(self):
        """Cuenta los invitados de hoy"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM invitados WHERE fecha_visita = {get_current_date_expression()} AND estado != 'eliminado'")
        count = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
        conn.close()
        return count
    
    def buscar_por_dni(self, dni):
        """Busca un invitado activo por DNI exacto"""
        if not dni:
            return None
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM invitados WHERE dni = %s AND estado != 'eliminado'", (dni,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def buscar_por_telefono(self, telefono):
        """Busca un invitado activo por teléfono exacto"""
        if not telefono:
            return None
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM invitados WHERE telefono = %s AND estado != 'eliminado'", (telefono,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None