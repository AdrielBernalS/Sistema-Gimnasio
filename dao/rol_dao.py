"""
DAO para gestión de roles
"""

import sqlite3
import json

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql, get_current_timestamp_peru, get_current_timestamp_peru_value

class RolDAO:
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    def obtener_todos(self):
        """Obtiene todos los roles activos e inactivos (excluye eliminados)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM roles WHERE estado != %s ORDER BY nombre', ('eliminado',))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_activos(self):
        """Obtiene solo los roles activos"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM roles WHERE estado = %s ORDER BY nombre', ('activo',))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_por_id(self, rol_id):
        """Obtiene un rol por su ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM roles WHERE id = %s', (rol_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def crear(self, nombre, descripcion="", permisos=None, usuario_creador_id=None):
        """Crea un nuevo rol"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if permisos is None:
            permisos = []
        
        # Obtener timestamp en hora peruana como valor string
        fecha_actual = get_current_timestamp_peru_value()
        
        # Usar la hora peruana (UTC-5)
        cursor.execute('''
            INSERT INTO roles (nombre, descripcion, permisos, estado, 
                            usuario_creador_id, fecha_creacion, fecha_modificacion)
            VALUES (%s, %s, %s, "activo", %s, %s, %s)
        ''', (nombre, descripcion, json.dumps(permisos), usuario_creador_id, fecha_actual, fecha_actual))
        
        rol_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return rol_id
        
    def actualizar(self, rol_id, data):
        """Actualiza un rol"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        campos = []
        valores = []
        for key, value in data.items():
            if key in ['nombre', 'descripcion', 'permisos', 'estado','usuario_creador_id']:
                if key == 'permisos' and isinstance(value, list):
                    value = json.dumps(value)
                campos.append(f'{key} = %s')
                valores.append(value)
        
        # Agregar fecha_modificacion con hora peruana (UTC-5) como valor string
        fecha_modificacion = get_current_timestamp_peru_value()
        campos.append('fecha_modificacion = %s')
        valores.append(fecha_modificacion)
        
        if campos:
            valores.append(rol_id)
            query = f"UPDATE roles SET {', '.join(campos)} WHERE id = %s"
            cursor.execute(query, valores)
            conn.commit()
        
        conn.close()
        return True
    
    def desactivar(self, rol_id):
        """Desactiva un rol (cambia estado a 'inactivo')"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener timestamp en hora peruana como valor string
        fecha_modificacion = get_current_timestamp_peru_value()
        
        cursor.execute("UPDATE roles SET estado = 'inactivo', fecha_modificacion = %s WHERE id = %s", (fecha_modificacion, rol_id,))
        conn.commit()
        conn.close()
        return True
    
    def activar(self, rol_id):
        """Activa un rol (cambia estado a 'activo')"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener timestamp en hora peruana como valor string
        fecha_modificacion = get_current_timestamp_peru_value()
        
        cursor.execute("UPDATE roles SET estado = 'activo', fecha_modificacion = %s WHERE id = %s", (fecha_modificacion, rol_id,))
        conn.commit()
        conn.close()
        return True
    
    def eliminar(self, rol_id, nuevo_rol_id=None):
        """
        Elimina un rol y reasigna los usuarios a otro rol o les quita el rol
        
        Args:
            rol_id: ID del rol a eliminar
            nuevo_rol_id: ID del rol al que reasignar los usuarios (opcional)
                         Si es None, se les quita el rol (rol_id = NULL)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Primero, reasignar o quitar el rol de los usuarios
            if nuevo_rol_id:
                # Reasignar usuarios a otro rol
                cursor.execute('''
                    UPDATE usuarios 
                    SET rol_id = %s
                    WHERE rol_id = %s AND estado != 'eliminado'
                ''', (nuevo_rol_id, rol_id))
            else:
                # Quitar el rol a los usuarios (dejar rol_id como NULL)
                cursor.execute('''
                    UPDATE usuarios 
                    SET rol_id = NULL
                    WHERE rol_id = %s AND estado != 'eliminado'
                ''', (rol_id,))
            
            # Contar cuántos usuarios fueron afectados
            cursor.execute('SELECT COUNT(*) FROM usuarios WHERE rol_id = %s', (rol_id,))
            usuarios_antes = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
            
            # Ahora eliminar el rol (soft delete)
            # Obtener timestamp en hora peruana como valor string
            fecha_modificacion = get_current_timestamp_peru_value()
            cursor.execute("UPDATE roles SET estado = 'eliminado', fecha_modificacion = %s WHERE id = %s", (fecha_modificacion, rol_id,))
            
            conn.commit()
            
            # Obtener número de usuarios afectados
            cursor.execute('SELECT COUNT(*) FROM usuarios WHERE rol_id = %s', (rol_id,))
            usuarios_despues = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
            
            usuarios_afectados = usuarios_antes - usuarios_despues
            
            return {
                'success': True,
                'usuarios_afectados': usuarios_afectados,
                'reasignados': nuevo_rol_id is not None
            }
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def obtener_roles_para_reasignacion(self, rol_id_excluir):
        """Obtiene roles activos para reasignación (excluyendo el rol que se va a eliminar)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, nombre 
            FROM roles 
            WHERE estado = 'activo' AND id != %s
            ORDER BY nombre
        ''', (rol_id_excluir,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_permisos_por_rol(self, rol_id):
        """Obtiene los permisos de un rol específico"""
        rol = self.obtener_por_id(rol_id)
        if rol and rol.get('permisos'):
            try:
                return json.loads(rol['permisos'])
            except:
                return []
        return []
    
    def actualizar_permisos(self, rol_id, permisos):
        """Actualiza los permisos de un rol"""
        return self.actualizar(rol_id, {'permisos': permisos})
    
    def obtener_vistas_disponibles(self):
        """Retorna todas las vistas disponibles en el sistema basándose en la configuración"""
        # Obtener funcionalidades habilitadas desde la configuración
        conn = self._get_connection()
        cursor = conn.cursor()
        funcionalidades_habilitadas = []
        
        try:
            cursor.execute('SELECT funcionalidades_habilitadas FROM configuraciones ORDER BY id DESC LIMIT 1')
            result = cursor.fetchone()
            if result and (list(result.values())[0] if isinstance(result, dict) else result[0]):
                try:
                    funcionalidades_habilitadas = json.loads((list(result.values())[0] if isinstance(result, dict) else result[0]))
                except:
                    funcionalidades_habilitadas = []
        except:
            funcionalidades_habilitadas = []
        finally:
            conn.close()
        
        # Todas las vistas disponibles
        todas_vistas = [
            {'id': 'dashboard', 'nombre': 'Dashboard', 'descripcion': 'Panel principal'},
            {'id': 'clientes', 'nombre': 'Clientes', 'descripcion': 'Gestión de clientes'},
            {'id': 'personal', 'nombre': 'Personal', 'descripcion': 'Gestión de empleados'},
            {'id': 'productos', 'nombre': 'Productos', 'descripcion': 'Inventario y productos'},
            {'id': 'ventas', 'nombre': 'Ventas', 'descripcion': 'Registro de ventas'},
            {'id': 'pagos', 'nombre': 'Pagos', 'descripcion': 'Control de pagos'},
            {'id': 'membresias', 'nombre': 'Membresías', 'descripcion': 'Planes de membresía'},
            {'id': 'acceso', 'nombre': 'Control de Acceso', 'descripcion': 'Registro de entradas'},
            {'id': 'reportes', 'nombre': 'Reportes', 'descripcion': 'Reportes y estadísticas'},
            {'id': 'roles', 'nombre': 'Roles y Permisos', 'descripcion': 'Gestión de roles y permisos', 'categoria': 'Personal'},
            {'id': 'configuracion', 'nombre': 'Configuración', 'descripcion': 'Ajustes del sistema', 'categoria': 'Sistema'}
        ]
        
        # Si hay funcionalidades configuradas, filtrar las vistas
        if funcionalidades_habilitadas:
            # Mapeo de funcionalidades a vistas
            mapeo_funcionalidades = {
                'clientes': ['clientes'],
                'pagos': ['pagos', 'membresias'],
                'productos': ['productos', 'ventas'],
                'reportes': ['reportes'],
                'empleados': ['personal', 'roles'],
                'qr': ['acceso']
            }
            
            vistas_filtradas = []
            for vista in todas_vistas:
                # Por defecto, incluir dashboard y configuración
                if vista['id'] in ['dashboard', 'configuracion']:
                    vistas_filtradas.append(vista)
                    continue
                
                # Verificar si la vista está habilitada
                habilitada = False
                for func, vistas in mapeo_funcionalidades.items():
                    if func in funcionalidades_habilitadas and vista['id'] in vistas:
                        habilitada = True
                        break
                
                if habilitada:
                    vistas_filtradas.append(vista)
            
            return vistas_filtradas
        else:
            # Si no hay funcionalidades configuradas, devolver todas las vistas
            return todas_vistas
    
    def contar_usuarios_por_rol(self, rol_id):
        """Cuenta cuántos usuarios tienen este rol"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) 
            FROM usuarios 
            WHERE rol_id = %s AND estado != 'eliminado'
        ''', (rol_id,))
        count = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
        conn.close()
        return count