"""
DAO de Usuario
Data Access Object para operaciones de base de datos de Usuarios (Personal del gimnasio).
"""

import sqlite3
import bcrypt  # <-- NUEVA IMPORTACIÓN

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql, get_current_timestamp_peru_value
from models import Usuario


class UsuarioDAO:
    """Clase para acceder a datos de Usuarios (Personal)"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        """Obtiene una conexión a la base de datos"""
        conn = get_db_connection()
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    # ==========================================
    # FUNCIONES DE ENCRIPTACIÓN
    # ==========================================
    
    def _hash_password(self, password):
        """Encripta una contraseña usando bcrypt"""
        # Convertir a bytes y generar salt
        password_bytes = password.encode('utf-8')
        salt = bcrypt.gensalt()
        # Generar hash
        hashed = bcrypt.hashpw(password_bytes, salt)
        # Devolver como string para guardar en BD
        return hashed.decode('utf-8')
    
    def _check_password(self, plain_password, hashed_password):
        """Verifica si una contraseña coincide con su hash"""
        try:
            password_bytes = plain_password.encode('utf-8')
            # Si el hash almacenado no tiene el formato bcrypt (posible migración)
            if not hashed_password or not hashed_password.startswith('$2b$'):
                # Comparación directa para contraseñas antiguas
                return plain_password == hashed_password
            # Verificación con bcrypt
            return bcrypt.checkpw(password_bytes, hashed_password.encode('utf-8'))
        except Exception as e:
            print(f"Error verificando password: {e}")
            return False
    
    # ==========================================
    # FUNCIONES DE VALIDACIÓN DE DUPLICADOS
    # ==========================================

    def existe_dni(self, dni, excluir_id=None):
        """Verifica si ya existe un usuario ACTIVO o INACTIVO con ese DNI.
        Los usuarios eliminados NO bloquean el registro.
        excluir_id: ID del usuario a ignorar (útil al editar)."""
        conn = self._get_connection()
        cursor = conn.cursor()
        if excluir_id:
            cursor.execute(
                "SELECT id FROM usuarios WHERE dni = %s AND estado IN ('activo', 'inactivo') AND id != %s",
                (dni, excluir_id)
            )
        else:
            cursor.execute(
                "SELECT id FROM usuarios WHERE dni = %s AND estado IN ('activo', 'inactivo')",
                (dni,)
            )
        row = cursor.fetchone()
        conn.close()
        return row is not None

    def existe_telefono(self, telefono, excluir_id=None):
        """Verifica si ya existe un usuario ACTIVO o INACTIVO con ese teléfono.
        Los usuarios eliminados NO bloquean el registro.
        excluir_id: ID del usuario a ignorar (útil al editar)."""
        conn = self._get_connection()
        cursor = conn.cursor()
        if excluir_id:
            cursor.execute(
                "SELECT id FROM usuarios WHERE telefono = %s AND estado IN ('activo', 'inactivo') AND id != %s",
                (telefono, excluir_id)
            )
        else:
            cursor.execute(
                "SELECT id FROM usuarios WHERE telefono = %s AND estado IN ('activo', 'inactivo')",
                (telefono,)
            )
        row = cursor.fetchone()
        conn.close()
        return row is not None

    def existe_email(self, email, excluir_id=None):
        """Verifica si ya existe un usuario ACTIVO o INACTIVO con ese email.
        Los usuarios eliminados NO bloquean el registro.
        Si el email es vacío o None, retorna False (el email no es obligatorio)."""
        if not email or not email.strip():
            return False
        conn = self._get_connection()
        cursor = conn.cursor()
        if excluir_id:
            cursor.execute(
                "SELECT id FROM usuarios WHERE email = %s AND estado IN ('activo', 'inactivo') AND id != %s",
                (email.strip(), excluir_id)
            )
        else:
            cursor.execute(
                "SELECT id FROM usuarios WHERE email = %s AND estado IN ('activo', 'inactivo')",
                (email.strip(),)
            )
        row = cursor.fetchone()
        conn.close()
        return row is not None

    def validar_duplicados(self, dni, telefono, email, excluir_id=None):
        """Valida los tres campos a la vez. Retorna una lista de errores (vacía si no hay problemas)."""
        errores = []
        if self.existe_dni(dni, excluir_id):
            errores.append(f"Ya existe un empleado registrado con este DNI")
        if self.existe_telefono(telefono, excluir_id):
            errores.append(f"Ya existe un empleado registrado con este teléfono")
        if email and self.existe_email(email, excluir_id):
            errores.append(f"Ya existe un empleado registrado con este email.")
        return errores

    def obtener_id_usuario_inicial(self):
        """Obtiene el ID del primer usuario creado (administrador inicial)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM usuarios ORDER BY id ASC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        return dict(row)["id"] if row else None

    def obtener_todos(self):
        """Obtiene todos los usuarios activos e inactivos (no eliminados)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.*, r.nombre as rol_nombre 
            FROM usuarios u
            LEFT JOIN roles r ON u.rol_id = r.id
            WHERE u.estado != 'eliminado'  -- Excluir usuarios eliminados
            ORDER BY 
                CASE u.estado 
                    WHEN 'activo' THEN 1
                    WHEN 'inactivo' THEN 2
                    WHEN 'eliminado' THEN 3
                    ELSE 4
                END,
                u.nombre_completo
        ''')
        rows = cursor.fetchall()
        conn.close()
        # NUNCA devolver la contraseña en las respuestas
        result = []
        for row in rows:
            user_dict = dict(row)
            if 'password' in user_dict:
                del user_dict['password']  # Eliminar por seguridad
            result.append(user_dict)
        return result
    
    def obtener_por_id(self, usuario_id):
        """Obtiene un usuario por su ID (sin contraseña)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM usuarios WHERE id = %s', (usuario_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            user_dict = dict(row)
            if 'password' in user_dict:
                del user_dict['password']  # Eliminar por seguridad
            return user_dict
        return None
    
    def obtener_por_dni(self, dni):
        """Obtiene un usuario por su DNI (sin contraseña)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM usuarios WHERE dni = %s', (dni,))
        row = cursor.fetchone()
        conn.close()
        if row:
            user_dict = dict(row)
            if 'password' in user_dict:
                del user_dict['password']
            return user_dict
        return None
    
    def obtener_por_username(self, username):
        """Obtiene un usuario por su nombre de usuario (incluye contraseña para verificación)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM usuarios WHERE username = %s AND estado = %s', (username, 'activo'))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def verificar_credenciales(self, username, password):
        """Verifica las credenciales de un usuario usando bcrypt"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.*, r.nombre as rol_nombre 
            FROM usuarios u
            LEFT JOIN roles r ON u.rol_id = r.id
            WHERE u.username = %s AND u.estado = 'activo'
        ''', (username,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            user_data = dict(row)
            stored_password = user_data.get('password', '')
            
            # Verificar contraseña usando bcrypt
            if self._check_password(password, stored_password):
                # Eliminar password antes de devolver
                if 'password' in user_data:
                    del user_data['password']
                # Renombrar rol_nombre a rol para compatibilidad
                user_data['rol'] = user_data.pop('rol_nombre', '')
                return user_data
        
        return None
    
    def crear(self, usuario):
        """Crea un nuevo usuario CON CONTRASEÑA ENCRIPTADA
        Si el usuario no tiene username/password (entrenador), se guardan como NULL"""
        # Validar duplicados antes de insertar
        errores = self.validar_duplicados(usuario.dni, usuario.telefono, usuario.email)
        if errores:
            raise ValueError(" | ".join(errores))

        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Encriptar contraseña solo si existe (para empleados)
        # Los entrenadores no tendrán password
        hashed_password = None
        if usuario.password:
            hashed_password = self._hash_password(usuario.password)
        
        # Construir la consulta dinámicamente según los campos disponibles
        campos = ['dni', 'nombre_completo', 'telefono', 'estado', 'fecha_registro', 'usuario_creador_id']
        valores = [
            usuario.dni, usuario.nombre_completo, usuario.telefono,
            usuario.estado if usuario.estado else 'activo',
            usuario.fecha_registro,
            usuario.usuario_creador_id
        ]
        
        # Agregar campos opcionales solo si tienen valor
        if usuario.email:
            campos.append('email')
            valores.append(usuario.email)
        
        if usuario.rol_id:
            campos.append('rol_id')
            valores.append(usuario.rol_id)
        
        if usuario.username:
            campos.append('username')
            valores.append(usuario.username)
        
        # Password: puede ser None o el hash encriptado
        if hashed_password or usuario.password is None:
            # Si es None, guardamos NULL; si tiene valor, guardamos el hash
            campos.append('password')
            valores.append(hashed_password)
        
        # Construir y ejecutar la consulta
        placeholders = ', '.join(['%s'] * len(campos))
        query = f"INSERT INTO usuarios ({', '.join(campos)}) VALUES ({placeholders})"
        
        cursor.execute(query, valores)
        usuario_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return usuario_id
    
    def crear_from_dict(self, data):
        """Crea un usuario desde un diccionario"""
        usuario = Usuario.from_dict(data)
        return self.crear(usuario)
    
    def actualizar(self, usuario_id, datos):
        """Actualiza un usuario (NUNCA actualiza contraseña directamente)"""
        # Validar duplicados excluyendo el propio usuario
        dni = datos.get('dni')
        telefono = datos.get('telefono')
        email = datos.get('email') or ''  # Normalizar None y '' igual
        if dni or telefono or email.strip():
            errores = self.validar_duplicados(
                dni or '', telefono or '', email.strip() or '', excluir_id=usuario_id
            )
            # Filtrar errores de campos vacíos (si no vienen en datos, no validar)
            if not dni:
                errores = [e for e in errores if 'DNI' not in e]
            if not telefono:
                errores = [e for e in errores if 'teléfono' not in e]
            if errores:
                raise ValueError(" | ".join(errores))

        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Eliminar password de los datos si viene (nunca se actualiza por aquí)
        if 'password' in datos:
            del datos['password']
        
        campos = []
        valores = []
        for key, value in datos.items():
            if key not in ['id', 'password']:
                campos.append(f'{key} = %s')
                valores.append(value)
        
        if campos:
            valores.append(usuario_id)
            query = f"UPDATE usuarios SET {', '.join(campos)} WHERE id = %s"
            cursor.execute(query, valores)
            conn.commit()
        
        conn.close()
        return True
    
    def actualizar_password(self, usuario_id, nueva_password):
        """Cambia la contraseña de un usuario (ENCRIPTADA)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Encriptar la nueva contraseña
        hashed_password = self._hash_password(nueva_password)
        
        cursor.execute('UPDATE usuarios SET password = %s WHERE id = %s', 
                      (hashed_password, usuario_id))
        conn.commit()
        conn.close()
        return True
    
    def actualizar_permisos(self, usuario_id, permisos):
        """Actualiza los permisos de un usuario"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE usuarios SET permisos = %s WHERE id = %s', (permisos, usuario_id))
        conn.commit()
        conn.close()
        return True
    
    def cambiar_estado(self, usuario_id, nuevo_estado):
        """Cambia el estado de un usuario (solo 'activo' o 'inactivo')"""
        if nuevo_estado not in ['activo', 'inactivo']:
            raise ValueError("Estado no válido. Use 'activo' o 'inactivo'")
        
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE usuarios SET estado = %s WHERE id = %s', (nuevo_estado, usuario_id))
        conn.commit()
        conn.close()
        return True
    
    def toggle_estado(self, usuario_id):
        """Alterna el estado entre 'activo' y 'inactivo'"""
        usuario = self.obtener_por_id(usuario_id)
        if not usuario:
            return False
        
        nuevo_estado = 'inactivo' if usuario['estado'] == 'activo' else 'activo'
        return self.cambiar_estado(usuario_id, nuevo_estado)
    
    def eliminar(self, usuario_id):
        """Elimina un usuario (soft delete - estado = 'eliminado')"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE usuarios SET estado = 'eliminado' WHERE id = %s", (usuario_id,))
        conn.commit()
        conn.close()
        return True

    def activar(self, usuario_id):
        """Activa un usuario (estado = 'activo')"""
        return self.cambiar_estado(usuario_id, 'activo')
    
    def desactivar(self, usuario_id):
        """Desactiva un usuario (estado = 'inactivo')"""
        return self.cambiar_estado(usuario_id, 'inactivo')
    
    def actualizar_ultimo_login(self, usuario_id):
        """Actualiza la fecha del último login"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener timestamp en hora peruana como valor Python (no expresión SQL)
        ultimo_login = get_current_timestamp_peru_value()
        
        cursor.execute(
            "UPDATE usuarios SET ultimo_login = %s WHERE id = %s",
            (ultimo_login, usuario_id)
        )
        conn.commit()
        conn.close()
        return True
    
    def obtener_por_cargo(self, cargo):
        """Obtiene usuarios por cargo (sin contraseña)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM usuarios WHERE cargo = %s AND estado = %s', (cargo, 'activo'))
        rows = cursor.fetchall()
        conn.close()
        
        result = []
        for row in rows:
            user_dict = dict(row)
            if 'password' in user_dict:
                del user_dict['password']
            result.append(user_dict)
        return result