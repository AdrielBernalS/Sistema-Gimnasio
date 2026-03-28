"""
 Sistema de Gestión de Gimnasio
 Aplicación principal con arquitectura MVC
"""

from dotenv import load_dotenv
load_dotenv()

from flask_session import Session
sess = Session()

# Zona horaria peruana — necesario en servidores UTC (PythonAnywhere, AWS, etc.)
import os
os.environ['TZ'] = 'America/Lima'
try:
    import time
    time.tzset()  # Aplica el cambio en Linux (PythonAnywhere). En Windows no existe pero tampoco es necesario.
except AttributeError:
    pass

from flask import Flask, send_from_directory,render_template, request, redirect, url_for, jsonify, session, flash
from werkzeug.utils import secure_filename
import os
import json
import random
import string
import sqlite3
from datetime import datetime, timedelta
from functools import wraps
from flask_mail import Mail
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

# Importar configuración de base de datos
import db_config
from db_helper import get_db_connection, execute_query, is_sqlite, is_mysql, get_current_timestamp
import time

# Cargar configuración de base de datos
db_config.load_config()

# ==========================================
# CACHÉ EN MEMORIA (evita consultas BD repetidas)
# ==========================================
_config_cache = {'data': None, 'ts': 0, 'ttl': 5}    # se refresca cada 5 seg (multi-worker safe)
_setup_cache  = {'done': None}                          # se invalida al guardar config

def _invalidar_cache():
    """Invalida ambos cachés (llamar al guardar/actualizar configuración)."""
    _config_cache['data'] = None
    _config_cache['ts']   = 0
    _setup_cache['done']  = None

# Decorador personalizado para rutas que requieren login
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Si la configuración inicial no está completada, siempre redirigir
        if not verificar_configuracion_inicial():
            return redirect(url_for('configuracion_inicial'))
        if not session.get('logged_in'):
            flash('Debes iniciar sesión para acceder a esta sección.', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Importar estructura MVC
from models import Cliente, Producto, Usuario, PlanMembresia, Pago, Venta, Configuracion
from dao import cliente_dao, producto_dao, usuario_dao, plan_dao, rol_dao
from controllers import init_auth_controller, init_dashboard_controller, init_clientes_controller, init_productos_controller, init_personal_controller, init_pagos_controller, init_ventas_controller, init_planes_controller, init_invitados_controller, init_acceso_controller, init_roles_controller, init_perfil_controller, init_fotos_controller, init_notificaciones_controller, init_password_recovery_controller, init_reportes_controller 

app = Flask(__name__)
app.config["SESSION_TYPE"] = "sqlalchemy"
app.config["SESSION_MYSQL_HOST"] = os.getenv('MYSQL_HOST', 'localhost')
app.config["SESSION_MYSQL_PORT"] = int(os.getenv('MYSQL_PORT', 3306))
app.config["SESSION_MYSQL_USER"] = os.getenv('MYSQL_USER', 'gimnasio_admin')
app.config["SESSION_MYSQL_PASSWORD"] = os.getenv('MYSQL_PASSWORD', '')
app.config["SESSION_MYSQL_DB"] = os.getenv('MYSQL_DATABASE', 'sistema_gimnasio')
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_USE_SIGNER"] = True
app.config["SECRET_KEY"] = os.getenv('SECRET_KEY')
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("MYSQL_DATABASE_URI")
# SECRET_KEY debe ser fija — si falta en .env, el servidor no arranca
_secret_key = os.getenv('SECRET_KEY')
if not _secret_key:
    raise RuntimeError(
        "❌ SECRET_KEY no está definida en .env\n"
        "Genera una con: python -c \"import secrets; print(secrets.token_hex(32))\"\n"
        "Y agrégala al .env como: SECRET_KEY=tu_clave"
    )
app.secret_key = _secret_key

# Comprimir respuestas HTML/JSON con gzip — reduce tamaño transferido hasta 70%
try:
    from flask_compress import Compress
    Compress(app)
except ImportError:
    pass  # pip install flask-compress para activar
app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, 'static', 'uploads', 'perfiles')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Configuración de Flask-Mail desde .env
app.config['MAIL_SERVER']         = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
app.config['MAIL_PORT']           = int(os.getenv('MAIL_PORT', 587))
app.config['MAIL_USE_TLS']        = os.getenv('MAIL_USE_TLS', 'True') == 'True'
app.config['MAIL_USERNAME']       = os.getenv('MAIL_USERNAME', '')
app.config['MAIL_PASSWORD']       = os.getenv('MAIL_PASSWORD', '')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER', '')

# Inicializar Flask-Mail
mail = Mail(app)
sess.init_app(app)
# ==========================================
# SEGURIDAD DE SESIONES
# ==========================================
app.config['SESSION_COOKIE_HTTPONLY']  = True    # JS no puede leer la cookie
app.config['SESSION_COOKIE_SAMESITE']  = 'Lax'   # Protección CSRF básica
app.config['PERMANENT_SESSION_LIFETIME'] = 28800  # 8 horas

# En producción, la cookie solo viaja por HTTPS
if os.getenv('FLASK_ENV') == 'production':
    app.config['SESSION_COOKIE_SECURE'] = True



# ==========================================
# VERIFICACIÓN GLOBAL DE CONFIGURACIÓN
# ==========================================

@app.before_request
def verificar_setup():
    """Antes de cualquier request, verificar si la configuración inicial está completa"""
    rutas_permitidas = [
        'configuracion_inicial',
        'guardar_configuracion',
        'enviar_codigo_verificacion',
        'verificar_codigo',
        'static',
        'serve_sound',
        'login',
        'recuperar_password',
        'restablecer_password',
    ]
    if request.endpoint in rutas_permitidas:
        return None
    if not verificar_configuracion_inicial():
        return redirect(url_for('configuracion_inicial'))
    return None

# Asegurar que el directorio de uploads existe
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Servir uploads con caché agresiva (1 año) — evita re-descargar logo en cada navegación
@app.route('/static/uploads/perfiles/<path:filename>')
def serve_upload(filename):
    from flask import send_from_directory, make_response
    response = make_response(send_from_directory(app.config['UPLOAD_FOLDER'], filename))
    response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
    return response

# Función global para Jinja2 - obtener configuración
@app.context_processor
def inject_config():
    config = obtener_configuracion()
    funcionalidades = []
    try:
        raw = config.get('funcionalidades_habilitadas', '[]')
        funcionalidades = json.loads(raw) if isinstance(raw, str) else raw or []
    except:
        funcionalidades = ['clientes', 'pagos', 'productos', 'reportes']

    # Verificar si hay planes habilitados que permitan invitados
    hay_invitados_habilitados = False
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Verificar si hay al menos un plan habilitado que permita invitados
        cursor.execute('SELECT COUNT(*) as total FROM planes_membresia WHERE habilitado = 1 AND permite_invitados = 1')
        result = cursor.fetchone()
        if result:
            # Manejar tanto SQLite como MySQL
            if isinstance(result, dict):
                count = result.get('total', 0)
            else:
                count = result[0] if result else 0
            hay_invitados_habilitados = count > 0
        conn.close()
    except Exception as e:
        print(f"Error al verificar invitados habilitados: {e}")
        hay_invitados_habilitados = False

    # Foto del usuario — cacheada en sesión para no consultar BD en cada request
    foto_usuario = None
    usuario_id = session.get('usuario_id')
    if usuario_id:
        # Usar valor cacheado en sesión si existe
        foto_usuario = session.get('_foto_usuario_cache')
        if foto_usuario is None and '_foto_usuario_cache' not in session:
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT foto FROM usuarios WHERE id = %s', (usuario_id,))
                row = cursor.fetchone()
                conn.close()
                if row:
                    foto_usuario = row[0] if isinstance(row, (list, tuple)) else row.get('foto')
                # Guardar en sesión (None también, para no repetir la query)
                session['_foto_usuario_cache'] = foto_usuario
            except Exception:
                pass

    return dict(
        obtener_configuracion=obtener_configuracion,
        config=config,
        funcionalidades=funcionalidades,
        hay_invitados_habilitados=hay_invitados_habilitados,
        foto_usuario=foto_usuario,
    )
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

# ==========================================
# BASE DE DATOS
# ==========================================

def init_db():
    """Inicializar base de datos"""
    return db_config.init_database()

def verificar_configuracion_inicial():
    """
    Verifica si la configuración inicial está completada.
    OPTIMIZADO: resultado en caché — consulta BD solo la primera vez.
    """
    if _setup_cache['done'] is not None:
        return _setup_cache['done']

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT configuracion_completada FROM configuraciones ORDER BY id DESC LIMIT 1')
        result = cursor.fetchone()
        if not result:
            # Sin registros: BD vacía o reseteada — nunca cachear False
            # para que detecte el nuevo registro al guardar configuración
            return False
        if isinstance(result, dict):
            valor = result.get('configuracion_completada') == 1
        else:
            valor = result[0] == 1
        # Solo cachear si está completada — si no, consultar BD en cada request
        # para detectar cuándo el usuario completa la configuración
        if valor:
            _setup_cache['done'] = True
        return valor
    except Exception as e:
        print(f"Error verificando configuración: {e}")
        return False
    finally:
        conn.close()

def generar_codigo_verificacion():
    """Genera un código numérico de 6 dígitos"""
    return ''.join(random.choices(string.digits, k=6))

def obtener_configuracion():
    """
    Obtiene la configuración actual del sistema.
    OPTIMIZADO: caché en memoria con TTL de 60 segundos.
    """
    ahora = time.time()
    if _config_cache['data'] and (ahora - _config_cache['ts']) < _config_cache['ttl']:
        return _config_cache['data']

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if is_sqlite():
            conn.row_factory = sqlite3.Row

        cursor.execute('SELECT * FROM configuraciones ORDER BY id DESC LIMIT 1')
        config = cursor.fetchone()

        if is_sqlite():
            config_dict = dict(config) if config else None
        else:
            config_dict = config

        conn.close()

        if config_dict:
            config_dict.setdefault('color_botones', '#2e9d36')
            config_dict.setdefault('color_sidebar', '#1a1a1a')
            config_dict.setdefault('color_navbar',  '#1a1a1a')
            config_dict.setdefault('color_fondo',   '#000000')
            config_dict.setdefault('color_iconos',  '#ffffff')
            config_dict.setdefault('color_letras',  '#ffffff')
            _config_cache['data'] = config_dict
            _config_cache['ts']   = ahora
            return config_dict
        else:
            defaults = {
                'color_sidebar':  '#1a1a1a',
                'color_navbar':   '#1a1a1a',
                'color_fondo':    '#000000',
                'color_iconos':   '#ffffff',
                'color_letras':   '#ffffff',
                'color_botones':  '#2e9d36',
                'empresa_nombre': 'Sistema de Gimnasio',
                'empresa_logo':   None
            }
            _config_cache['data'] = defaults
            _config_cache['ts']   = ahora
            return defaults

    except Exception as e:
        print(f"Error al obtener configuración: {e}")
        import traceback
        traceback.print_exc()
        return {
            'color_sidebar':  '#1a1a1a',
            'color_navbar':   '#1a1a1a',
            'color_fondo':    '#000000',
            'color_iconos':   '#ffffff',
            'color_letras':   '#ffffff',
            'color_botones':  '#2e9d36',
            'empresa_nombre': 'Sistema de Gimnasio',
            'empresa_logo':   None
        }

def enviar_codigo_verificacion_email(email, codigo, empresa_nombre):
    """Envía un correo con el código de verificación"""
    try:
        # Configuración del servidor SMTP
        mail_server = app.config.get('MAIL_SERVER', 'smtp.gmail.com')
        mail_port = app.config.get('MAIL_PORT', 587)
        mail_username = app.config.get('MAIL_USERNAME')
        mail_password = app.config.get('MAIL_PASSWORD')
        
        # Crear mensaje
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'Código de Verificación - {empresa_nombre}'
        msg['From'] = mail_username
        msg['To'] = email
        
        # HTML del correo con diseño profesional
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f4f4;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f4f4f4; padding: 20px;">
                <tr>
                    <td align="center">
                        <table width="100%" cellpadding="0" cellspacing="0" style="max-width: 500px; background-color: #ffffff; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                            <!-- Header -->
                            <tr>
                                <td style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center; border-radius: 10px 10px 0 0;">
                                    <h1 style="color: #ffffff; margin: 0; font-size: 24px; font-weight: 600;">{empresa_nombre}</h1>
                                    <p style="color: #ffffff; margin: 10px 0 0 0; font-size: 14px; opacity: 0.9;">Sistema de Gestión</p>
                                </td>
                            </tr>
                            <!-- Contenido -->
                            <tr>
                                <td style="padding: 40px 30px;">
                                    <h2 style="color: #333333; margin: 0 0 20px 0; font-size: 20px; text-align: center;">Código de Verificación</h2>
                                    <p style="color: #666666; font-size: 14px; line-height: 1.6; text-align: center; margin: 0 0 30px 0;">
                                        Para completar la configuración de su cuenta de administrador, ingrese el siguiente código de verificación:
                                    </p>
                                    <!-- Código de verificación -->
                                    <table width="100%" cellpadding="0" cellspacing="0">
                                        <tr>
                                            <td align="center">
                                                <div style="background-color: #f8f9fa; border: 2px dashed #667eea; border-radius: 8px; padding: 20px 30px; display: inline-block;">
                                                    <span style="font-size: 36px; font-weight: bold; color: #667eea; letter-spacing: 8px;">{codigo}</span>
                                                </div>
                                            </td>
                                        </tr>
                                    </table>
                                    <p style="color: #999999; font-size: 12px; text-align: center; margin: 30px 0 0 0;">
                                        Este código vence en 10 minutos.<br>
                                        Si no solicitó este código, puede ignorar este correo.
                                    </p>
                                </td>
                            </tr>
                            <!-- Footer -->
                            <tr>
                                <td style="background-color: #f8f9fa; padding: 20px; text-align: center; border-radius: 0 0 10px 10px;">
                                    <p style="color: #999999; font-size: 12px; margin: 0;">
                                        © 2024 {empresa_nombre}. Todos los derechos reservados.
                                    </p>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """
        
        part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(part)

        # Encodear Subject con UTF-8 para soportar caracteres especiales
        from email.header import Header
        msg.replace_header('Subject', Header(f'Codigo de Verificacion - {empresa_nombre}', 'utf-8'))
        
        # Enviar correo usando sendmail con bytes para soportar UTF-8
        server = smtplib.SMTP(mail_server, mail_port)
        server.starttls()
        server.login(mail_username, mail_password)
        server.sendmail(mail_username, email, msg.as_bytes())
        server.quit()
        
        return True, "Correo enviado exitosamente"
    except Exception as e:
        print(f"Error al enviar correo: {e}")
        return False, str(e)

# ==========================================
# RUTAS PRINCIPALES
# ==========================================

@app.route('/static/sounds/<path:filename>')
def serve_sound(filename):
    """Servir archivos de sonido sin Range requests"""
    response = send_from_directory(
        os.path.join(app.root_path, 'static', 'sounds'),
        filename
    )
    # Deshabilitar Range requests
    response.headers['Accept-Ranges'] = 'none'
    return response

@app.route('/')
def index():
    """Página principal - redirigir según el estado de configuración"""
    if verificar_configuracion_inicial():
        return redirect(url_for('login'))
    else:
        return redirect(url_for('configuracion_inicial'))

@app.route('/configuracion-inicial')
def configuracion_inicial():
    """Vista de configuración inicial - solo accesible si no está completada"""
    if verificar_configuracion_inicial():
        return redirect(url_for('login'))
    
    # Obtener los permisos del rol de administrador para filtrar las funcionalidades
    admin_permisos = []
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Buscar el rol de administrador
        cursor.execute("SELECT permisos FROM roles WHERE nombre = 'Administrador' LIMIT 1")
        rol = cursor.fetchone()
        if rol and rol[0]:
            import json
            admin_permisos = json.loads(rol[0])
        conn.close()
    except Exception as e:
        print(f"Error al obtener permisos del rol: {e}")
        admin_permisos = []
    
    return render_template('configuracion_inicial.html', admin_permisos=admin_permisos)

@app.route('/enviar-codigo-verificacion', methods=['POST'])
def enviar_codigo_verificacion():
    """Envía un código de verificación al correo del administrador"""
    try:
        data = request.get_json()
        email = data.get('email', '').strip()
        empresa_nombre = data.get('empresa_nombre', 'Sistema de Gimnasio')
        
        if not email:
            return jsonify({'success': False, 'message': 'El correo electrónico es obligatorio'}), 400
        
        # Validar formato de correo
        if '@' not in email or '.' not in email.split('@')[-1]:
            return jsonify({'success': False, 'message': 'El correo electrónico no tiene un formato válido'}), 400
        
        # Generar código de verificación
        codigo = generar_codigo_verificacion()
        
        # Guardar código en sesión (para verificar después)
        session['codigo_verificacion'] = codigo
        session['email_verificacion'] = email
        session['codigo_expiracion'] = (datetime.now().timestamp() + 600)  # 10 minutos
        
        # Enviar correo
        success, message = enviar_codigo_verificacion_email(email, codigo, empresa_nombre)
        
        if success:
            return jsonify({
                'success': True, 
                'message': f'Código de verificación enviado a {email}',
                'email': email
            })
        else:
            return jsonify({'success': False, 'message': f'Error al enviar correo: {message}'}), 500
            
    except Exception as e:
        print(f"Error en enviar_codigo_verificacion: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/verificar-codigo', methods=['POST'])
def verificar_codigo():
    """Verifica el código de verificación"""
    try:
        data = request.get_json()
        codigo_ingresado = data.get('codigo', '').strip()
        
        # Verificar que existe un código en sesión
        codigo_guardado = session.get('codigo_verificacion')
        email_guardado = session.get('email_verificacion')
        expiracion = session.get('codigo_expiracion')
        
        if not codigo_guardado:
            return jsonify({'success': False, 'message': 'No se ha solicitado un código de verificación'}), 400
        
        # Verificar si expiró
        if datetime.now().timestamp() > expiracion:
            # Limpiar sesión
            session.pop('codigo_verificacion', None)
            session.pop('email_verificacion', None)
            session.pop('codigo_expiracion', None)
            return jsonify({'success': False, 'message': 'El código de verificación ha expirado. Solicite uno nuevo.'}), 400
        
        # Verificar código
        if codigo_ingresado == codigo_guardado:
            # Marcar como verificado
            session['email_verificado'] = True
            return jsonify({
                'success': True, 
                'message': 'Código verificado correctamente',
                'email': email_guardado
            })
        else:
            return jsonify({'success': False, 'message': 'El código de verificación es incorrecto'}), 400
            
    except Exception as e:
        print(f"Error en verificar_codigo: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/guardar-configuracion', methods=['POST'])
def guardar_configuracion():
    """Guardar configuración inicial"""
    try:
        # Verificar que el email fue verificado
        if not session.get('email_verificado'):
            flash('Debe verificar su correo electrónico antes de continuar', 'error')
            return redirect(url_for('configuracion_inicial'))
        
        empresa_nombre = request.form.get('empresa_nombre', '').strip()
        
        # Colores personalizados
        color_sidebar = request.form.get('color_sidebar', '#1a1a1a')
        color_navbar = request.form.get('color_navbar', '#1a1a1a')
        color_fondo = request.form.get('color_fondo', '#000000')
        color_iconos = request.form.get('color_iconos', '#ffffff')
        color_letras = request.form.get('color_letras', '#ffffff')
        color_botones = request.form.get('color_botones', '#2e9d36')
        
        # Datos del administrador
        admin_dni = request.form.get('admin_dni', '').strip()
        admin_nombre = request.form.get('admin_nombre', '').strip()
        admin_telefono = request.form.get('admin_telefono', '').strip()
        admin_email = request.form.get('admin_email', '').strip()
        admin_usuario = request.form.get('admin_usuario', '').strip()
        admin_password = request.form.get('admin_password', '').strip()
        
        errores = []
        
        if not empresa_nombre:
            errores.append("El nombre de la empresa es obligatorio.")
        
        # Validar datos del administrador
        if not admin_dni:
            errores.append("El DNI del administrador es obligatorio.")
        elif len(admin_dni) < 7:
            errores.append("El DNI debe tener al menos 7 dígitos.")
            
        if not admin_nombre:
            errores.append("El nombre del administrador es obligatorio.")
            
        if not admin_usuario:
            errores.append("El usuario del administrador es obligatorio.")
            
        if not admin_password:
            errores.append("La contraseña del administrador es obligatoria.")
        elif len(admin_password) < 4:
            errores.append("La contraseña debe tener al menos 4 caracteres.")
        
        logo_filename = None
        if 'empresa_logo' in request.files:
            file = request.files['empresa_logo']
            if file and file.filename:
                if file.mimetype and file.mimetype.startswith('image/'):
                    filename = secure_filename(file.filename)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    logo_filename = f"logo_{timestamp}_{filename}"
                    file.save(os.path.join(app.config['UPLOAD_FOLDER'], logo_filename))
                else:
                    errores.append("El archivo debe ser una imagen válida.")
            else:
                errores.append("El logo de la empresa es obligatorio.")
        else:
            errores.append("El logo de la empresa es obligatorio.")
        
        if errores:
            for error in errores:
                flash(error, 'error')
            return redirect(url_for('configuracion_inicial'))
        
        funcionalidades_habilitadas = request.form.getlist('funcionalidades_habilitadas')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO configuraciones (
                empresa_nombre, empresa_logo, 
                color_sidebar, color_navbar, color_fondo, 
                color_iconos, color_letras, color_botones,
                funcionalidades_habilitadas, configuracion_completada,
                fecha_modificacion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP)
        ''', (
            empresa_nombre, logo_filename, 
            color_sidebar, color_navbar, color_fondo, 
            color_iconos, color_letras, color_botones,
            json.dumps(funcionalidades_habilitadas)
        ))
        
        cursor.execute('SELECT * FROM configuraciones ORDER BY id DESC LIMIT 1')
        config = cursor.fetchone()
        
        # Crear el usuario administrador
        import bcrypt
        hashed_password = bcrypt.hashpw(admin_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Primero crear el rol de administrador si no existe
        cursor.execute('SELECT id FROM roles WHERE nombre = %s', ('Administrador',))
        rol_admin = cursor.fetchone()
        
        if not rol_admin:
            # Crear rol de administrador con todos los permisos
            permisos_admin = json.dumps([
                'dashboard', 'clientes', 'pagos', 'membresias', 'productos', 
                'ventas', 'reportes', 'personal', 'roles', 'configuracion', 'acceso'
            ])
            
            # Usar función de fecha según tipo de BD
            fecha_hora = (datetime.utcnow() - timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                INSERT INTO roles (nombre, descripcion, permisos, estado, fecha_creacion, fecha_modificacion)
                VALUES (%s, %s, %s, 'activo', %s, %s)
            ''', ('Administrador', 'Usuario con acceso total al sistema', permisos_admin, fecha_hora, fecha_hora))
            rol_admin_id = cursor.lastrowid
        else:
            rol_admin_id = rol_admin['id']
        
        # Insertar el usuario administrador
        cursor.execute('''
            INSERT INTO usuarios (dni, nombre_completo, telefono, email, rol_id, 
                                username, password, estado, fecha_registro, usuario_creador_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'activo', %s, 1)
        ''', (
            admin_dni, admin_nombre, admin_telefono or None, 
            admin_email or None, rol_admin_id, admin_usuario, hashed_password, fecha_hora
        ))
        
        conn.commit()
        conn.close()
        
        # Limpiar sesión de verificación
        session.pop('codigo_verificacion', None)
        session.pop('email_verificacion', None)
        session.pop('codigo_expiracion', None)
        session.pop('email_verificado', None)
        
        # Invalidar caché para que los nuevos datos se carguen
        _invalidar_cache()
        
        # Redirigir al login después de completar la configuración
        return redirect(url_for('login'))
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f'Error al guardar la configuración: {str(e)}', 'error')
        return redirect(url_for('configuracion_inicial'))

# ==========================================
# RUTAS DE SECCIONES
# ==========================================

@app.route('/acceso')
@login_required
def acceso():
    """Control de acceso"""
    return render_template('acceso.html')

@app.route('/actualizar-configuracion', methods=['POST'])
def actualizar_configuracion():
    """Actualizar configuración existente"""
    try:
        empresa_nombre = request.form.get('empresa_nombre', '').strip()
        
        # Colores personalizados
        color_sidebar = request.form.get('color_sidebar', '#1a1a1a')
        color_navbar = request.form.get('color_navbar', '#1a1a1a')
        color_fondo = request.form.get('color_fondo', '#000000')
        color_iconos = request.form.get('color_iconos', '#ffffff')
        color_letras = request.form.get('color_letras', '#ffffff')
        color_botones = request.form.get('color_botones', '#2e9d36')
        
        errores = []
        
        if not empresa_nombre:
            errores.append("El nombre de la empresa es obligatorio.")
        
        logo_filename = request.form.get('logo_actual')
        if 'empresa_logo' in request.files:
            file = request.files['empresa_logo']
            if file and file.filename:
                if file.mimetype and file.mimetype.startswith('image/'):
                    filename = secure_filename(file.filename)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    logo_filename = f"logo_{timestamp}_{filename}"
                    file.save(os.path.join(app.config['UPLOAD_FOLDER'], logo_filename))
                else:
                    errores.append("El archivo debe ser una imagen válida.")
            # Logo opcional - no bloquear si no hay logo
        
        funcionalidades_habilitadas = request.form.getlist('funcionalidades_habilitadas')
        
        if errores:
            for error in errores:
                flash(error, 'error')
            return redirect(url_for('configuraciones'))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Primero obtener el ID más reciente
        cursor.execute('SELECT id FROM configuraciones ORDER BY id DESC LIMIT 1')
        result = cursor.fetchone()
        
        if result:
            config_id = result['id'] if isinstance(result, dict) else result[0]
            
            # Ahora hacer el UPDATE con el ID conocido
            cursor.execute('''
                UPDATE configuraciones SET
                    empresa_nombre=%s, empresa_logo=%s, 
                    color_sidebar=%s, color_navbar=%s, color_fondo=%s,
                    color_iconos=%s, color_letras=%s, color_botones=%s,
                    funcionalidades_habilitadas=%s, fecha_modificacion=CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (
                empresa_nombre, logo_filename, 
                color_sidebar, color_navbar, color_fondo, 
                color_iconos, color_letras, color_botones,
                json.dumps(funcionalidades_habilitadas),
                config_id
            ))
        
        conn.commit()
        conn.close()
        
        flash('Configuración actualizada exitosamente.', 'success')
        # Invalidar caché para reflejar los cambios de inmediato
        _invalidar_cache()
        return redirect(url_for('configuraciones'))
        
    except Exception as e:
        flash(f'Error al actualizar la configuración: {str(e)}', 'error')
        return redirect(url_for('configuraciones'))

# ==========================================
# APIS
# ==========================================

@app.route('/api/obtener-configuracion')
def api_obtener_configuracion():
    """API para obtener configuración actual"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        
        cursor.execute('SELECT * FROM configuraciones ORDER BY id DESC LIMIT 1')
        config = cursor.fetchone()
        
        if config:
            if isinstance(config, dict):
                config_dict = config
            elif is_sqlite():
                config_dict = dict(config)
            else:
                columns = [col[0] for col in cursor.description] if cursor.description else []
                config_dict = dict(zip(columns, config))
            return jsonify(config_dict)
        return jsonify({})
    finally:
        conn.close()

@app.route('/api/consultar-dni/<dni>')
def api_consultar_dni(dni):
    """API para consultar datos de DNI en RENIEC (Perú)"""
    if not dni or len(dni) != 8 or not dni.isdigit():
        return jsonify({
            'success': False,
            'mensaje': 'El DNI debe tener 8 dígitos numéricos'
        }), 400
    
    import requests
    
    API_TOKEN = os.getenv('APIPERU_TOKEN', '')
    
    try:
        api_url = f'https://apiperu.dev/api/dni/{dni}'
        headers = {
            'Authorization': f'Bearer {API_TOKEN}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(api_url, headers=headers, timeout=20)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], dict):
                d = data['data']
                nombre_completo = f"{d.get('nombres', '')} {d.get('apellido_paterno', '')} {d.get('apellido_materno', '')}"
                return jsonify({
                    'success': True,
                    'nombre_completo': nombre_completo.strip(),
                    'dni': dni
                })
            elif 'nombres' in data:
                nombre_completo = f"{data.get('nombres', '')} {data.get('apellido_paterno', '')} {data.get('apellido_materno', '')}"
                return jsonify({
                    'success': True,
                    'nombre_completo': nombre_completo.strip(),
                    'dni': dni
                })
            
            return jsonify({
                'success': False,
                'mensaje': 'DNI no encontrado en RENIEC'
            }), 404
            
        elif response.status_code == 401:
            return jsonify({
                'success': False,
                'mensaje': 'Token inválido. Contacta a soporte de apiperu.dev'
            }), 401
        elif response.status_code == 429:
            return jsonify({
                'success': False,
                'mensaje': 'Límite de consultas alcanzado (100/mes). Actualiza tu plan.'
            }), 429
        else:
            return jsonify({
                'success': False,
                'mensaje': f'Error del servidor ({response.status_code}). Intenta más tarde.'
            }), 500
            
    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'mensaje': 'Timeout: La API está lenta. Intenta en unos segundos.'
        }), 504
    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'mensaje': 'No se puede conectar a apiperu.dev. Verifica tu conexión a internet.'
        }), 503
    except Exception as e:
        return jsonify({
            'success': False,
            'mensaje': f'Error: {str(e)[:50]}'
        }), 500


# ==========================================
# INICIALIZACIÓN DE CONTROLADORES
# ==========================================

def inicializar_controladores():
    """Inicializa todos los controladores"""
    init_auth_controller(app)
    init_dashboard_controller(app)
    init_clientes_controller(app)
    init_productos_controller(app)
    init_personal_controller(app)
    init_pagos_controller(app)
    init_ventas_controller(app)
    init_planes_controller(app)
    init_invitados_controller(app)
    init_acceso_controller(app)
    init_roles_controller(app)
    init_perfil_controller(app)
    init_fotos_controller(app)
    init_notificaciones_controller(app)
    init_password_recovery_controller(app)
    init_reportes_controller(app)


# ==========================================
# ENDPOINT DECODIFICAR QR DESDE IMAGEN (MÓVIL)
# ==========================================
@app.route('/api/decodificar-qr-imagen', methods=['POST'])
@login_required
def decodificar_qr_imagen():
    try:
        from PIL import Image
        from pyzbar.pyzbar import decode
        import io

        if 'imagen' not in request.files:
            return jsonify({'success': False, 'error': 'No se recibió imagen'})

        file = request.files['imagen']
        img = Image.open(io.BytesIO(file.read()))
        
        # Convertir a RGB si es necesario
        if img.mode != 'RGB':
            img = img.convert('RGB')

        codigos = decode(img)
        if codigos:
            texto = codigos[0].data.decode('utf-8')
            return jsonify({'success': True, 'texto': texto})
        else:
            return jsonify({'success': False, 'error': 'No se encontró QR en la imagen'})
    except ImportError:
        return jsonify({'success': False, 'error': 'Instala: pip install pyzbar pillow'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/set-sidebar-state', methods=['POST'])
@login_required
def set_sidebar_state():
    """
    Guarda el estado del sidebar (colapsado/expandido) en la sesión del usuario
    """
    try:
        data = request.get_json()
        if data and 'collapsed' in data:
            # Guardar en la sesión
            session['sidebar_collapsed'] = data['collapsed']
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Datos inválidos'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

        
# ==========================================
# INICIO DE APLICACIÓN
# ==========================================

inicializar_controladores()
if __name__ == '__main__':
    # Inicializar base de datos
    init_db()
    
    # Inicializar controladores
    inicializar_controladores()
    
    # debug=False en producción — nunca exponer el debugger
    _debug = os.getenv('FLASK_ENV', 'development') == 'development'
    _port  = int(os.getenv('PORT', 5000))
    
    # threaded=True: atiende múltiples usuarios simultáneos
    # En producción usar: gunicorn -c gunicorn.conf.py app:app
    app.run(
        debug=_debug,
        host='0.0.0.0',
        port=_port,
        threaded=True
    )