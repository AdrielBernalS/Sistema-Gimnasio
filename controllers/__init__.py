"""
Controladores
Manejan las rutas y lógica de la aplicación.
"""

from flask_mail import Mail, Message

# Nota: La configuración de Flask y Flask-Mail se hace en app.py
# Aquí solo importamos el objeto 'app' que viene del archivo principal

# Inicializar Flask-Mail (se configurará en app.py)
mail = Mail()

from flask import render_template, request, redirect, url_for, jsonify, session, flash, send_file
from functools import wraps
from io import BytesIO
from datetime import datetime, date, timedelta, timezone
from dao import cliente_dao, producto_dao, usuario_dao, pago_dao, venta_dao, acceso_dao, plan_dao, invitado_dao, historial_membresia_dao, notificacion_dao, configuracion_dao, rol_dao, promocion_dao, pareja_promocion_dao
from db_helper import get_db_connection, get_connection, execute_query, is_sqlite, is_mysql, get_current_timestamp_peru, get_current_timestamp_peru_value, get_current_date_peru, get_current_month_expression, get_current_date_expression
import re
import json
import traceback
import hashlib
import calendar
from models import Venta, Cliente, Producto, Usuario, Pago
from controllers.acceso_tablet_controller import init_acceso_tablet_controller

# ==========================================
# FUNCIÓN HELPER PARA TIMESTAMP PERUANO
# ==========================================

def obtener_timestamp_peru():
    """Obtiene la fecha y hora actual en zona horaria de Perú (UTC-5)"""
    peru_tz = timezone(timedelta(hours=-5))
    ahora_peru = datetime.now(timezone.utc).astimezone(peru_tz)
    return ahora_peru.strftime('%Y-%m-%d %H:%M:%S')


# ==========================================
# DECORADORES
# ==========================================

def verificar_configuracion_inicial():
    try:
        from db_helper import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT configuracion_completada FROM configuraciones ORDER BY id DESC LIMIT 1')
        result = cursor.fetchone()
        conn.close()
        
        if result:
            if isinstance(result, dict):
                return result.get('configuracion_completada') == 1
            else:
                return result[0] == 1
        return False
    except Exception as e:
        print(f"Error verificando configuración: {e}")
        return False

def login_required(f):
    """Decorador para verificar que el usuario haya iniciado sesión"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Si la configuración inicial no está completada, siempre redirigir
        if not verificar_configuracion_inicial():
            return redirect(url_for('configuracion_inicial'))
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """Decorador para verificar que sea administrador"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            # Para solicitudes AJAX, retornar JSON con error de autenticación
            if request.is_json:
                return jsonify({'success': False, 'message': 'Sesión expirada. Por favor, inicia sesión nuevamente.', 'redirect': '/login'}), 401
            return redirect(url_for('login'))
        
        rol = (session.get('rol') or '').lower()
        if rol != 'administrador':
            # Para solicitudes AJAX, retornar JSON con error de autorización
            if request.is_json:
                return jsonify({'success': False, 'message': 'No tienes permiso para realizar esta acción', 'redirect': '/dashboard'}), 403
            return redirect(url_for('dashboard'))
        
        return f(*args, **kwargs)
    return decorated_function


# ==========================================
# CONTROLADOR: AUTENTICACIÓN
# ==========================================

def init_auth_controller(app):
    """Inicializa las rutas de autenticación"""
    
    @app.route('/login', methods=['GET', 'POST'])
    def login():
        """Controlador de login con bloqueo progresivo por intentos fallidos"""
        
        if request.method == 'POST':
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '')
            ip_address = request.remote_addr
            
            # === VERIFICAR SI ESTÁ BLOQUEADO ===
            bloqueado, tiempo_restante, intentos_totales = verificar_bloqueo(username, ip_address)
            
            if bloqueado:
                # Obtener configuración usando el DAO
                config = configuracion_dao.obtener_actual()
                
                # Formatear tiempo de bloqueo
                if tiempo_restante >= 60:
                    minutos = tiempo_restante // 60
                    segundos = tiempo_restante % 60
                    if segundos > 0:
                        mensaje_tiempo = f"{minutos} minutos y {segundos} segundos"
                    else:
                        mensaje_tiempo = f"{minutos} minutos"
                else:
                    mensaje_tiempo = f"{tiempo_restante} segundos"
                
                return render_template('login.html', 
                                config=config, 
                                error_message=f"Demasiados intentos fallidos ({intentos_totales} intentos). Espera {mensaje_tiempo} antes de intentar nuevamente.",
                                username=username,
                                tiempo_restante=tiempo_restante)
            
            # Verificar en base de datos
            usuario = usuario_dao.verificar_credenciales(username, password)
            
            if usuario:
                # CREDENCIALES CORRECTAS
                # Resetear intentos fallidos
                resetear_intentos(username, ip_address)
                
                # Establecer sesión
                session['usuario_id'] = usuario['id']
                session['usuario'] = usuario['nombre_completo']
                session['username'] = usuario['username']
                session['rol'] = usuario.get('rol', 'Usuario')
                session['logged_in'] = True
                session['rol_id'] = usuario.get('rol_id')

                # Actualizar último login
                try:
                    usuario_dao.actualizar_ultimo_login(usuario['id'])
                except Exception as e:
                    print(f"Error al actualizar último login: {e}")
                
                # OBTENER PERMISOS DEL ROL USANDO EL ROL DAO
                try:
                    if usuario.get('rol_id'):
                        permisos = rol_dao.obtener_permisos_por_rol(usuario['rol_id'])
                        session['permisos'] = permisos
                    else:
                        session['permisos'] = []
                except Exception as e:
                    print(f"Error al obtener permisos: {e}")
                    session['permisos'] = []
                
                return redirect(url_for('dashboard'))
            else:
                # CREDENCIALES INCORRECTAS
                # Registrar intento fallido
                registrar_intento_fallido(username, ip_address)
                
                # Verificar si ahora está bloqueado después de este intento
                bloqueado, tiempo_restante, intentos_totales = verificar_bloqueo(username, ip_address)
                
                # Obtener configuración usando el DAO
                config = configuracion_dao.obtener_actual()
                
                if bloqueado:
                    # Formatear tiempo de bloqueo
                    if tiempo_restante >= 60:
                        minutos = tiempo_restante // 60
                        segundos = tiempo_restante % 60
                        if segundos > 0:
                            mensaje_tiempo = f"{minutos} minutos y {segundos} segundos"
                        else:
                            mensaje_tiempo = f"{minutos} minutos"
                    else:
                        mensaje_tiempo = f"{tiempo_restante} segundos"
                        
                    error_msg = f"Demasiados intentos fallidos ({intentos_totales} intentos). Espera {mensaje_tiempo} antes de intentar nuevamente."
                else:
                    # Obtener número de intentos restantes antes del próximo bloqueo
                    intentos_restantes = obtener_intentos_restantes(username, ip_address)
                    
                    # Obtener intentos totales
                    intentos_totales = obtener_intentos_totales(username, ip_address)
                    
                    if intentos_restantes > 0:
                        error_msg = f"Usuario o contraseña incorrectos. Intento {intentos_totales}. Te quedan {intentos_restantes} intentos antes del bloqueo."
                    else:
                        error_msg = f"Usuario o contraseña incorrectos. Intento {intentos_totales}. Estás a punto de ser bloqueado."
                
                return render_template('login.html', 
                                    config=config, 
                                    error_message=error_msg,
                                    username=username)
        
        # GET request - mostrar formulario
        # Si ya está logueado, redirigir al dashboard
        if session.get('logged_in'):
            return redirect(url_for('dashboard'))
        # Si la configuración inicial no está completada, redirigir
        if not verificar_configuracion_inicial():
            return redirect(url_for('configuracion_inicial'))
        config = configuracion_dao.obtener_actual()
        
        return render_template('login.html', config=config)
    
    @app.route('/logout')
    def logout():
        """Cerrar sesión"""
        session.clear()
        return redirect(url_for('login'))

# ==========================================
# FUNCIONES DE BLOQUEO POR INTENTOS FALLIDOS (VERSIÓN CORREGIDA)
# ==========================================

def get_connection():
    """Obtiene conexión usando MySQL"""
    from db_helper import get_db_connection
    return get_db_connection()

def registrar_intento_fallido(username, ip_address):
    """Registra un intento fallido de login"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Obtener timestamp en hora peruana
    timestamp_peru = obtener_timestamp_peru()
    
    # MySQL
    cursor.execute('''
        SELECT id, intentos, bloqueado_hasta FROM intentos_login 
        WHERE (username = %s OR ip_address = %s)
        ORDER BY ultimo_intento DESC
        LIMIT 1
    ''', (username, ip_address))
    
    registro = cursor.fetchone()
    
    if registro:
        # El cursor puede devolver dict o tupla según el conector
        if isinstance(registro, dict):
            intento_id = registro['id']
            intentos_actuales = int(registro['intentos'])
            bloqueado_hasta_str = registro['bloqueado_hasta']
        else:
            intento_id, intentos_actuales, bloqueado_hasta_str = registro
            intentos_actuales = int(intentos_actuales)
        
        # Si ya está bloqueado, no incrementar intentos
        if bloqueado_hasta_str:
            try:
                bloqueado_hasta = datetime.strptime(str(bloqueado_hasta_str), '%Y-%m-%d %H:%M:%S')
                if bloqueado_hasta > datetime.now():
                    conn.close()
                    return
            except:
                pass
        
        # Incrementar intentos
        nuevos_intentos = intentos_actuales + 1
        
        # Verificar si se alcanzó un múltiplo de 5 para bloqueo
        if nuevos_intentos % 5 == 0:
            nivel_bloqueo = nuevos_intentos // 5
            segundos_bloqueo = 30 * (2 ** (nivel_bloqueo - 1))
            if segundos_bloqueo > 3600:
                segundos_bloqueo = 3600
            bloqueado_hasta = datetime.now() + timedelta(seconds=segundos_bloqueo)
            bloqueado_hasta_str = bloqueado_hasta.strftime('%Y-%m-%d %H:%M:%S')
        else:
            bloqueado_hasta_str = None
        
        cursor.execute('''
            UPDATE intentos_login 
            SET intentos = %s, 
                bloqueado_hasta = %s,
                ultimo_intento = %s
            WHERE id = %s
        ''', (nuevos_intentos, bloqueado_hasta_str, timestamp_peru, intento_id))
    else:
        # Primer intento fallido
        cursor.execute('''
            INSERT INTO intentos_login (ip_address, username, intentos, ultimo_intento)
            VALUES (%s, %s, 1, %s)
        ''', (ip_address, username, timestamp_peru))
    
    conn.commit()
    conn.close()

def verificar_bloqueo(username, ip_address):
    """Verifica si el usuario/IP está bloqueado"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT bloqueado_hasta, intentos FROM intentos_login 
        WHERE (username = %s OR ip_address = %s)
        ORDER BY ultimo_intento DESC
        LIMIT 1
    ''', (username, ip_address))
    
    resultado = cursor.fetchone()
    conn.close()
    
    if resultado:
        if isinstance(resultado, dict):
            bloqueado_hasta_str = resultado['bloqueado_hasta']
            intentos = int(resultado['intentos'])
        else:
            bloqueado_hasta_str, intentos = resultado
            intentos = int(intentos)
        
        if bloqueado_hasta_str:
            try:
                bloqueado_hasta = datetime.strptime(str(bloqueado_hasta_str), '%Y-%m-%d %H:%M:%S')
                ahora = datetime.now()
                
                if bloqueado_hasta > ahora:
                    tiempo_restante = int((bloqueado_hasta - ahora).total_seconds())
                    return True, tiempo_restante, intentos
            except:
                pass
    
    return False, 0, 0

def resetear_intentos(username, ip_address):
    """Resetea los intentos fallidos cuando el login es exitoso"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        DELETE FROM intentos_login 
        WHERE username = %s OR ip_address = %s
    ''', (username, ip_address))
    
    conn.commit()
    conn.close()

def obtener_intentos_restantes(username, ip_address):
    """Obtiene cuántos intentos restantes tiene antes del próximo bloqueo"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT intentos, bloqueado_hasta FROM intentos_login 
        WHERE (username = %s OR ip_address = %s)
        ORDER BY ultimo_intento DESC
        LIMIT 1
    ''', (username, ip_address))
    
    resultado = cursor.fetchone()
    conn.close()
    
    if resultado:
        if isinstance(resultado, dict):
            intentos_actuales = int(resultado['intentos'])
            bloqueado_hasta_str = resultado['bloqueado_hasta']
        else:
            intentos_actuales, bloqueado_hasta_str = resultado
            intentos_actuales = int(intentos_actuales)
        
        # Si ya está bloqueado, devolver 0 intentos restantes
        if bloqueado_hasta_str:
            try:
                bloqueado_hasta = datetime.strptime(str(bloqueado_hasta_str), '%Y-%m-%d %H:%M:%S')
                if bloqueado_hasta > datetime.now():
                    return 0
            except:
                pass
        
        # Calcular intentos restantes antes del próximo bloqueo
        proximo_bloqueo = ((intentos_actuales // 5) + 1) * 5
        intentos_restantes = proximo_bloqueo - intentos_actuales
        
        return intentos_restantes if intentos_restantes > 0 else 0
    
    return 5  # Si no hay registro, tiene todos los intentos disponibles

def obtener_intentos_totales(username, ip_address):
    """Obtiene el número total de intentos fallidos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT COALESCE(SUM(intentos), 0) FROM intentos_login 
        WHERE (username = %s OR ip_address = %s)
    ''', (username, ip_address))
    
    total = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone()) or 0
    conn.close()
    
    return total

def limpiar_intentos_antiguos():
    """Limpia registros antiguos de intentos (ejecutar periódicamente)"""
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Obtener timestamp en hora peruana para comparación
    timestamp_peru = obtener_timestamp_peru()
    # Calcular fecha límite (24 horas atrás)
    hace_24_horas = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
    
    # Eliminar registros con más de 24 horas sin actividad
    cursor.execute('''
        DELETE FROM intentos_login 
        WHERE ultimo_intento < %s
        AND bloqueado_hasta IS NULL
    ''', (hace_24_horas,))
    
    # Eliminar registros bloqueados que ya expiraron (más de 2 horas)
    hace_2_horas = (datetime.now() - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute('''
        DELETE FROM intentos_login 
        WHERE bloqueado_hasta < %s
    ''', (hace_2_horas,))
    
    conn.commit()
    conn.close()
# ==========================================
# CONTROLADOR: DASHBOARD
# ==========================================

def init_dashboard_controller(app):
    """Inicializa las rutas del dashboard"""
    
    @app.route('/dashboard')
    @login_required
    @permiso_required('dashboard')  # Todos los roles deberían tener dashboard
    def dashboard():
        """Dashboard con estadísticas reales"""
        
        # Estadísticas de clientes
        total_clientes = cliente_dao.contar_por_estado()
        stats_dashboard = cliente_dao.obtener_estadisticas_dashboard()
        clientes_activos = len(stats_dashboard.get('clientes_pagado_ids', []))
        clientes_pendientes = stats_dashboard.get('clientes_pendientes', 0)
        clientes_morosos = stats_dashboard.get('clientes_morosos', 0)
        
        # Estadísticas de productos
        productos = producto_dao.obtener_todos()
        productos_bajo_stock = producto_dao.obtener_con_stock_bajo()
        total_inventario = producto_dao.obtener_total_inventario()
        
        # Ingresos del mes actual
        ingresos_mes = pago_dao.obtener_total_mes()
        
        # Productos más vendidos (simulado por ahora)
        productos_mas_vendidos = [
            {'nombre': 'Proteína Whey', 'total_vendido': 23, 'total_ingresos': 1955.00},
            {'nombre': 'Pre-entreno', 'total_vendido': 18, 'total_ingresos': 1440.00},
            {'nombre': 'Creatina', 'total_vendido': 15, 'total_ingresos': 900.00}
        ]
        
        # Actividades recientes (simulado)
        actividades_recientes = [
            {
                'tipo': 'success',
                'icono': 'fa-user-plus',
                'titulo': 'Nuevo cliente registrado',
                'descripcion': 'Juan Pérez se unió al gimnasio',
                'hace': '5 min'
            },
            {
                'tipo': 'primary',
                'icono': 'fa-credit-card',
                'titulo': 'Pago recibido',
                'descripcion': 'María García - Plan Mensual',
                'hace': '15 min'
            },
            {
                'tipo': 'warning',
                'icono': 'fa-exclamation-triangle',
                'titulo': 'Membresía por vencer',
                'descripcion': 'Carlos López - Vence en 2 días',
                'hace': '1 hora'
            }
        ]
        
        # Clientes del día (simulado)
        clientes_hoy = [
            {'iniciales': 'JD', 'nombre': 'Juan Pérez', 'plan': 'Plan Mensual', 'hora': '08:30'},
            {'iniciales': 'MG', 'nombre': 'María García', 'plan': 'Plan Interdiario', 'hora': '09:15'},
            {'iniciales': 'CL', 'nombre': 'Carlos López', 'plan': 'Plan Diaria', 'hora': '10:00'}
        ]
        
        # Datos para gráficos
        ingresos_semana = [
            {'dia': 'Lun', 'monto': 1200},
            {'dia': 'Mar', 'monto': 1900},
            {'dia': 'Mié', 'monto': 1500},
            {'dia': 'Jue', 'monto': 2100},
            {'dia': 'Vie', 'monto': 1800},
            {'dia': 'Sáb', 'monto': 2400},
            {'dia': 'Dom', 'monto': 800}
        ]
        
        planes_distribucion = [
            {'nombre': 'Plan A', 'cantidad': 15},
            {'nombre': 'Plan B', 'cantidad': 35},
            {'nombre': 'Plan C', 'cantidad': 40},
            {'nombre': 'Plan D', 'cantidad': 10}
        ]
        
        clientes_dia_semana = [
            {'dia': 'Lun', 'cantidad': 45},
            {'dia': 'Mar', 'cantidad': 52},
            {'dia': 'Mié', 'cantidad': 48},
            {'dia': 'Jue', 'cantidad': 61},
            {'dia': 'Vie', 'cantidad': 55},
            {'dia': 'Sáb', 'cantidad': 72},
            {'dia': 'Dom', 'cantidad': 38}
        ]
        
        return render_template(
            'dashboard.html',
            total_clientes=total_clientes,
            clientes_activos=clientes_activos,
            clientes_pendientes=clientes_pendientes,
            clientes_morosos=clientes_morosos,
            total_productos=len(productos),
            productos_bajo_stock=len(productos_bajo_stock),
            total_inventario=total_inventario,
            ingresos_mes=ingresos_mes,
            productos_mas_vendidos=productos_mas_vendidos,
            actividades_recientes=actividades_recientes,
            clientes_hoy=clientes_hoy,
            ingresos_semana=ingresos_semana,
            planes_distribucion=planes_distribucion,
            clientes_dia_semana=clientes_dia_semana
        )
    
    @app.route('/api/dashboard/stats')
    @login_required
    def api_dashboard_stats():
        try:
            from dao.configuracion_dao import ConfiguracionDAO
            config_dao = ConfiguracionDAO()
            config = config_dao.obtener_actual()
            
            funcionalidades = []
            if config and config.get('funcionalidades_habilitadas'):
                try:
                    funcionalidades = json.loads(config['funcionalidades_habilitadas'])
                except:
                    funcionalidades = []
            
            total_clientes = cliente_dao.contar_por_estado()
            ingresos_mes = pago_dao.obtener_total_mes(funcionalidades=funcionalidades)
            stats = cliente_dao.obtener_estadisticas_dashboard()
            clientes_activos = len(stats.get('clientes_pagado_ids', []))
            
            return jsonify({
                'success': True,
                'data': {
                    'total_clientes': total_clientes,
                    'clientes_activos': clientes_activos,
                    'ingresos_mes': float(ingresos_mes) if ingresos_mes else 0,
                    'total_pendiente': stats['total_pendiente'],
                    'clientes_morosos': stats['clientes_morosos']
                }
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
        
    @app.route('/api/dashboard/actividades')
    @login_required
    def api_dashboard_actividades():
        """API para obtener actividades. ?todos=1 para historial completo, por defecto solo hoy."""
        
        try:
            actividades = []
            modo_todos = request.args.get('todos', '0') == '1'

            def normalizar_fecha(f):
                """Convierte cualquier formato de fecha a objeto datetime"""
                # Si ya es datetime (MySQL devuelve datetime directamente), retornarlo
                if isinstance(f, datetime):
                    return f
                
                # Si es date (fecha sin hora), convertir a datetime
                if isinstance(f, date):
                    return datetime.combine(f, datetime.min.time())
                
                # Si es None o vacío
                if f is None or (isinstance(f, str) and not f.strip()):
                    return None
                
                # Si es string, intentar parsear
                if isinstance(f, str):
                    f = f.strip()
                    # Intentar varios formatos comunes
                    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S',
                                '%Y-%m-%d', '%d/%m/%Y', '%d/%m/%Y %H:%M:%S'):
                        try:
                            return datetime.strptime(f, fmt)
                        except:
                            continue
                
                return None

            def formatear_hace(fecha_dt):
                """Para modo todos, muestra fecha+hora legible en vez de 'hace X'"""
                if fecha_dt is None:  # Manejar fecha nula
                    return '—'
                if modo_todos:
                    if fecha_dt == datetime.min:
                        return '—'
                    hoy = datetime.now().date()
                    ayer = hoy - timedelta(days=1)
                    # Verificar si la hora es 00:00 (fecha sin hora)
                    es_sin_hora = fecha_dt.hour == 0 and fecha_dt.minute == 0 and fecha_dt.second == 0
                    if fecha_dt.date() == hoy:
                        if es_sin_hora:
                            return "Hoy"
                        return f"Hoy {fecha_dt.strftime('%H:%M')}"
                    elif fecha_dt.date() == ayer:
                        if es_sin_hora:
                            return "Ayer"
                        return f"Ayer {fecha_dt.strftime('%H:%M')}"
                    else:
                        if es_sin_hora:
                            return fecha_dt.strftime('%d/%m/%Y')
                        return fecha_dt.strftime('%d/%m/%Y %H:%M')
                return calcular_hace(fecha_dt)

            conn = get_connection()
            cursor = conn.cursor()

            if modo_todos:
                # Historial completo sin límite de fecha
                cursor.execute('''
                    SELECT pa.*, c.nombre_completo, p.nombre as plan_nombre
                    FROM pagos pa
                    JOIN clientes c ON pa.cliente_id = c.id
                    JOIN planes_membresia p ON pa.plan_id = p.id
                    ORDER BY pa.fecha_pago DESC
                    LIMIT 100
                ''')
            else:
                # Solo últimas 24 horas
                hace_24h = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute('''
                    SELECT pa.*, c.nombre_completo, p.nombre as plan_nombre
                    FROM pagos pa
                    JOIN clientes c ON pa.cliente_id = c.id
                    JOIN planes_membresia p ON pa.plan_id = p.id
                    WHERE pa.fecha_pago >= %s
                    ORDER BY pa.fecha_pago DESC
                    LIMIT 10
                ''', (hace_24h,))

            pagos = cursor.fetchall()
            conn.close()

            for pago in pagos:
                fecha_dt = normalizar_fecha(pago['fecha_pago'])
                actividades.append({
                    'tipo': 'success',
                    'icono': 'fa-credit-card',
                    'titulo': 'Pago recibido',
                    'descripcion': f"{pago['nombre_completo']} - {pago['plan_nombre']}",
                    'monto': float(pago['monto']),
                    'hace': formatear_hace(fecha_dt),
                    '_fecha_dt': fecha_dt
                })

            # Accesos
            accesos = acceso_dao.obtener_todos()
            limite_accesos = len(accesos) if modo_todos else 5
            for acceso in accesos[:limite_accesos]:
                fecha_dt = normalizar_fecha(acceso['fecha_hora_entrada'])
                # En modo normal solo mostrar accesos de hoy
                if not modo_todos:
                    if fecha_dt is None:  # Skip fechas nulas
                        continue
                    hace_24h_dt = datetime.now() - timedelta(hours=24)
                    if fecha_dt < hace_24h_dt:
                        continue
                actividades.append({
                    'tipo': 'primary',
                    'icono': 'fa-door-open',
                    'titulo': 'Entrada registrada',
                    'descripcion': acceso.get('cliente_nombre', 'Invitado'),
                    'hace': formatear_hace(fecha_dt),
                    '_fecha_dt': fecha_dt
                })

            # Clientes nuevos
            conn = get_connection()
            cursor = conn.cursor()
            if modo_todos:
                cursor.execute('''
                    SELECT * FROM clientes
                    ORDER BY fecha_registro DESC
                    LIMIT 50
                ''')
            else:
                hace_24h = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute('''
                    SELECT * FROM clientes
                    WHERE fecha_registro >= %s
                    ORDER BY fecha_registro DESC
                    LIMIT 5
                ''', (hace_24h,))
            nuevos_clientes = [serializar_row(r) for r in cursor.fetchall()]
            conn.close()

            for cliente in nuevos_clientes:
                fecha_dt = normalizar_fecha(cliente['fecha_registro'])
                actividades.append({
                    'tipo': 'info',
                    'icono': 'fa-user-plus',
                    'titulo': 'Nuevo cliente',
                    'descripcion': cliente['nombre_completo'],
                    'hace': formatear_hace(fecha_dt),
                    '_fecha_dt': fecha_dt
                })

            # Ordenar más recientes primero (filtrar None primero)
            actividades = [a for a in actividades if a.get('_fecha_dt') is not None]
            actividades.sort(key=lambda x: x['_fecha_dt'], reverse=True)
            for a in actividades:
                a.pop('_fecha_dt', None)

            return jsonify({'success': True, 'data': actividades})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
    
    @app.route('/api/dashboard/productos-mas-vendidos')
    @login_required
    def api_dashboard_productos_mas_vendidos():
        """API para obtener productos más vendidos"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Obtener productos más vendidos
            cursor.execute('''
                SELECT 
                    p.nombre as producto_nombre,
                    SUM(dv.cantidad) as total_vendido,
                    SUM(dv.subtotal) as total_ingresos
                FROM detalle_ventas dv
                JOIN productos p ON dv.producto_id = p.id
                JOIN ventas v ON dv.venta_id = v.id
                WHERE v.estado = 'completado'
                GROUP BY p.id
                ORDER BY total_vendido DESC
                LIMIT 10
            ''')
            
            productos = []
            for row in cursor.fetchall():
                productos.append({
                    'nombre': row['producto_nombre'],
                    'total_vendido': row['total_vendido'],
                    'total_ingresos': float(row['total_ingresos']) if row['total_ingresos'] else 0
                })
            
            conn.close()
            
            return jsonify({'success': True, 'data': productos})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
    
    @app.route('/api/dashboard/clientes-hoy')
    @login_required
    def api_dashboard_clientes_hoy():
        """API para obtener clientes del día (accesos hoy)"""
        try:
            # Obtener accesos de hoy
            accesos_hoy = acceso_dao.obtener_hoy()
            
            clientes = []
            seen = set()
            
            for acceso in accesos_hoy:
                # Evitar duplicados
                key = acceso.get('cliente_id') or acceso.get('dni')
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                
                # Formatear hora
                fecha_hora = acceso.get('fecha_hora_entrada', '')
                hora = ''
                if fecha_hora:
                    try:
                        from datetime import datetime, date as dt
                        if isinstance(fecha_hora, dt):
                            hora = fecha_hora.strftime('%H:%M')
                        elif isinstance(fecha_hora, str):
                            if ' ' in fecha_hora:
                                hora = fecha_hora.split(' ')[1][:5]
                            elif 'T' in fecha_hora:
                                hora = fecha_hora.split('T')[1][:5]
                            else:
                                hora = fecha_hora[:5]
                    except:
                        pass
                
                nombre = acceso.get('cliente_nombre', '') or 'Invitado'
                iniciales = ''
                if nombre and nombre != 'Invitado':
                    partes = nombre.split()
                    iniciales = ''.join([p[0] for p in partes[:2]]).upper()
                
                clientes.append({
                    'iniciales': iniciales or 'IN',
                    'nombre': nombre,
                    'plan': acceso.get('tipo', 'Visitante'),
                    'hora': hora if hora else '--'
                })
            
            return jsonify({'success': True, 'data': clientes})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
    
    @app.route('/api/dashboard/graficos/ingresos')
    @login_required
    def api_dashboard_graficos_ingresos():
        """API para obtener datos de gráficos de ingresos"""
        
        try:
            dias = int(request.args.get('dias', 7))
            hoy = datetime.now()
            fecha_inicio = (hoy - timedelta(days=dias)).strftime('%Y-%m-%d')

            # OPTIMIZADO: 1 query con GROUP BY en lugar de N queries en loop
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute('''
                SELECT DATE(fecha_pago) as dia, COALESCE(SUM(monto), 0) as total
                FROM pagos
                WHERE estado = 'completado' AND DATE(fecha_pago) >= %s
                GROUP BY DATE(fecha_pago)
            ''', (fecha_inicio,))
            pagos_raw = {str(r['dia']): float(r['total']) for r in cursor.fetchall()}

            cursor.execute('''
                SELECT DATE(fecha_venta) as dia, COALESCE(SUM(total), 0) as total
                FROM ventas
                WHERE estado = 'completado' AND DATE(fecha_venta) >= %s
                GROUP BY DATE(fecha_venta)
            ''', (fecha_inicio,))
            ventas_raw = {str(r['dia']): float(r['total']) for r in cursor.fetchall()}
            conn.close()

            # Construir listas con todos los días (incluyendo días sin datos = 0)
            ingresos_pagos = []
            ingresos_ventas = []
            for i in range(dias):
                fecha_str = (hoy - timedelta(days=i)).strftime('%Y-%m-%d')
                ingresos_pagos.append({'dia': fecha_str, 'monto': pagos_raw.get(fecha_str, 0)})
                ingresos_ventas.append({'dia': fecha_str, 'monto': ventas_raw.get(fecha_str, 0)})
            
            # Combinar ingresos
            ingresos_combinados = []
            for i in range(dias):
                fecha = (hoy - timedelta(days=dias-1-i)).strftime('%Y-%m-%d')
                pago = next((x['monto'] for x in ingresos_pagos if x['dia'] == fecha), 0)
                venta = next((x['monto'] for x in ingresos_ventas if x['dia'] == fecha), 0)
                ingresos_combinados.append({
                    'dia': obtener_nombre_dia(fecha),
                    'monto': pago + venta
                })
            
            return jsonify({
                'success': True, 
                'data': {
                    'pagos': ingresos_pagos,
                    'ventas': ingresos_ventas,
                    'combinado': ingresos_combinados
                }
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500


# ==========================================
# FUNCIONES AUXILIARES PARA CALCULAR ESTADO DE MEMBRESÍAS
# ==========================================

def calcular_estado_membresia(historial_id, cliente_id, fecha_inicio, fecha_fin, estado_original, pagos, es_ultimo=True, es_previo_a_cambio_plan=False, tiene_pendiente=False):
    """
    Calcula el estado correcto de una membresía basándose en los pagos y fechas.
    Lógica alineada con obtener_clientes_para_pagos_optimizado en cliente_dao:
        Pagado   = tiene completado Y NO tiene pendiente (sin filtro de mes)
        Pendiente = tiene_pendiente explícito O estado_original='pendiente'
        Vencido  = sin pagos, sin pendiente, fecha vencida

    Args:
        historial_id: ID del registro en historial_membresia
        cliente_id: ID del cliente
        fecha_inicio: Fecha de inicio de la membresía
        fecha_fin: Fecha de fin de la membresía
        estado_original: Estado original de la tabla historial_membresia
        pagos: Lista de pagos COMPLETADOS del cliente (solo_completados=True)
        es_ultimo: True si es el registro más reciente (membresía actual)
        es_previo_a_cambio_plan: True si el registro siguiente es un cambio de plan
        tiene_pendiente: True si el cliente tiene algún pago con estado='pendiente' en la BD
    
    Returns:
        tuple: (estado_calculado, metodo_pago)
    """
    
    try:
        # Obtener fecha actual
        hoy = datetime.now().date()
        
        # Parsear fecha de fin (acepta datetime/date de MySQL o string)
        fecha_fin_date = None
        if fecha_fin:
            try:
                if hasattr(fecha_fin, 'date'):      # datetime object
                    fecha_fin_date = fecha_fin.date()
                elif isinstance(fecha_fin, date):  # date object
                    fecha_fin_date = fecha_fin
                elif isinstance(fecha_fin, str):
                    fecha_fin_date = datetime.strptime(fecha_fin.split(' ')[0], '%Y-%m-%d').date()
            except:
                pass
        
        # ── Obtener el método del pago completado más reciente ──
        metodo_pago_real = 'Efectivo'
        if pagos and len(pagos) > 0:
            m = pagos[0].get('metodo_pago', 'Efectivo')
            metodo_pago_real = m.capitalize() if m and m.lower() not in ('pendiente', '') else 'Efectivo'

        # ── Si NO es el último registro (historial anterior) ──
        # "Terminado" solo si es el registro inmediatamente anterior a un CAMBIO DE PLAN.
        # Cualquier otro registro anterior = "Pagado".
        if not es_ultimo:
            if es_previo_a_cambio_plan:
                return 'Terminado', metodo_pago_real
            else:
                return 'Pagado', metodo_pago_real

        # ── Si es el último registro (membresía actual) ──

        # PRIORIDAD 0: Tiene pago pendiente explícito en la BD → Pendiente
        # Cubre "aumentar-meses" y cualquier pago registrado como pendiente.
        if tiene_pendiente or (estado_original and estado_original.lower() == 'pendiente'):
            return 'Pendiente', 'Pendiente'

        # PRIORIDAD 1: Tiene pagos completados Y no tiene pendiente → Pagado
        # No importa si el pago fue este mes o meses anteriores.
        if pagos and len(pagos) > 0:
            ultimo_pago = pagos[0]
            metodo = ultimo_pago.get('metodo_pago', 'Efectivo')
            metodo = metodo.capitalize() if metodo else 'Efectivo'
            return 'Pagado', metodo

        # PRIORIDAD 2: Sin completados ni pendientes → Vencido o Pendiente por fechas
        if fecha_fin_date and fecha_fin_date < hoy:
            return 'Vencido', 'Efectivo'

        if fecha_fin_date and fecha_fin_date >= hoy:
            return 'Pendiente', 'Efectivo'

        # Por defecto
        return estado_original if estado_original else 'Pendiente', 'Efectivo'
        
    except Exception as e:
        print(f"Error calculating estado membresia: {e}")
        traceback.print_exc()
        return estado_original if estado_original else 'Pendiente', 'Efectivo'


# ==========================================
# CONTROLADOR: CLIENTES
# ==========================================

def init_clientes_controller(app):
    """Inicializa las rutas de clientes"""
    
    @app.route('/clientes')
    @login_required
    @permiso_required('clientes')  # Solo roles con permiso 'clientes'

    def clientes():
        """Lista de clientes"""
        clientes = cliente_dao.obtener_todos()
        return render_template('clientes.html', clientes=clientes)
    
    @app.route('/api/clientes')
    @login_required
    @permiso_required('clientes')
    def api_listar_clientes():
        """API para listar clientes"""
        query = request.args.get('q', '')
        if query:
            clientes = cliente_dao.buscar(query)
        else:
            clientes = cliente_dao.obtener_todos()
        
        # Convertir fechas a string antes de enviar
        for cliente in clientes:
            if 'fecha_registro' in cliente and cliente['fecha_registro']:
                # Si es datetime object, convertir a string YYYY-MM-DD
                if hasattr(cliente['fecha_registro'], 'strftime'):
                    cliente['fecha_registro'] = cliente['fecha_registro'].strftime('%Y-%m-%d')
                # Si ya es string, asegurar que solo tenga la fecha
                elif isinstance(cliente['fecha_registro'], str) and ' ' in cliente['fecha_registro']:
                    cliente['fecha_registro'] = cliente['fecha_registro'].split(' ')[0]
        
        return jsonify({'success': True, 'data': clientes})
    
    @app.route('/api/clientes', methods=['POST'])
    @login_required
    def api_crear_cliente():
        """API para crear cliente"""
        try:
            data = request.get_json()
            data['usuario_id'] = session.get('usuario_id', 1)  # Default a 1 si no hay sesión
            cliente_id = cliente_dao.crear_from_dict(data)
            
            # === REGISTRAR EN HISTORIAL DE MEMBRESÍAS ===
            # Obtener información del cliente y plan para guardar en historial
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if cliente and cliente.get('plan_id'):
                plan = plan_dao.obtener_por_id(cliente['plan_id'])
                if plan:
                    historial_data = {
                        'cliente_id': cliente_id,
                        'plan_id': cliente['plan_id'],
                        'fecha_inicio': cliente.get('fecha_inicio'),
                        'fecha_fin': cliente.get('fecha_vencimiento'),
                        'monto_pagado': plan.get('precio', 0),
                        'metodo_pago': None,
                        'estado': 'pendiente',
                        'observaciones': 'Nueva membresía registrada',
                        'usuario_id': session.get('usuario_id', 1)
                    }
                    historial_membresia_dao.crear_from_dict(historial_data)

                # === GENERAR NOTIFICACIÓN DE NUEVO CLIENTE ===
                notificacion_dao.crear_notificacion(
                    tipo='client',
                    titulo='Nuevo cliente registrado',
                    mensaje=f'{data.get("nombre_completo", "Nuevo cliente")} se registró como nuevo miembro',
                    cliente_id=cliente_id,
                    usuario_id=session.get('usuario_id', 1)
                )
                _invalidar_cache_notif()  # forzar recarga inmediata en el próximo fetch
            
            return jsonify({
                'success': True,
                'message': 'Cliente creado exitosamente',
                'cliente_id': cliente_id
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 400
    
    @app.route('/api/clientes/<int:cliente_id>', methods=['GET'])
    @login_required
    def api_obtener_cliente(cliente_id):
        """API para obtener datos de un cliente"""
        try:
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if cliente:
                return jsonify({'success': True, 'data': cliente})
            else:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        
    @app.route('/api/clientes/<int:cliente_id>', methods=['PUT'])
    @login_required
    def api_actualizar_cliente(cliente_id):
        """API para actualizar cliente"""
        try:
            data = request.get_json()
            if 'usuario_id' not in data:
                data['usuario_id'] = session.get('usuario_id', 1)
            cliente_dao.actualizar(cliente_id, data)
            return jsonify({'success': True, 'message': 'Cliente actualizado'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/clientes/<int:cliente_id>', methods=['DELETE'])
    @login_required
    def api_eliminar_cliente(cliente_id):
        """API para eliminar cliente"""
        try:
            cliente_dao.eliminar(cliente_id)
            return jsonify({'success': True, 'message': 'Cliente eliminado'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/clientes/<int:cliente_id>/historial')
    @login_required
    def api_historial_cliente(cliente_id):
        """API para obtener historial de pagos de un cliente"""
        pagos = pago_dao.obtener_por_cliente(cliente_id)
        historial = []
        
        for pago in pagos:
            historial.append({
                'id': pago.get('id'),
                'fecha_pago': pago.get('fecha_pago'),
                'monto': float(pago.get('monto', 0)),
                'detalle': pago.get('plan_nombre', 'Pago de membresía'),
                'estado': pago.get('estado', 'pendiente')
            })
        
        return jsonify({'success': True, 'data': historial})
    
    # ==========================================
    # ENDPOINT: REGISTRAR PROMOCIÓN 2x1
    # ==========================================
    
    @app.route('/api/promocion-2x1/registrar', methods=['POST'])
    @login_required
    def api_registrar_promocion_2x1():
        """
        API para registrar una promoción 2x1.
        Registra dos clientes y los vincula en la promoción.
        AHORA: Si el DNI ya existe, ACTUALIZA los datos del cliente existente.
        """
        try:
            data = request.get_json()
            
            # Validaciones básicas
            promocion_id = data.get('promocion_id')
            if not promocion_id:
                return jsonify({'success': False, 'message': 'Se requiere el ID de la promoción'}), 400
            
            # Datos del cliente principal
            cliente_principal_data = data.get('cliente_principal', {})
            if not cliente_principal_data.get('dni') or not cliente_principal_data.get('nombre_completo'):
                return jsonify({'success': False, 'message': 'Datos incompletos del cliente principal'}), 400
            
            # Datos del cliente secundario
            cliente_secundario_data = data.get('cliente_secundario', {})
            if not cliente_secundario_data.get('dni') or not cliente_secundario_data.get('nombre_completo'):
                return jsonify({'success': False, 'message': 'Datos incompletos del cliente secundario'}), 400
            
            # Determinar si es un plan 2x1 o una promoción tradicional
            plan_id = None
            precio_total = 0
            fecha_vencimiento = None
            promo_id = None
            
            if str(promocion_id).startswith('plan_'):
                plan_codigo = str(promocion_id).replace('plan_', '')
                plan = plan_dao.obtener_por_codigo(plan_codigo)
                if not plan:
                    return jsonify({'success': False, 'message': 'Plan no encontrado'}), 400
                
                if not plan.get('es_2x1'):
                    return jsonify({'success': False, 'message': 'El plan no es de tipo 2x1'}), 400
                
                plan_id = plan['id']
                precio_total = float(plan.get('precio_2x1', 0))
                
                # La fecha_vencimiento del CLIENTE se calcula desde la duración del PLAN
                # La fecha_vencimiento de la PAREJA es la fecha_fin de la PROMOCIÓN
                from datetime import datetime, timedelta
                import calendar as _calendar
                fecha_actual = datetime.now()
                duracion_str = plan.get('duracion', '1 mes')
                duracion_dict = cliente_dao._parsear_duracion(duracion_str)
                tipo_dur = duracion_dict.get('tipo', 'meses')
                cantidad_dur = duracion_dict.get('cantidad', 1)
                if tipo_dur == 'meses':
                    mes_obj = fecha_actual.month + cantidad_dur
                    año_obj = fecha_actual.year + (mes_obj - 1) // 12
                    mes_obj = ((mes_obj - 1) % 12) + 1
                    ultimo_dia = _calendar.monthrange(año_obj, mes_obj)[1]
                    dia_obj = min(fecha_actual.day, ultimo_dia)
                    fecha_vencimiento_cliente = datetime(año_obj, mes_obj, dia_obj).strftime('%Y-%m-%d')
                elif tipo_dur == 'horas':
                    fecha_vencimiento_cliente = (fecha_actual + timedelta(hours=cantidad_dur)).strftime('%Y-%m-%d %H:%M:%S')
                elif tipo_dur == 'dias':
                    fecha_vencimiento_cliente = (fecha_actual + timedelta(days=cantidad_dur)).strftime('%Y-%m-%d')
                else:
                    fecha_vencimiento_cliente = (fecha_actual + timedelta(days=30)).strftime('%Y-%m-%d')
                # Para la pareja, usar la misma fecha (un plan directo tiene fecha_fin igual a vencimiento del plan)
                fecha_vencimiento_pareja = fecha_vencimiento_cliente
                fecha_vencimiento = fecha_vencimiento_cliente
                
            elif str(promocion_id).startswith('promo_'):
                promo_id_real = int(str(promocion_id).replace('promo_', ''))
                promocion = promocion_dao.obtener_por_id(promo_id_real)
                if not promocion:
                    return jsonify({'success': False, 'message': 'Promoción no encontrada'}), 400
                
                if promocion.get('tipo_promocion') != '2x1':
                    return jsonify({'success': False, 'message': 'La promoción no es de tipo 2x1'}), 400
                
                plan_id = promocion['plan_id']
                precio_total = float(promocion.get('precio_2x1', 0))
                # La fecha_vencimiento del CLIENTE se calcula desde la duración del PLAN
                plan_info = plan_dao.obtener_por_id(plan_id)
                duracion_str = plan_info.get('duracion', '1 mes') if plan_info else '1 mes'
                duracion_dict = cliente_dao._parsear_duracion(duracion_str)
                from datetime import datetime, timedelta
                import calendar as _calendar
                fecha_actual = datetime.now()
                tipo = duracion_dict.get('tipo', 'meses')
                cantidad = duracion_dict.get('cantidad', 1)
                if tipo == 'meses':
                    mes_objetivo = fecha_actual.month + cantidad
                    año_objetivo = fecha_actual.year + (mes_objetivo - 1) // 12
                    mes_objetivo = ((mes_objetivo - 1) % 12) + 1
                    ultimo_dia = _calendar.monthrange(año_objetivo, mes_objetivo)[1]
                    dia = min(fecha_actual.day, ultimo_dia)
                    fecha_vencimiento_cliente = datetime(año_objetivo, mes_objetivo, dia).strftime('%Y-%m-%d')
                elif tipo == 'horas':
                    fecha_vencimiento_cliente = (fecha_actual + timedelta(hours=cantidad)).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    fecha_vencimiento_cliente = (fecha_actual + timedelta(days=cantidad)).strftime('%Y-%m-%d')
                # La fecha_vencimiento de la pareja es la misma del plan
                # (la promoción como oferta ya tiene su propia fecha_fin en la tabla promociones)
                fecha_vencimiento_pareja = fecha_vencimiento_cliente
                # Mantener compatibilidad con el resto de la función
                fecha_vencimiento = fecha_vencimiento_cliente
                promo_id = promo_id_real
            else:
                promocion = promocion_dao.obtener_por_id(promocion_id)
                if not promocion:
                    return jsonify({'success': False, 'message': 'Promoción no encontrada'}), 400
                
                if promocion.get('tipo_promocion') != '2x1':
                    return jsonify({'success': False, 'message': 'La promoción no es de tipo 2x1'}), 400
                
                plan_id = promocion['plan_id']
                precio_total = float(promocion.get('precio_2x1', 0))
                # La fecha_vencimiento del CLIENTE se calcula desde la duración del PLAN
                plan_info = plan_dao.obtener_por_id(plan_id)
                duracion_str = plan_info.get('duracion', '1 mes') if plan_info else '1 mes'
                duracion_dict = cliente_dao._parsear_duracion(duracion_str)
                from datetime import datetime, timedelta
                import calendar as _calendar
                fecha_actual = datetime.now()
                tipo = duracion_dict.get('tipo', 'meses')
                cantidad = duracion_dict.get('cantidad', 1)
                if tipo == 'meses':
                    mes_objetivo = fecha_actual.month + cantidad
                    año_objetivo = fecha_actual.year + (mes_objetivo - 1) // 12
                    mes_objetivo = ((mes_objetivo - 1) % 12) + 1
                    ultimo_dia = _calendar.monthrange(año_objetivo, mes_objetivo)[1]
                    dia = min(fecha_actual.day, ultimo_dia)
                    fecha_vencimiento_cliente = datetime(año_objetivo, mes_objetivo, dia).strftime('%Y-%m-%d')
                elif tipo == 'horas':
                    fecha_vencimiento_cliente = (fecha_actual + timedelta(hours=cantidad)).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    fecha_vencimiento_cliente = (fecha_actual + timedelta(days=cantidad)).strftime('%Y-%m-%d')
                # La fecha_vencimiento de la pareja es la misma del plan
                # (la promoción como oferta ya tiene su propia fecha_fin en la tabla promociones)
                fecha_vencimiento_pareja = fecha_vencimiento_cliente
                # Mantener compatibilidad con el resto de la función
                fecha_vencimiento = fecha_vencimiento_cliente
                promo_id = promocion_id
            
            # Método de pago individual por cliente
            metodo_pago_principal = cliente_principal_data.get('metodo_pago', 'efectivo')
            metodo_pago_secundario = cliente_secundario_data.get('metodo_pago', 'efectivo')
            monto_por_persona = precio_total / 2
            
            # =========================================================
            # FUNCIÓN INTERNA: Registrar o ACTUALIZAR cliente (SOLO PARA 2x1)
            # =========================================================
            def registrar_o_actualizar_cliente_2x1(datos_cliente, es_principal):
                from datetime import datetime
                dni = datos_cliente.get('dni')
                
                # Buscar si el cliente ya existe (INCLUYENDO inactivos)
                cliente_existente = cliente_dao.obtener_por_dni(dni)
                
                # Preparar datos comunes
                fecha_actual = datetime.now()
                fecha_inicio_str = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')
                metodo_pago = metodo_pago_principal if es_principal else metodo_pago_secundario
                rol = 'principal' if es_principal else 'secundario'
                
                if cliente_existente:
                    # ===== CLIENTE YA EXISTE: ACTUALIZAR =====
                    cliente_id = cliente_existente['id']
                    
                    # Actualizar datos del cliente existente,
                    # incluyendo fecha_vencimiento para que el QR quede correcto
                    cliente_dao.actualizar(cliente_id, {
                        'nombre_completo': datos_cliente.get('nombre_completo'),
                        'telefono': datos_cliente.get('telefono'),
                        'turno': datos_cliente.get('turno', 'manana'),
                        'segmento': datos_cliente.get('segmento', 'No Asignado'),
                        'sexo': datos_cliente.get('sexo', 'no_especificado'),
                        'plan_id': plan_id,
                        'fecha_vencimiento': fecha_vencimiento,
                        'activo': 1
                    })
                    
                    # Registrar en historial de membresía
                    historial_membresia_dao.crear_from_dict({
                        'cliente_id': cliente_id,
                        'plan_id': plan_id,
                        'fecha_inicio': fecha_inicio_str,
                        'fecha_fin': fecha_vencimiento,
                        'monto_pagado': monto_por_persona,
                        'metodo_pago': metodo_pago,
                        'estado': 'activa',
                        'observaciones': f'Cliente {rol} - Promoción 2x1 (Cliente existente actualizado)',
                        'usuario_id': session.get('usuario_id', 1)
                    })
                    
                    # Registrar pago
                    pago_dao.crear_from_dict({
                        'cliente_id': cliente_id,
                        'plan_id': plan_id,
                        'monto': monto_por_persona,
                        'metodo_pago': metodo_pago,
                        'estado': 'completado',
                        'usuario_registro': session.get('usuario_id', 1)
                    })
                    
                    return cliente_id, True  # True = actualizado
                
                else:
                    # ===== CLIENTE NUEVO: CREAR =====
                    # Verificar si el plan tiene QR habilitado para generarlo
                    plan_info = plan_dao.obtener_por_id(plan_id)
                    plan_tiene_qr = plan_info and plan_info.get('qr_habilitado') == 1
                    
                    nuevo_cliente = Cliente(
                        dni=datos_cliente.get('dni'),
                        nombre_completo=datos_cliente.get('nombre_completo'),
                        telefono=datos_cliente.get('telefono'),
                        plan_id=plan_id,
                        fecha_inicio=fecha_inicio_str,
                        fecha_vencimiento=fecha_vencimiento,
                        usuario_id=session.get('usuario_id', 1),
                        turno=datos_cliente.get('turno', 'manana'),
                        sexo=datos_cliente.get('sexo', 'no_especificado'),
                        segmento=datos_cliente.get('segmento', 'No Asignado')
                    )
                    
                    # Crear cliente; si el plan tiene QR, generarlo en el mismo paso
                    cliente_id = cliente_dao.crear(nuevo_cliente, generar_qr=plan_tiene_qr)
                    
                    # Registrar en historial de membresía
                    historial_membresia_dao.crear_from_dict({
                        'cliente_id': cliente_id,
                        'plan_id': plan_id,
                        'fecha_inicio': fecha_inicio_str,
                        'fecha_fin': fecha_vencimiento,
                        'monto_pagado': monto_por_persona,
                        'metodo_pago': metodo_pago,
                        'estado': 'activa',
                        'observaciones': f'Cliente {rol} - Promoción 2x1 (Nuevo)',
                        'usuario_id': session.get('usuario_id', 1)
                    })
                    
                    # Registrar pago
                    pago_dao.crear_from_dict({
                        'cliente_id': cliente_id,
                        'plan_id': plan_id,
                        'monto': monto_por_persona,
                        'metodo_pago': metodo_pago,
                        'estado': 'completado',
                        'usuario_registro': session.get('usuario_id', 1)
                    })
                    
                    return cliente_id, False  # False = nuevo
            
            # Registrar o actualizar cliente principal
            cliente_principal_id, principal_actualizado = registrar_o_actualizar_cliente_2x1(cliente_principal_data, True)
            
            # Registrar o actualizar cliente secundario
            cliente_secundario_id, secundario_actualizado = registrar_o_actualizar_cliente_2x1(cliente_secundario_data, False)
            
            # Crear el registro de pareja en promoción
            pareja_data = {
                'promocion_id': promo_id,
                'cliente_principal_id': cliente_principal_id,
                'cliente_secundario_id': cliente_secundario_id,
                'precio_total': precio_total,
                'fecha_vencimiento': fecha_vencimiento_pareja,
                'activo': 1
            }
            pareja_promocion_dao.crear_from_dict(pareja_data)
            
            # Mensaje personalizado
            mensaje_parts = []
            if principal_actualizado:
                mensaje_parts.append("cliente principal actualizado")
            else:
                mensaje_parts.append("cliente principal nuevo")
            
            if secundario_actualizado:
                mensaje_parts.append("cliente secundario actualizado")
            else:
                mensaje_parts.append("cliente secundario nuevo")
            
            mensaje_final = f"Promoción 2x1 registrada: {mensaje_parts[0]} y {mensaje_parts[1]}"
            
            return jsonify({
                'success': True,
                'message': mensaje_final,
                'cliente_principal_id': cliente_principal_id,
                'cliente_secundario_id': cliente_secundario_id,
                'principal_actualizado': principal_actualizado,
                'secundario_actualizado': secundario_actualizado,
                'pareja_id': pareja_promocion_dao.obtener_por_cliente_principal(cliente_principal_id)[-1]['id'] if pareja_promocion_dao.obtener_por_cliente_principal(cliente_principal_id) else None
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 400
    
    @app.route('/api/promocion-2x1/separar/<int:pareja_id>', methods=['POST'])
    @login_required
    def api_separar_promocion_2x1(pareja_id):
        """
        API para separar una pareja en promoción 2x1.
        Esto les permite continuar con sus membresías de forma individual.
        """
        try:
            pareja_dao = pareja_promocion_dao
            pareja = pareja_dao.obtener_por_id(pareja_id)
            
            if not pareja:
                return jsonify({'success': False, 'message': 'Pareja en promoción no encontrada'}), 404
            
            if pareja.get('separada'):
                return jsonify({'success': False, 'message': 'La pareja ya ha sido separada'}), 400
            
            # Separar la pareja
            pareja_dao.separar_pareja(pareja_id)
            
            return jsonify({
                'success': True,
                'message': 'Pareja separada exitosamente. Los clientes ahora tienen membresías individuales.'
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 400
    
    @app.route('/api/promocion-2x1/<int:cliente_id>')
    @login_required
    def api_obtener_pareja_cliente(cliente_id):
        """
        API para obtener la pareja en promoción de un cliente.
        """
        try:
            pareja_dao = pareja_promocion_dao
            pareja = pareja_dao.obtener_pareja_activa_cliente(cliente_id)
            
            if pareja:
                # Obtener detalles completos
                detalles = pareja_dao.obtener_detalles_completos(pareja['id'])
                return jsonify({'success': True, 'data': detalles})
            else:
                return jsonify({'success': True, 'data': None})
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 400
    
    @app.route('/api/clientes/historial-membresias', methods=['POST'])
    @login_required
    def api_historial_membresias_multiple():
        """API para obtener historial de membresías de múltiples clientes"""
        try:
            data = request.get_json()
            cliente_ids = data.get('cliente_ids', [])
            
            if not cliente_ids:
                return jsonify({'success': False, 'message': 'Se requieren IDs de clientes'}), 400
            
            historial_resultado = {}
            
            for cliente_id in cliente_ids:
                # Obtener el historial de membresías del cliente
                historial = historial_membresia_dao.obtener_por_cliente(cliente_id)

# Obtener información del cliente
                cliente = cliente_dao.obtener_por_id(cliente_id)
                
                # Usar nuevo método del DAO para obtener pagos
                pagos = cliente_dao.obtener_pagos_por_cliente(cliente_id, solo_completados=True)
                
                # Verificar si tiene pago pendiente explícito en la BD
                pagos_pendientes = cliente_dao.verificar_pagos_pendientes(cliente_id)
                tiene_pendiente = pagos_pendientes.get('tiene_pendiente', False)
                
                # Formatear los datos del historial CON ESTADO CORREGIDO
                historial_formateado = []
                
                for idx, h in enumerate(historial):
                    # Obtener datos originales
                    estado_original = h.get('estado', '')
                    fecha_inicio = h.get('fecha_inicio', '')
                    fecha_fin = h.get('fecha_fin', '')
                    historial_id = h.get('id')

                    # Verificar si el siguiente registro (más reciente) es CAMBIO DE PLAN
                    es_previo_a_cambio_plan = False
                    if idx > 0:
                        obs_siguiente = (historial[idx - 1].get('observaciones') or '').upper()
                        if 'CAMBIO DE PLAN' in obs_siguiente:
                            es_previo_a_cambio_plan = True

                    # Calcular el estado correcto
                    estado_calulado, metodo_calulado = calcular_estado_membresia(
                        historial_id=historial_id,
                        cliente_id=cliente_id,
                        fecha_inicio=fecha_inicio,
                        fecha_fin=fecha_fin,
                        estado_original=estado_original,
                        pagos=pagos,
                        es_ultimo=(idx == 0),
                        es_previo_a_cambio_plan=es_previo_a_cambio_plan,
                        tiene_pendiente=tiene_pendiente
                    )
                    
                    def _fmt_fecha(v):
                        if not v: return ''
                        if hasattr(v, 'strftime'): return v.strftime('%Y-%m-%d')
                        return str(v)[:10]

                    historial_formateado.append({
                        'id': h.get('id'),
                        'plan_nombre': h.get('plan_nombre', ''),
                        'plan_codigo': h.get('plan_codigo', ''),
                        'fecha_inicio': _fmt_fecha(h.get('fecha_inicio')),
                        'fecha_fin': _fmt_fecha(h.get('fecha_fin')),
                        'monto_pagado': float(h.get('monto_pagado', 0)) if h.get('monto_pagado') else 0,
                        'metodo_pago': metodo_calulado,
                        'estado': estado_calulado,
                        'observaciones': h.get('observaciones', ''),
                        'fecha_registro': str(h.get('fecha_registro', ''))[:19] if h.get('fecha_registro') else '',
                        'usuario_nombre': h.get('usuario_nombre', '')
                    })
                
                historial_resultado[cliente_id] = {
                    'cliente_nombre': cliente.get('nombre_completo', '') if cliente else '',
                    'cliente_dni': cliente.get('dni', '') if cliente else '',
                    'historial': historial_formateado
                }
            
            return jsonify({
                'success': True,
                'historial': historial_resultado
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
    
    @app.route('/api/clientes/validar', methods=['POST'])
    @login_required
    def api_validar_cliente():
        """API para validar DNI y teléfono antes de crear/editar cliente"""
        try:
            data = request.get_json()
            dni = data.get('dni', '').strip()
            telefono = data.get('telefono', '').strip()
            cliente_id = data.get('cliente_id')  # Para edición, excluir el cliente actual
            
            resultado = {
                'dni_cliente': None,
                'telefono_cliente': None,
                'dni_invitado': None,
                'telefono_invitado': None,
                'mismo_invitado': False
            }
            
            # Verificar DNI en clientes activos
            if dni:
                cliente_existente = cliente_dao.buscar_por_dni(dni)
                if cliente_existente and (not cliente_id or cliente_existente.get('id') != cliente_id):
                    resultado['dni_cliente'] = cliente_existente.get('nombre_completo')
            
            # Verificar teléfono en clientes activos
            if telefono:
                cliente_tel = cliente_dao.buscar_por_telefono(telefono)
                if cliente_tel and (not cliente_id or cliente_tel.get('id') != cliente_id):
                    resultado['telefono_cliente'] = cliente_tel.get('nombre_completo')
            
            # Verificar en invitados activos
            if dni:
                invitado_dni = invitado_dao.buscar_por_dni(dni)
                if invitado_dni:
                    resultado['dni_invitado'] = invitado_dni
            
            if telefono:
                invitado_tel = invitado_dao.buscar_por_telefono(telefono)
                if invitado_tel:
                    resultado['telefono_invitado'] = invitado_tel
            
            # Verificar si es el mismo invitado (DNI y teléfono coinciden)
            if resultado['dni_invitado'] and resultado['telefono_invitado']:
                if resultado['dni_invitado'].get('id') == resultado['telefono_invitado'].get('id'):
                    resultado['mismo_invitado'] = True
            
            return jsonify({'success': True, 'data': resultado})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/clientes/<int:cliente_id>/pagar', methods=['POST'])
    @login_required
    def api_pagar_membresia_acceso(cliente_id):
        """API para registrar pago y extender membresía de un cliente desde Control de Acceso"""
        try:
            data = request.get_json() or {}
            monto_override = data.get('monto')

            # Obtener el cliente
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if not cliente:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
            
            # Obtener el plan del cliente
            plan_id = cliente.get('plan_id')
            if not plan_id:
                return jsonify({'success': False, 'message': 'El cliente no tiene un plan asignado'}), 400
            
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404

            # Calcular monto: frontend > promo > precio base
            precio_base = float(plan.get('precio', 0))
            if monto_override is not None:
                monto_pago = float(monto_override)
            else:
                monto_pago = precio_base
                try:
                    monto_pago, _, _ = promocion_dao.calcular_precio_con_descuento(
                        plan_id, precio_base, sexo_cliente=cliente.get('sexo'), turno_cliente=cliente.get('turno'), segmento_cliente=cliente.get('segmento_promocion')
                    )
                except Exception:
                    pass

            # Calcular nueva fecha de vencimiento
            hoy = datetime.now()
            
            # Determinar fecha de inicio (hoy o mañana si ya venció)
            fecha_vencimiento_actual = cliente.get('fecha_vencimiento', '')
            dias_restantes = 0
            
            if fecha_vencimiento_actual:
                try:
                    if ' ' in fecha_vencimiento_actual:
                        fecha_venc = datetime.strptime(fecha_vencimiento_actual, '%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_venc = datetime.strptime(fecha_vencimiento_actual, '%Y-%m-%d')
                    dias_restantes = (fecha_venc.date() - hoy.date()).days
                except:
                    pass
            
            # Si aún tiene días restantes, la nueva membresía empieza desde la fecha actual + 1 día
            # Si ya venció, empieza desde hoy
            if dias_restantes > 0:
                nueva_fecha_inicio = fecha_venc + timedelta(days=1)
            else:
                nueva_fecha_inicio = hoy
            
            # Calcular duración del plan
            duracion_dias = int(plan.get('duracion_dias', 30))
            nueva_fecha_fin = nueva_fecha_inicio + timedelta(days=duracion_dias)
            
            # Actualizar la fecha de vencimiento del cliente
            cliente_dao.actualizar(cliente_id, {
                'fecha_inicio': nueva_fecha_inicio.strftime('%Y-%m-%d'),
                'fecha_vencimiento': nueva_fecha_fin.strftime('%Y-%m-%d'),
                'habilitado': 1  # Activar el cliente
            })
            
            # Registrar el pago
            pago_dao.crear_from_dict({
                'cliente_id': cliente_id,
                'monto': monto_pago,
                'metodo_pago': data.get('metodo_pago', 'efectivo'),
                'estado': 'completado'
            })
            
            # Registrar en historial de membresías
            historial_membresia_dao.crear_from_dict({
                'cliente_id': cliente_id,
                'plan_id': plan_id,
                'fecha_inicio': nueva_fecha_inicio.strftime('%Y-%m-%d'),
                'fecha_fin': nueva_fecha_fin.strftime('%Y-%m-%d'),
                'monto_pagado': monto_pago,
                'metodo_pago': data.get('metodo_pago', 'efectivo'),
                'estado': 'activa',
                'observaciones': 'Pago registrado desde Control de Acceso',
                'usuario_id': session.get('usuario_id',1)
            })
            
            # === GENERAR NOTIFICACIÓN DE PAGO ===
            notificacion_dao.crear_notificacion(
                tipo='payment',
                titulo='Pago recibido desde acceso',
                mensaje=f'{cliente["nombre_completo"]} pagó S/. {monto_pago:.2f} desde Control de Acceso',
                cliente_id=cliente_id,
                usuario_id=session.get('usuario_id', 1)
            )
            _invalidar_cache_notif()  # forzar recarga inmediata en el próximo fetch
            
            # === ELIMINAR NOTIFICACIÓN DE VENCIMIENTO SI EXISTE ===
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM notificaciones 
                WHERE cliente_id = %s 
                AND tipo = 'vencimiento'
                AND leida = 0
            ''', (cliente_id,))
            conn.close()
            
            return jsonify({
                'success': True,
                'message': 'Pago registrado correctamente',
                'nueva_fecha_vencimiento': nueva_fecha_fin.strftime('%Y-%m-%d')
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/invitados/validar', methods=['POST'])
    @login_required
    def api_validar_invitado():
        """API para validar DNI y teléfono antes de crear/editar invitado"""
        try:
            data = request.get_json()
            dni = data.get('dni', '').strip()
            telefono = data.get('telefono', '').strip()
            invitado_id = data.get('invitado_id')  # Para edición
            
            errores = []
            
            # Verificar DNI en clientes
            if dni:
                cliente = cliente_dao.buscar_por_dni(dni)
                if cliente and cliente.get('activo', 1) == 1:
                    errores.append(f'Este DNI pertenece a un cliente')
            
            # Verificar teléfono en clientes
            if telefono:
                cliente = cliente_dao.buscar_por_telefono(telefono)
                if cliente and cliente.get('activo', 1) == 1:
                    errores.append(f'El teléfono pertenece a un cliente')
            
            # Verificar DNI en otros invitados
            if dni:
                invitado = invitado_dao.buscar_por_dni(dni)
                if invitado and (not invitado_id or invitado.get('id') != invitado_id):
                    errores.append(f'Este DNI ya está registrado en otro invitado')
            
            # Verificar teléfono en otros invitados
            if telefono:
                invitado = invitado_dao.buscar_por_telefono(telefono)
                if invitado and (not invitado_id or invitado.get('id') != invitado_id):
                    errores.append(f'Este teléfono ya está registrado en otro invitado')
            
            return jsonify({'success': True, 'errores': errores})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/clientes/<int:cliente_id>/renovar', methods=['POST'])
    @login_required
    def api_renovar_membresia(cliente_id):
        """API para renovar la membresía de un cliente (agregar meses)"""
        
        try:
            data = request.get_json()
            meses = int(data.get('meses', 1))
            metodo_pago = data.get('metodo_pago', 'efectivo')
            
            # Obtener cliente actual
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if not cliente:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
            
            # Obtener plan actual
            plan_id = cliente.get('plan_id')
            if not plan_id:
                return jsonify({'success': False, 'message': 'Cliente sin plan asignado'}), 400
            
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404
            
            # Calcular nueva fecha de vencimiento
            # La fecha de inicio de la nueva membresía será la fecha actual
            fecha_actual = datetime.now()
            fecha_inicio_nueva = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')
            
            # Calcular fecha de fin sumando los meses correspondientes
            año_objetivo = fecha_actual.year
            mes_objetivo = fecha_actual.month + meses
            
            while mes_objetivo > 12:
                mes_objetivo -= 12
                año_objetivo += 1
            
            # Determinar el último día del mes de destino
            dia_objetivo = fecha_actual.day
            try:
                if mes_objetivo == 12:
                    fecha_temp = datetime(año_objetivo + 1, 1, 1)
                else:
                    fecha_temp = datetime(año_objetivo, mes_objetivo + 1, 1)
                fecha_temp = fecha_temp - timedelta(days=1)
                ultimo_dia_mes_objetivo = fecha_temp.day
            except ValueError:
                ultimo_dia_mes_objetivo = 28
            
            dia_final = min(dia_objetivo, ultimo_dia_mes_objetivo)
            fecha_fin_nueva = datetime(año_objetivo, mes_objetivo, dia_final, 
                                       fecha_actual.hour, fecha_actual.minute, fecha_actual.second)
            fecha_fin_nueva_str = fecha_fin_nueva.strftime('%Y-%m-%d %H:%M:%S')
            
            # Calcular monto total
            monto_total = float(plan['precio']) * meses
            
            # === REGISTRAR EN HISTORIAL DE MEMBRESÍAS ===
            historial_data = {
                'cliente_id': cliente_id,
                'plan_id': plan_id,
                'fecha_inicio': fecha_inicio_nueva,
                'fecha_fin': fecha_fin_nueva_str,
                'monto_pagado': monto_total,
                'metodo_pago': metodo_pago,
                'estado': 'activa',
                'observaciones': f'RenOVACIÓN: {meses} mes(es) agregado(s)',
                'usuario_id': session.get('usuario_id', 1)
            }
            historial_membresia_dao.crear_from_dict(historial_data)
            
            # Actualizar fecha de vencimiento del cliente
            cliente_dao.actualizar(cliente_id, {
                'fecha_vencimiento': fecha_fin_nueva_str
            })
            
            # === GENERAR NOTIFICACIÓN DE RENOVACIÓN ===
            notificacion_dao.crear_notificacion(
                tipo='membership',
                titulo='Membresía renovada',
                mensaje=f'{cliente["nombre_completo"]} renovó su membresía por {meses} mes(es)',
                cliente_id=cliente_id,
                usuario_id=session.get('usuario_id', 1)
            )
            # Limpiar notificaciones de vencimiento (próximo y ya vencido)
            notificacion_dao.limpiar_notificaciones_vencimiento(cliente_id)
            _invalidar_cache_notif()  # forzar recarga inmediata en el próximo fetch
            
            return jsonify({
                'success': True,
                'message': f'Membresía renovada por {meses} mes(es)',
                'nueva_fecha_vencimiento': fecha_fin_nueva_str,
                'monto_pagado': monto_total
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/clientes/<int:cliente_id>/historial-membresia')
    @login_required
    def api_historial_membresia_cliente(cliente_id):
        """API para obtener el historial de membresías de un cliente"""
        try:
            historial = historial_membresia_dao.obtener_por_cliente(cliente_id)
            return jsonify({'success': True, 'data': historial})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/clientes/<int:cliente_id>/estado-pago-actual')
    @login_required
    def api_estado_pago_actual(cliente_id):
        """API para verificar el estado de pago del cliente (incluye vencido)"""
        try:
            resultado = cliente_dao.verificar_estado_pago_actual(cliente_id)
            return jsonify({'success': True, 'data': resultado})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
        
    @app.route('/api/clientes/<int:cliente_id>/tiene-membresia-extendida')
    @login_required
    def api_tiene_membresia_extendida(cliente_id):
        """Bloquea la edición del plan cuando el cliente ya tiene pagos"""
        try:
            pagos = cliente_dao.obtener_pagos_por_cliente(cliente_id, solo_completados=True)
            tiene_pagos = len(pagos) > 0
            return jsonify({
                'success': True,
                'tiene_membresia_extendida': tiene_pagos
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400

    @app.route('/api/clientes/<int:cliente_id>/uso-promocion')
    @login_required
    def api_uso_promocion(cliente_id):
        """
        Verifica si el cliente ya usó la promoción (tiene pagos completados o accesos registrados).
        El bloqueo SOLO aplica si:
        1. El cliente tiene un segmento específico (NO "No Asignado")
        2. Y existe una promoción VIGENTE para ese segmento y plan
        3. Y el cliente ya tiene pagos o accesos
        
        Si el segmento es "No Asignado" o la promoción ya expiró, se permite editar.
        """
        try:
            # Obtener cliente
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if not cliente:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
            
            segmento_actual = cliente.get('segmento', 'No Asignado')
            
            # Si el segmento es "No Asignado", siempre se puede editar
            if segmento_actual == 'No Asignado' or not segmento_actual:
                return jsonify({
                    'success': True,
                    'data': {
                        'tiene_uso': False,
                        'tiene_pagos': False,
                        'tiene_accesos': False,
                        'segmento_bloqueado': False,
                        'motivo': 'Segmento no asignado, se puede editar libremente'
                    }
                })
            
            # Verificar pagos completados
            pagos = cliente_dao.obtener_pagos_por_cliente(cliente_id, solo_completados=True)
            tiene_pagos = len(pagos) > 0
            
            # Verificar accesos registrados
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) as total_accesos 
                FROM accesos 
                WHERE cliente_id = %s AND (tipo = 'cliente' OR tipo IS NULL)
            ''', (cliente_id,))
            result = cursor.fetchone()
            conn.close()
            
            tiene_accesos = result['total_accesos'] > 0 if result else False

            
            plan_id = cliente.get('plan_id')
            tiene_promocion_vigente = False
            
            if plan_id:
                # Buscar promoción vigente para este segmento y plan
                promocion = promocion_dao.obtener_vigentes_por_plan(
                    plan_id, 
                    sexo_cliente=cliente.get('sexo'),
                    turno_cliente=cliente.get('turno'),
                    segmento_cliente=segmento_actual
                )
                tiene_promocion_vigente = promocion is not None
            
            # SOLO se bloquea si:
            # 1. Tiene un segmento específico (NO "No Asignado")
            # 2. Existe una promoción VIGENTE para ese segmento
            # 3. Y ya tiene pagos o accesos
            segmento_bloqueado = tiene_promocion_vigente and (tiene_pagos or tiene_accesos)
            
            motivo = ""
            if not tiene_promocion_vigente:
                motivo = "No hay promoción vigente para este segmento"
            elif not (tiene_pagos or tiene_accesos):
                motivo = "El cliente aún no tiene pagos ni accesos"
            
            
            return jsonify({
                'success': True,
                'data': {
                    'tiene_uso': tiene_pagos or tiene_accesos,
                    'tiene_pagos': tiene_pagos,
                    'tiene_accesos': tiene_accesos,
                    'segmento_bloqueado': segmento_bloqueado,
                    'tiene_promocion_vigente': tiene_promocion_vigente,
                    'segmento_actual': segmento_actual,
                    'motivo': motivo
                }
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/clientes/<int:cliente_id>/aumentar-meses', methods=['POST'])
    @login_required
    def api_aumentar_meses(cliente_id):
        """
        API para aumentar meses a la membresía de un cliente
        Crea un registro PENDIENTE en historial_membresia con:
        - fecha_inicio = fecha_vencimiento ACTUAL del cliente
        - fecha_fin = fecha_inicio + duración del plan
        - estado = 'pendiente'
        """
        
        try:
            data = request.get_json()
            
            if 'cantidad' in data and 'tipo' in data:
                cantidad = int(data.get('cantidad', 1))
                tipo = data.get('tipo', 'meses')
            else:
                cantidad = int(data.get('meses', 1))
                tipo = 'meses'
            
            if cantidad < 1:
                return jsonify({'success': False, 'message': 'Debe aumentar al menos 1 tiempo'}), 400
            
            if cantidad > 12:
                return jsonify({'success': False, 'message': 'Máximo 12 tiempos permitidos'}), 400
            
            # Obtener cliente actual
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if not cliente:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
            
            # Obtener plan actual
            plan_id = cliente.get('plan_id')
            if not plan_id:
                return jsonify({'success': False, 'message': 'Cliente sin plan asignado'}), 400
            
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 400
            
            # Verificar si ha pagado el mes actual
            fecha_actual = datetime.now()
            primer_dia_mes = fecha_actual.replace(day=1).strftime('%Y-%m-%d')
            
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id FROM pagos
                WHERE cliente_id = %s
                AND estado = 'completado'
                AND DATE(fecha_pago) >= DATE(%s)
                LIMIT 1
            ''', (cliente_id, primer_dia_mes))
            
            ha_pagado = cursor.fetchone() is not None
            
            # También verificar si tiene pago pendiente
            cursor.execute('''
                SELECT id FROM pagos
                WHERE cliente_id = %s AND estado = 'pendiente'
                LIMIT 1
            ''', (cliente_id,))
            tiene_pendiente = cursor.fetchone() is not None
            
            if not ha_pagado or tiene_pendiente:
                conn.close()
                return jsonify({
                    'success': False, 
                    'message': 'El cliente debe pagar el mes actual y no tener pagos pendientes antes de aumentar meses'
                }), 400
            
            # Obtener fecha de vencimiento ACTUAL del cliente
            fecha_vencimiento_actual = cliente.get('fecha_vencimiento')
            
            # La NUEVA fecha de inicio = fecha de vencimiento actual
            if fecha_vencimiento_actual:
                try:
                    if ' ' in str(fecha_vencimiento_actual):
                        fecha_inicio_nueva = datetime.strptime(str(fecha_vencimiento_actual), '%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_inicio_nueva = datetime.strptime(str(fecha_vencimiento_actual)[:10], '%Y-%m-%d')
                except:
                    fecha_inicio_nueva = fecha_actual
            else:
                fecha_inicio_nueva = fecha_actual
            
            # Calcular fecha de fin según el tipo de duración
            duracion_plan = plan.get('duracion', '1 mes')
            duracion_dict = cliente_dao._parsear_duracion(duracion_plan)
            tipo_plan = duracion_dict.get('tipo', 'meses')
            cantidad_plan = duracion_dict.get('cantidad', 1)
            
            # Usar la cantidad del plan base (NO multiplicar)
            if tipo == 'meses':
                # Sumar meses
                año_objetivo = fecha_inicio_nueva.year
                mes_objetivo = fecha_inicio_nueva.month + cantidad_plan
                
                while mes_objetivo > 12:
                    mes_objetivo -= 12
                    año_objetivo += 1
                
                dia_objetivo = fecha_inicio_nueva.day
                try:
                    if mes_objetivo == 12:
                        fecha_temp = datetime(año_objetivo + 1, 1, 1)
                    else:
                        fecha_temp = datetime(año_objetivo, mes_objetivo + 1, 1)
                    fecha_temp = fecha_temp - timedelta(days=1)
                    ultimo_dia_mes_objetivo = fecha_temp.day
                except ValueError:
                    ultimo_dia_mes_objetivo = 28
                
                dia_final = min(dia_objetivo, ultimo_dia_mes_objetivo)
                fecha_fin_nueva = datetime(año_objetivo, mes_objetivo, dia_final,
                                        fecha_inicio_nueva.hour, fecha_inicio_nueva.minute, fecha_inicio_nueva.second)
            elif tipo == 'semanas':
                fecha_fin_nueva = fecha_inicio_nueva + timedelta(weeks=cantidad_plan)
            elif tipo == 'dias':
                fecha_fin_nueva = fecha_inicio_nueva + timedelta(days=cantidad_plan)
            else:
                # Por defecto, meses
                año_objetivo = fecha_inicio_nueva.year
                mes_objetivo = fecha_inicio_nueva.month + cantidad_plan
                while mes_objetivo > 12:
                    mes_objetivo -= 12
                    año_objetivo += 1
                dia_objetivo = fecha_inicio_nueva.day
                try:
                    if mes_objetivo == 12:
                        fecha_temp = datetime(año_objetivo + 1, 1, 1)
                    else:
                        fecha_temp = datetime(año_objetivo, mes_objetivo + 1, 1)
                    fecha_temp = fecha_temp - timedelta(days=1)
                    ultimo_dia_mes_objetivo = fecha_temp.day
                except ValueError:
                    ultimo_dia_mes_objetivo = 28
                dia_final = min(dia_objetivo, ultimo_dia_mes_objetivo)
                fecha_fin_nueva = datetime(año_objetivo, mes_objetivo, dia_final,
                                        fecha_inicio_nueva.hour, fecha_inicio_nueva.minute, fecha_inicio_nueva.second)
            
            fecha_inicio_nueva_str = fecha_inicio_nueva.strftime('%Y-%m-%d %H:%M:%S')
            fecha_fin_nueva_str = fecha_fin_nueva.strftime('%Y-%m-%d %H:%M:%S')
            
            # Calcular monto (precio base del plan, sin multiplicar)
            monto_total = float(plan['precio'])
            
            # Aplicar promoción si existe
            sexo_cliente = cliente.get('sexo', None)
            turno_cliente = cliente.get('turno', None)
            segmento_cliente = cliente.get('segmento_promocion', None)
            
            try:
                precio_final, descuento, promocion = promocion_dao.calcular_precio_con_descuento(
                    plan_id, monto_total, sexo_cliente=sexo_cliente, turno_cliente=turno_cliente, segmento_cliente=segmento_cliente
                )
                monto_total = precio_final
            except Exception as e:
                print(f"[Aumento Meses] Error al calcular promoción: {e}")
            
            tipoTexto = 'mes(es)' if tipo == 'meses' else ('semana(s)' if tipo == 'semanas' else 'día(s)')
            
            # REGISTRAR EN HISTORIAL con estado PENDIENTE
            historial_data = {
                'cliente_id': cliente_id,
                'plan_id': plan_id,
                'fecha_inicio': fecha_inicio_nueva_str,
                'fecha_fin': fecha_fin_nueva_str,
                'monto_pagado': monto_total,
                'metodo_pago': 'pendiente',
                'estado': 'pendiente',
                'observaciones': f'AUMENTO DE MESES: {cantidad} {tipoTexto} agregado(s) - PENDIENTE DE PAGO',
                'usuario_id': session.get('usuario_id', 1)
            }
            historial_membresia_dao.crear_from_dict(historial_data)

            # Actualizar fechas del cliente AHORA al aumentar meses
            # El pago queda pendiente pero las fechas ya reflejan el nuevo período
            cursor.execute('''
                UPDATE clientes
                SET fecha_inicio = %s, fecha_vencimiento = %s
                WHERE id = %s
            ''', (fecha_inicio_nueva_str, fecha_fin_nueva_str, cliente_id))
            conn.commit()

            conn.close()
            
            # Notificación
            notificacion_dao.crear_notificacion(
                tipo='membership',
                titulo='Membresía extendida (pendiente)',
                mensaje=f'{cliente["nombre_completo"]} solicitó extender su membresía por {cantidad} {tipoTexto} - PENDIENTE DE PAGO',
                cliente_id=cliente_id,
                usuario_id=session.get('usuario_id', 1)
            )
            _invalidar_cache_notif()
            
            return jsonify({
                'success': True,
                'message': f'Se ha registrado la extensión de {cantidad} {tipoTexto}. El pago está pendiente.',
                'data': {
                    'nueva_fecha_inicio': fecha_inicio_nueva_str,
                    'nueva_fecha_fin': fecha_fin_nueva_str,
                    'monto_pendiente': monto_total
                }
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400

    # ──────────────────────────────────────────────────────────────────────────
    # ENDPOINT: Verificar si un cliente tiene pagos pendientes
    # Usado por el modal "Cambiar Plan" para bloquear el cambio si hay deuda
    # ──────────────────────────────────────────────────────────────────────────
    @app.route('/api/clientes/<int:cliente_id>/pagos-pendientes', methods=['GET'])
    @login_required
    def api_pagos_pendientes_cliente(cliente_id):
        """Verifica si el cliente tiene pagos pendientes"""
        try:
            resultado = cliente_dao.verificar_pagos_pendientes(cliente_id)
            return jsonify({
                'success': True,
                'data': resultado
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500


    # ──────────────────────────────────────────────────────────────────────────
    # ENDPOINT: Obtener última membresía activa de un cliente
    # Usado por el modal "Cambiar Plan" para obtener la fecha_fin como
    # fecha_inicio de la nueva membresía.
    # ──────────────────────────────────────────────────────────────────────────
    @app.route('/api/clientes/<int:cliente_id>/ultima-membresia', methods=['GET'])
    @login_required
    def api_ultima_membresia(cliente_id):
        """Devuelve la última membresía activa del cliente"""
        try:
            ultima = historial_membresia_dao.obtener_ultima_membresia(cliente_id)
            if ultima:
                return jsonify({
                    'success': True,
                    'data': {
                        'id': ultima.get('id'),
                        'plan_nombre': ultima.get('plan_nombre'),
                        'fecha_inicio': ultima.get('fecha_inicio'),
                        'fecha_fin': ultima.get('fecha_fin'),
                        'estado': ultima.get('estado')
                    }
                })
            else:
                return jsonify({
                    'success': False,
                    'data': None,
                    'message': 'No se encontró membresía activa'
                })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500

    # ──────────────────────────────────────────────────────────────────────────
    # ENDPOINT: Cambiar el plan de un cliente
    # Body JSON: { plan_codigo, fecha_inicio, qr_url }
    # Lógica:
    #   1. Finaliza la membresía activa actual (estado → 'terminada')
    #   2. Crea nuevo historial con fecha_inicio recibida y calcula fecha_fin
    #      según la duración del nuevo plan
    #   3. Actualiza plan_id y fecha_vencimiento en la tabla clientes
    #   4. Actualiza qr_code si se proporcionó
    # ──────────────────────────────────────────────────────────────────────────
    @app.route('/api/clientes/<int:cliente_id>/cambiar-plan', methods=['POST'])
    @login_required
    def api_cambiar_plan(cliente_id):
        """Cambia el plan de un cliente creando un nuevo historial de membresía"""

        try:
            data = request.get_json()
            plan_codigo = data.get('plan_codigo')
            fecha_inicio_str = data.get('fecha_inicio')
            qr_url = data.get('qr_url')

            if not plan_codigo:
                return jsonify({'success': False, 'message': 'El código del plan es requerido'}), 400
            if not fecha_inicio_str:
                return jsonify({'success': False, 'message': 'La fecha de inicio es requerida'}), 400

            # Verificar que el cliente existe
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if not cliente:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404

            # Obtener información del nuevo plan
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT id, nombre, duracion, precio FROM planes_membresia WHERE codigo = %s AND habilitado = 1',
                (plan_codigo,)
            )
            plan = cursor.fetchone()
            conn.close()

            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado o deshabilitado'}), 404

            plan_id      = plan['id']
            plan_precio  = float(plan['precio'])
            plan_duracion = plan['duracion'] or '1 mes'

            # Parsear fecha_inicio
            from datetime import timezone, timedelta as tz_delta
            peru_tz = timezone(tz_delta(hours=-5))
            ahora_peru = datetime.now(peru_tz)

            fecha_inicio_str_clean = fecha_inicio_str.strip()[:10]
            fecha_inicio = datetime.strptime(fecha_inicio_str_clean, '%Y-%m-%d').replace(
                hour=ahora_peru.hour,
                minute=ahora_peru.minute,
                second=ahora_peru.second
            )

            # Calcular fecha_fin
            duracion_dict = cliente_dao._parsear_duracion(plan_duracion)
            tipo     = duracion_dict.get('tipo', 'meses')
            cantidad = duracion_dict.get('cantidad', 1)

            if tipo == 'meses':
                mes_objetivo = fecha_inicio.month + cantidad
                año_objetivo = fecha_inicio.year
                while mes_objetivo > 12:
                    mes_objetivo -= 12
                    año_objetivo += 1
                ultimo_dia = calendar.monthrange(año_objetivo, mes_objetivo)[1]
                dia_final  = min(fecha_inicio.day, ultimo_dia)
                fecha_fin  = datetime(año_objetivo, mes_objetivo, dia_final,
                                    ahora_peru.hour, ahora_peru.minute, ahora_peru.second)
            elif tipo == 'horas':
                fecha_fin = fecha_inicio + timedelta(hours=cantidad)
            else:
                fecha_fin = fecha_inicio + timedelta(days=cantidad)

            fecha_inicio_db = fecha_inicio.strftime('%Y-%m-%d %H:%M:%S')
            fecha_fin_db    = fecha_fin.strftime('%Y-%m-%d %H:%M:%S')

            # Finalizar membresía activa
            ultima_membresia = historial_membresia_dao.obtener_ultima_membresia(cliente_id)
            if ultima_membresia:
                historial_membresia_dao.actualizar_estado(ultima_membresia['id'], 'terminada')

            # Crear nuevo historial
            nuevo_historial = {
                'cliente_id':    cliente_id,
                'plan_id':       plan_id,
                'fecha_inicio':  fecha_inicio_db,
                'fecha_fin':     fecha_fin_db,
                'monto_pagado':  plan_precio,
                'metodo_pago':   'pendiente',
                'estado':        'pendiente',
                'observaciones': f'CAMBIO DE PLAN: nuevo plan asignado - PENDIENTE DE PAGO',
                'usuario_id':    session.get('usuario_id')
            }
            historial_membresia_dao.crear_from_dict(nuevo_historial)

            # 2b. Registrar pago pendiente en la tabla pagos
            conn2 = get_connection()
            cursor2 = conn2.cursor()
            
            # Obtener timestamp en hora peruana
            timestamp_peru = obtener_timestamp_peru()
            
            cursor2.execute('''
                INSERT INTO pagos (cliente_id, plan_id, monto, metodo_pago,
                                usuario_registro, estado, fecha_pago)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                cliente_id,
                plan_id,
                plan_precio,
                'pendiente',
                session.get('usuario_id', 1),
                'pendiente',
                get_current_timestamp_peru_value()
            ))
            conn2.commit()
            conn2.close()

            # Actualizar cliente
            datos_actualizacion = {
                'plan_id':          plan_id,
                'fecha_vencimiento': fecha_fin.strftime('%Y-%m-%d %H:%M:%S')
            }
            if qr_url:
                datos_actualizacion['qr_code'] = qr_url

            cliente_dao.actualizar(cliente_id, datos_actualizacion)

            # Limpiar notificaciones de vencimiento (próximo y ya vencido) al asignar nuevo plan
            notificacion_dao.limpiar_notificaciones_vencimiento(cliente_id)
            _invalidar_cache_notif()

            return jsonify({
                'success': True,
                'message': 'Plan cambiado correctamente',
                'data': {
                    'plan_codigo':   plan_codigo,
                    'fecha_inicio':  fecha_inicio_db,
                    'fecha_fin':     fecha_fin_db
                }
            })

        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500

    @app.route('/api/reportes/cliente/<int:cliente_id>/historial-completo')
    @login_required
    def api_historial_completo_cliente(cliente_id):
        """API para obtener el historial completo de membresías de un cliente"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Obtener el historial de membresías
            cursor.execute('''
                SELECT 
                    hm.id,
                    hm.cliente_id,
                    c.nombre_completo as cliente_nombre,
                    p.nombre as plan_nombre,
                    hm.fecha_inicio,
                    hm.fecha_fin,
                    hm.monto_pagado,
                    hm.metodo_pago,
                    hm.estado,
                    hm.observaciones,
                    hm.fecha_registro,
                    u.nombre_completo as usuario_registro
                FROM historial_membresia hm
                JOIN clientes c ON hm.cliente_id = c.id
                JOIN planes_membresia p ON hm.plan_id = p.id
                LEFT JOIN usuarios u ON hm.usuario_id = u.id
                WHERE hm.cliente_id = %s
                ORDER BY hm.id DESC
            ''', (cliente_id,))
            
            historial = cursor.fetchall()
            conn.close()
            
            # Usar nuevo método del DAO para obtener pagos
            pagos = cliente_dao.obtener_pagos_por_cliente(cliente_id, solo_completados=True)
            
            # Verificar si tiene pago pendiente explícito en la BD
            pagos_pendientes = cliente_dao.verificar_pagos_pendientes(cliente_id)
            tiene_pendiente = pagos_pendientes.get('tiene_pendiente', False)
            
            historial_list = []
            
            for idx, row in enumerate(historial):
                historial_id = row['id']
                fecha_inicio = row['fecha_inicio']
                fecha_fin = row['fecha_fin']
                estado_original = row['estado']

                # Verificar si el registro SIGUIENTE (más reciente, idx-1) es un CAMBIO DE PLAN
                # historial está en DESC, entonces idx-1 es más reciente
                es_previo_a_cambio_plan = False
                if idx > 0:
                    obs_siguiente = (historial[idx - 1].get('observaciones') or '').upper()
                    if 'CAMBIO DE PLAN' in obs_siguiente:
                        es_previo_a_cambio_plan = True

                # Calcular el estado correcto
                estado_calculado, metodo_calculado = calcular_estado_membresia(
                    historial_id=historial_id,
                    cliente_id=cliente_id,
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                    estado_original=estado_original,
                    pagos=pagos,
                    es_ultimo=(idx == 0),
                    es_previo_a_cambio_plan=es_previo_a_cambio_plan,
                    tiene_pendiente=tiene_pendiente
                )
                
                historial_list.append({
                    'id': row['id'],
                    'cliente_id': row['cliente_id'],
                    'cliente_nombre': row['cliente_nombre'],
                    'plan_nombre': row['plan_nombre'],
                    'fecha_inicio': str(row['fecha_inicio'])[:10] if row['fecha_inicio'] else '',
                    'fecha_fin': str(row['fecha_fin'])[:10] if row['fecha_fin'] else '',
                    'monto_pagado': float(row['monto_pagado']) if row['monto_pagado'] else 0,
                    'metodo_pago': metodo_calculado,
                    'estado': estado_calculado,
                    'observaciones': row['observaciones'],
                    'fecha_registro': str(row['fecha_registro']) if row['fecha_registro'] else '',
                    'usuario_registro': row['usuario_registro']
                })
            
            return jsonify({'success': True, 'data': historial_list})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400


# ==========================================
# CONTROLADOR: PRODUCTOS
# ==========================================

def init_productos_controller(app):
    """Inicializa las rutas de productos"""
    
    @app.route('/productos')
    @login_required
    @permiso_required('productos')

    def productos():
        """Lista de productos"""
        productos = producto_dao.obtener_todos()
        categorias = producto_dao.obtener_categorias()
        return render_template('productos.html', productos=productos, categorias=categorias)
    
    @app.route('/api/productos')
    @login_required
    def api_listar_productos():
        """API para listar productos"""
        productos = producto_dao.obtener_todos()
        return jsonify({'success': True, 'data': productos})
    
    @app.route('/api/productos/<int:producto_id>', methods=['GET'])
    @login_required
    def api_obtener_producto(producto_id):
        """API para obtener un producto por ID"""
        producto = producto_dao.obtener_por_id(producto_id)

        if producto:
            return jsonify({'success': True, 'data': producto})
        else:
            return jsonify({
                'success': False,
                'message': 'Producto no encontrado'
            }), 404
        
    @app.route('/api/productos', methods=['POST'])
    @login_required
    def api_crear_producto():
        """API para crear producto"""
        try:
            data = request.get_json()
            data['usuario_id'] = session.get('usuario_id', 1)
            
            # Verificar que no sea None
            if data['usuario_id'] is None:
                data['usuario_id'] = 1
            producto_id = producto_dao.crear_from_dict(data)
            return jsonify({
                'success': True,
                'message': 'Producto creado exitosamente',
                'producto_id': producto_id
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/productos/<int:producto_id>', methods=['PUT'])
    @login_required
    def api_actualizar_producto(producto_id):
        """API para actualizar producto"""
        try:
            data = request.get_json()
            data['usuario_id'] = session.get('usuario_id', 1)
            
            # Verificar que no sea None
            if data['usuario_id'] is None:
                data['usuario_id'] = 1
            producto_dao.actualizar(producto_id, data)
            return jsonify({'success': True, 'message': 'Producto actualizado'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/productos/<int:producto_id>', methods=['DELETE'])
    @login_required
    def api_eliminar_producto(producto_id):
        """API para eliminar producto"""
        try:
            producto_dao.eliminar(producto_id)
            return jsonify({'success': True, 'message': 'Producto eliminado'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/productos/stock-bajo')
    @login_required
    def api_productos_stock_bajo():
        """API para productos con stock bajo"""
        productos = producto_dao.obtener_con_stock_bajo()
        return jsonify({'success': True, 'data': productos})

    @app.route('/api/productos/<int:id>/stock', methods=['POST'])
    
    def actualizar_stock_producto(id):
        data = request.get_json()
        cantidad = int(data.get('cantidad', 0))
        tipo = data.get('tipo_movimiento')
        costo_uni = float(data.get('costo_unitario', 0))
        nota = data.get('nota')
        
        # Calculamos el Costo Total
        cantidad_absoluta = abs(cantidad)
        costo_total = cantidad_absoluta * costo_uni
        
        usuario_actual = session.get('usuario') or 'Sistema'

        conn = get_connection()
        cursor = conn.cursor()
        try:
            # 1. Actualizar stock en productos
            cursor.execute('UPDATE productos SET stock = stock + %s WHERE id = %s', (cantidad, id))

            # 2. Registrar entrada con el nuevo campo costo_total
            if tipo == 'entrada':
                # Obtener timestamp en hora peruana
                timestamp_peru = obtener_timestamp_peru()
                
                cursor.execute('''
                    INSERT INTO entradas_inventario 
                    (producto_id, cantidad, costo_unitario, costo_total, observaciones, usuario_registro, fecha_entrada)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (id, cantidad_absoluta, costo_uni, costo_total, nota, usuario_actual, timestamp_peru))

            conn.commit()
            return jsonify({'success': True, 'message': 'Movimiento y costos registrados'})
        except Exception as e:
            conn.rollback()
            return jsonify({'success': False, 'message': str(e)})
        finally:
            conn.close()

    @app.route('/api/productos/<int:id>/eliminar', methods=['POST'])
    def eliminar_producto(id):
        try:
            # Llamamos a tu método que pone el estado en "eliminado"
            exito = producto_dao.eliminar(id)
            
            if exito:
                return jsonify({'success': True, 'message': 'Producto eliminado correctamente'})
            else:
                return jsonify({'success': False, 'message': 'No se pudo eliminar'}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
    
    @app.route('/api/productos/<int:producto_id>/historial_entradas')
    @login_required
    @permiso_required('productos')
    def api_historial_entradas_producto(producto_id):
        """API para obtener el historial de entradas de inventario de un producto"""
        try:
            historial = producto_dao.obtener_historial_entradas(producto_id)
            return jsonify({
                'success': True,
                'data': historial
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
# ==========================================
# CONTROLADOR: PAGOS
# ==========================================

def init_pagos_controller(app):
    """Inicializa las rutas de pagos"""
    
    @app.route('/pagos')
    @login_required
    def pagos():
        """Página de pagos"""
        clientes = cliente_dao.obtener_todos()
        return render_template('pagos.html', clientes=clientes)
    
    @app.route('/api/pagos')
    @login_required
    def api_listar_pagos():
        """API para listar clientes con información de pagos, con opción de filtrar por estado"""
        filtro = request.args.get('filtro', 'todos')
        
        # Método optimizado: devuelve ha_pagado, tiene_pendiente, estado_pago, dias_mora, envia_whatsapp
        clientes_filtrados = cliente_dao.obtener_clientes_para_pagos_optimizado(filtro)  # ← LÍNEA NUEVA
        
        return jsonify({'success': True, 'data': clientes_filtrados})
    
    @app.route('/api/pagos/stats')
    @login_required
    def api_pagos_stats():
        """API para obtener estadísticas de pagos"""
        try:
            stats = cliente_dao.obtener_estadisticas_pagos()
            
            return jsonify({
                'success': True,
                'data': {
                    'total_pendiente': stats['total_pendiente'],
                    'total_vencido': stats['total_vencido'],
                    'clientes_pendientes': stats['clientes_pendientes']
                }
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
        
    @app.route('/api/pagos/<int:cliente_id>/pagar', methods=['POST'])
    @login_required
    def api_pagar_cliente(cliente_id):
        """API para registrar el pago de un cliente"""
        try:
            data = request.get_json() or {}
            metodo_pago = data.get('metodo_pago', 'efectivo')
            usuario_id = session.get('usuario_id', 1)
            monto = data.get('monto')  # Precio con descuento enviado desde el frontend

            resultado = cliente_dao.registrar_pago_cliente(cliente_id, metodo_pago, usuario_id, monto_override=monto)
            
            if resultado['success']:
                # Obtener cliente para la notificación
                cliente = cliente_dao.obtener_por_id(cliente_id)
                
                # Generar notificación
                notificacion_dao.crear_notificacion(
                    tipo='payment',
                    titulo='Pago recibido',
                    mensaje=f'{cliente["nombre_completo"]} realizó un pago',
                    cliente_id=cliente_id,
                    usuario_id=session.get('usuario_id', 1)
                )
                # Limpiar notificaciones de vencimiento (próximo y ya vencido)
                notificacion_dao.limpiar_notificaciones_vencimiento(cliente_id)
                _invalidar_cache_notif()  # forzar recarga inmediata en el próximo fetch
            
            return jsonify(resultado)
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400


# ==========================================
# CONTROLADOR: PERSONAL
# ==========================================

def init_personal_controller(app):
    """Inicializa las rutas de personal"""
    
    @app.route('/personal')
    @login_required
    @feature_required('empleados')
    @permiso_required('personal')
    def personal():
        """Lista de personal"""
        usuarios = usuario_dao.obtener_todos()
        
        # Calcular roles únicos
        roles = list(set(u.get('rol', '') for u in usuarios if u.get('rol')))
        
        # Calcular nuevos este mes
        hoy = datetime.now()
        primer_dia_mes = datetime(hoy.year, hoy.month, 1)

        def get_date_as_datetime(fecha_registro):
            """Convierte fecha_registro a datetime object"""
            if not fecha_registro:
                return None
            # Si ya es un objeto datetime (MySQL), usarlo directamente
            if isinstance(fecha_registro, datetime):
                return fecha_registro
            # Si es string, convertirlo
            if isinstance(fecha_registro, str):
                # Manejar diferentes formatos de fecha
                try:
                    # Intentar con formato datetime completo
                    return datetime.strptime(fecha_registro[:19], '%Y-%m-%d %H:%M:%S')
                except:
                    try:
                        # Intentar solo fecha
                        return datetime.strptime(fecha_registro[:10], '%Y-%m-%d')
                    except:
                        return None
            return None
        
        nuevos_mes = sum(1 for u in usuarios 
                        if u.get('fecha_registro') and 
                        get_date_as_datetime(u['fecha_registro']) >= primer_dia_mes)
        
        usuario_inicial_id = usuario_dao.obtener_id_usuario_inicial()
        return render_template('personal.html', usuarios=usuarios, roles=roles, nuevos_mes=nuevos_mes, usuario_inicial_id=usuario_inicial_id)
    
    @app.route('/api/usuarios')
    @login_required
    
    def api_listar_usuarios():
        """API para listar usuarios"""
        usuarios = usuario_dao.obtener_todos()
        return jsonify({'success': True, 'data': usuarios})
    
    @app.route('/api/usuarios', methods=['POST'])
    @login_required
    def api_crear_usuario():
        """API para crear usuario (genera username y password automáticamente)"""
        try:
            data = request.get_json()
            
            # Validar datos requeridos (comunes para empleado y entrenador)
            required_fields = ['dni', 'nombre_completo', 'telefono']
            for field in required_fields:
                if not data.get(field):
                    return jsonify({'success': False, 'message': f'El campo {field} es obligatorio'}), 400
            
            # Determinar si es empleado (tiene rol_id) o entrenador (sin rol_id)
            es_empleado = bool(data.get('rol_id'))
            
            # Si es empleado, rol_id es obligatorio
            if not es_empleado and data.get('rol_id') is not None:
                return jsonify({'success': False, 'message': 'El campo rol_id es obligatorio para empleados'}), 400
            
            # Si es entrenador, asegurarse que rol_id, username y password sean NULL
            if not es_empleado:
                data['rol_id'] = None
                data['username'] = None
                data['password'] = None
            
            # Verificar si el DNI ya existe
            usuario_existente = usuario_dao.obtener_por_dni(data['dni'])
            if usuario_existente and usuario_existente.get('estado') != 'eliminado':
                return jsonify({'success': False, 'message': 'Ya existe un usuario con este DNI'}), 400
            
            if es_empleado:
                # === SOLO EMPLEADOS: GENERAR USERNAME AUTOMÁTICAMENTE ===
                nombre_completo = data['nombre_completo'].strip()
                dni = data['dni'].strip()
                
                # Separar nombre y apellidos
                partes = nombre_completo.split()
                primer_nombre = partes[0].lower() if len(partes) > 0 else ""
                apellido = ""
                
                if len(partes) > 1:
                    # Tomar el último como apellido
                    apellido = partes[-1].lower()
                elif len(partes) == 1:
                    apellido = primer_nombre
                
                # Limpiar caracteres especiales
                import re
                apellido_limpio = re.sub(r'[^a-z]', '', apellido)
                primer_nombre_limpio = re.sub(r'[^a-z]', '', primer_nombre)
                
                # Tomar primera letra del primer nombre y apellido completo
                if primer_nombre_limpio and apellido_limpio:
                    base_username = f"{primer_nombre_limpio[0]}{apellido_limpio}"
                else:
                    base_username = f"user{dni[-4:]}"
                
                # Asegurar que no exista el username
                contador = 1
                username = base_username
                while usuario_dao.obtener_por_username(username):
                    username = f"{base_username}{contador}"
                    contador += 1
                    if contador > 100:  # Límite de seguridad
                        username = f"user{dni}{contador}"
                        break
                
                # === GENERAR PASSWORD SEGURO AUTOMÁTICAMENTE ===
                import secrets
                import string
                
                # Crear password con: 2 mayúsculas, 2 minúsculas, 2 dígitos, 2 caracteres especiales
                mayusculas = ''.join(secrets.choice(string.ascii_uppercase) for _ in range(2))
                minusculas = ''.join(secrets.choice(string.ascii_lowercase) for _ in range(2))
                digitos = ''.join(secrets.choice(string.digits) for _ in range(2))
                especiales = ''.join(secrets.choice('!@#$%^&*') for _ in range(2))
                
                # Combinar y mezclar
                password_chars = list(mayusculas + minusculas + digitos + especiales)
                secrets.SystemRandom().shuffle(password_chars)
                password = ''.join(password_chars)
                
                # Agregar credenciales generadas
                data['username'] = username
                data['password'] = password
            else:
                # Entrenador: sin acceso al sistema
                username = None
                password = None
            
            data['usuario_creador_id'] = session.get('usuario_id', 1)

            fecha_formulario = data.get('fecha_registro')
            if fecha_formulario:
                # La fecha viene como YYYY-MM-DD desde el frontend
                # Convertir a datetime y agregar hora actual
                try:
                    # Parsear la fecha del formulario
                    fecha_sin_hora = datetime.strptime(fecha_formulario, '%Y-%m-%d')
                    
                    # Obtener hora actual
                    hora_actual = datetime.now().time()
                    
                    # Combinar fecha del formulario con hora actual
                    fecha_con_hora = datetime.combine(fecha_sin_hora.date(), hora_actual)
                    
                    # Guardar en formato string con hora
                    data['fecha_registro'] = fecha_con_hora.strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    # Si hay error, usar fecha y hora actual
                    data['fecha_registro'] = get_current_timestamp_peru_value()
            else:
                # Si no viene fecha, usar fecha y hora actual
                data['fecha_registro'] = get_current_timestamp_peru_value()
            
            # Estado por defecto
            if not data.get('estado'):
                data['estado'] = 'activo'
            
            usuario_id = usuario_dao.crear_from_dict(data)
            
            # Obtener el usuario creado
            usuario_creado = usuario_dao.obtener_por_id(usuario_id)
            
            respuesta = {
                'success': True,
                'message': 'Usuario creado exitosamente',
                'usuario_id': usuario_id,
                'usuario': usuario_creado
            }
            # Solo enviar credenciales si es empleado
            if es_empleado:
                respuesta['username'] = username
                respuesta['password'] = password
            
            return jsonify(respuesta)
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/usuarios/<int:usuario_id>', methods=['PUT'])
    @login_required
    def api_actualizar_usuario(usuario_id):
        """API para actualizar usuario (NO permite cambiar username/password)"""
        try:
            data = request.get_json()
            data['usuario_creador_id'] = session.get('usuario_id', 1)
            
            # NO permitir cambiar username o password desde aquí
            if 'username' in data:
                del data['username']
            if 'password' in data:
                del data['password']
            
            # Obtener el usuario actual para saber si es entrenador o empleado
            usuario_actual = usuario_dao.obtener_por_id(usuario_id)
            if not usuario_actual:
                return jsonify({'success': False, 'message': 'Usuario no encontrado'}), 404
            
            es_entrenador = not usuario_actual.get('rol_id')
            
            if es_entrenador:
                # Entrenador: nunca se le puede asignar rol_id, username ni password
                data.pop('rol_id', None)
                data.pop('username', None)
                data.pop('password', None)
            else:
                # Empleado: si viene rol_id vacío o None, rechazar
                if 'rol_id' in data and not data.get('rol_id'):
                    return jsonify({'success': False, 'message': 'El rol es obligatorio para empleados'}), 400
            
            usuario_dao.actualizar(usuario_id, data)
            
            return jsonify({'success': True, 'message': 'Usuario actualizado'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/usuarios/<int:usuario_id>/estado', methods=['PUT'])
    @login_required
    def api_cambiar_estado_usuario(usuario_id):
        """API para cambiar estado de usuario (activo/inactivo)"""
        try:
            data = request.get_json()
            nuevo_estado = data.get('estado')
            
            if nuevo_estado not in ['activo', 'inactivo']:
                return jsonify({'success': False, 'message': 'Estado no válido. Use "activo" o "inactivo"'}), 400
            
            usuario_dao.cambiar_estado(usuario_id, nuevo_estado)
            
            # Obtener usuario actualizado
            usuario = usuario_dao.obtener_por_id(usuario_id)
            return jsonify({
                'success': True,
                'message': f'Usuario {nuevo_estado} correctamente',
                'estado': usuario['estado']
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        
    @app.route('/api/usuarios/<int:usuario_id>/toggle-estado', methods=['PUT'])
    @login_required
    def api_toggle_estado_usuario(usuario_id):
        """API para alternar estado entre activo/inactivo"""
        try:
            exito = usuario_dao.toggle_estado(usuario_id)
            
            if not exito:
                return jsonify({'success': False, 'message': 'Usuario no encontrado'}), 404
            
            # Obtener usuario actualizado
            usuario = usuario_dao.obtener_por_id(usuario_id)
            return jsonify({
                'success': True,
                'message': f'Usuario {usuario["estado"]} correctamente',
                'estado': usuario['estado']
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400   


    @app.route('/api/usuarios/<int:usuario_id>', methods=['DELETE'])
    @login_required
    def api_eliminar_usuario(usuario_id):
        """API para eliminar usuario (soft delete - estado = 'eliminado')"""
        try:
            usuario_dao.eliminar(usuario_id)
            return jsonify({
                'success': True, 
                'message': 'Usuario eliminado correctamente'
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/usuarios/<int:usuario_id>/restaurar', methods=['PUT'])
    @login_required
    def api_restaurar_usuario(usuario_id):
        """API para restaurar un usuario eliminado"""
        try:
            # Primero verificar que exista y esté eliminado
            usuario = usuario_dao.obtener_por_id(usuario_id)
            if not usuario:
                return jsonify({'success': False, 'message': 'Usuario no encontrado'}), 404
            
            if usuario.get('estado') != 'eliminado':
                return jsonify({'success': False, 'message': 'El usuario no está eliminado'}), 400
            
            # Restaurar a estado activo
            usuario_dao.activar(usuario_id)
            return jsonify({'success': True, 'message': 'Usuario restaurado correctamente'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400


    @app.route('/api/usuarios/enviar-email', methods=['POST'])
    @login_required
    def enviar_email_credenciales():
        try:
            data = request.get_json()
            email_destino = data.get('email')
            username = data.get('username')
            password = data.get('password')

            if not email_destino:
                return jsonify({'success': False, 'message': 'El correo es obligatorio'}), 400

            # Crear el mensaje
            config = configuracion_dao.obtener_actual()
            nombre_empresa = config.get('empresa_nombre', 'Sistema') if config else 'Sistema'
            
            msg = Message(f"Tus Credenciales de Acceso - {nombre_empresa}",
                        recipients=[email_destino])
            
            msg.body = f"""
            Hola,
            
            Se ha creado tu cuenta de acceso para el sistema de {nombre_empresa}.
            
            Tus credenciales son:
            Usuario: {username}
            Contraseña: {password}
            
            Por favor, inicia sesión y cambia tu contraseña por seguridad.
            """
            
            # Enviar el correo
            mail.send(msg)

            return jsonify({
                'success': True, 
                'message': f'Credenciales enviadas a {email_destino}'
            })

        except Exception as e:
            print(f"Error: {str(e)}")
            return jsonify({'success': False, 'message': 'No se pudo enviar el correo'}), 500

    @app.route('/api/usuarios/<int:usuario_id>/actualizar-email', methods=['POST'])
    @login_required
    def actualizar_email_usuario(usuario_id):
        try:
            data = request.get_json()
            nuevo_email = data.get('email')
            
            if not nuevo_email:
                return jsonify({'success': False, 'message': 'Email requerido'}), 400

            conn = get_connection()
            cursor = conn.cursor()
            
            # Actualizamos directamente el email en la tabla usuarios
            cursor.execute('''
                UPDATE usuarios 
                SET email = %s 
                WHERE id = %s
            ''', (nuevo_email, usuario_id))
            
            conn.commit()
            conn.close()
            
            return jsonify({'success': True, 'message': 'Email actualizado en la base de datos'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

# ==========================================
# CONTROLADOR: ROLES
# ==========================================

def init_roles_controller(app):
    """Inicializa las rutas de gestión de roles"""
    
    @app.route('/roles')
    @login_required
    @feature_required('empleados')
    @permiso_required('roles')
    def roles():
        """Página de gestión de roles"""
        roles_lista = rol_dao.obtener_todos()
        vistas_disponibles = rol_dao.obtener_vistas_disponibles()
        return render_template('roles.html', roles=roles_lista, vistas=vistas_disponibles)
    
    @app.route('/api/roles')
    @login_required
    def api_listar_roles():
        """API para listar roles con conteo de usuarios"""
        roles_lista = rol_dao.obtener_todos()
        
        # Agregar conteo de usuarios a cada rol
        for rol in roles_lista:
            rol['usuarios_count'] = rol_dao.contar_usuarios_por_rol(rol['id'])
        
        return jsonify({'success': True, 'data': roles_lista})
    
    @app.route('/api/roles/<int:rol_id>', methods=['GET'])
    @login_required
    def api_obtener_rol(rol_id):
        """API para obtener un rol específico"""
        rol = rol_dao.obtener_por_id(rol_id)
        if rol:
            # Parsear permisos JSON
            if rol.get('permisos'):
                try:
                    rol['permisos'] = json.loads(rol['permisos'])
                except:
                    rol['permisos'] = []
            return jsonify({'success': True, 'data': rol})
        return jsonify({'success': False, 'message': 'Rol no encontrado'}), 404
    
    @app.route('/api/roles', methods=['POST'])
    @login_required
    def api_crear_rol():
        """API para crear un nuevo rol"""
        try:
            data = request.get_json()
            nombre = data.get('nombre')
            descripcion = data.get('descripcion', '')
            permisos = data.get('permisos', [])
            
            if not nombre:
                return jsonify({'success': False, 'message': 'El nombre del rol es obligatorio'}), 400
            
            # Verificar si el rol ya existe
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM roles WHERE nombre = %s AND estado != 'eliminado'", (nombre,))
            if cursor.fetchone():
                conn.close()
                return jsonify({'success': False, 'message': 'Ya existe un rol con ese nombre'}), 400
            conn.close()
            
            # Obtener el ID del usuario actual desde la sesión
            usuario_creador_id = session.get('  usuario_id', 1)
            
            # Crear el rol con el usuario_creador_id
            rol_id = rol_dao.crear(nombre, descripcion, permisos, usuario_creador_id)
            
            return jsonify({
                'success': True,
                'message': 'Rol creado exitosamente',
                'rol_id': rol_id
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/roles/<int:rol_id>', methods=['PUT'])
    @login_required
    def api_actualizar_rol(rol_id):
        """API para actualizar un rol"""
        try:
            data = request.get_json()
            
            # Verificar que el rol existe
            rol = rol_dao.obtener_por_id(rol_id)
            if not rol:
                return jsonify({'success': False, 'message': 'Rol no encontrado'}), 404
            
            # Verificar nombre único si se cambió
            if 'nombre' in data and data['nombre'] != rol['nombre']:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM roles WHERE nombre = %s AND id != %s AND estado != 'eliminado'", 
                             (data['nombre'], rol_id))
                if cursor.fetchone():
                    conn.close()
                    return jsonify({'success': False, 'message': 'Ya existe un rol con ese nombre'}), 400
                conn.close()

            usuario_creador_id = session.get('usuario_id', 1)
            
            # Agregar usuario_creador_id a los datos que se van a actualizar
            data['usuario_creador_id'] = usuario_creador_id
            
            # Actualizar el rol
            rol_dao.actualizar(rol_id, data)
            
            return jsonify({'success': True, 'message': 'Rol actualizado correctamente'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/roles/<int:rol_id>/toggle-activo', methods=['PUT'])
    @login_required
    def api_toggle_activo_rol(rol_id):
        """API para alternar estado activo/inactivo de un rol"""
        try:
            rol = rol_dao.obtener_por_id(rol_id)
            if not rol:
                return jsonify({'success': False, 'message': 'Rol no encontrado'}), 404
            
            if rol['estado'] == 'activo':
                rol_dao.desactivar(rol_id)
                nuevo_estado = 'inactivo'
                mensaje = 'Rol desactivado correctamente'
            elif rol['estado'] == 'inactivo':
                rol_dao.activar(rol_id)
                nuevo_estado = 'activo'
                mensaje = 'Rol activado correctamente'
            else:
                return jsonify({'success': False, 'message': 'No se puede cambiar estado de un rol eliminado'}), 400
            
            return jsonify({
                'success': True,
                'message': mensaje,
                'estado': nuevo_estado
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/roles/<int:rol_id>/permisos', methods=['PUT'])
    @login_required
    def api_actualizar_permisos_rol(rol_id):
        """API para actualizar los permisos de un rol"""
        try:
            data = request.get_json()
            permisos = data.get('permisos', [])
            
            # Validar que el rol existe
            rol = rol_dao.obtener_por_id(rol_id)
            if not rol:
                return jsonify({'success': False, 'message': 'Rol no encontrado'}), 404
            
            # Actualizar permisos
            rol_dao.actualizar_permisos(rol_id, permisos)
            
            return jsonify({'success': True, 'message': 'Permisos actualizados correctamente'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/roles/<int:rol_id>', methods=['DELETE'])
    @login_required
    def api_eliminar_rol(rol_id):
        """API para eliminar un rol permanentemente"""
        try:
            # Verificar que el rol existe
            rol = rol_dao.obtener_por_id(rol_id)
            if not rol:
                return jsonify({'success': False, 'message': 'Rol no encontrado'}), 404
            
            # Contar usuarios con este rol para informar al frontend
            usuarios_con_rol = rol_dao.contar_usuarios_por_rol(rol_id)
            
            # Obtener datos del rol para el frontend
            return jsonify({
                'success': True,
                'data': {
                    'rol_id': rol_id,
                    'rol_nombre': rol['nombre'],
                    'usuarios_con_rol': usuarios_con_rol,
                    'requiere_confirmacion': usuarios_con_rol > 0
                }
            })
            
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/roles/<int:rol_id>/confirmar-eliminacion', methods=['POST'])
    @login_required
    def api_confirmar_eliminacion_rol(rol_id):
        """API para confirmar y ejecutar la eliminación de un rol"""
        try:
            data = request.get_json()
            nuevo_rol_id = data.get('nuevo_rol_id')  # Puede ser null
            
            # Ejecutar la eliminación con posible reasignación
            resultado = rol_dao.eliminar(rol_id, nuevo_rol_id)
            
            mensaje = f'Rol eliminado correctamente'
            if resultado['usuarios_afectados'] > 0:
                if nuevo_rol_id:
                    mensaje += f'. {resultado["usuarios_afectados"]} usuario(s) fueron reasignados.'
                else:
                    mensaje += f'. {resultado["usuarios_afectados"]} usuario(s) quedaron sin rol asignado.'
            
            return jsonify({
                'success': True,
                'message': mensaje,
                'usuarios_afectados': resultado['usuarios_afectados']
            })
            
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/roles/para-reasignacion/<int:rol_id_excluir>')
    @login_required
    def api_roles_para_reasignacion(rol_id_excluir):
        """API para obtener roles disponibles para reasignación"""
        try:
            roles = rol_dao.obtener_roles_para_reasignacion(rol_id_excluir)
            return jsonify({'success': True, 'data': roles})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/roles/vistas-disponibles')
    @login_required
    def api_vistas_disponibles():
        """API para obtener las vistas disponibles del sistema"""
        vistas = rol_dao.obtener_vistas_disponibles()
        return jsonify({'success': True, 'data': vistas})
    
def feature_required(feature):
    """Decorador para verificar que una funcionalidad esté habilitada en configuraciones"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT funcionalidades_habilitadas FROM configuraciones ORDER BY id DESC LIMIT 1')
                row = cursor.fetchone()
                conn.close()
                if row:
                    val = row.get('funcionalidades_habilitadas') if isinstance(row, dict) else row[0]
                    if val:
                        features = json.loads(val)
                        if feature not in features:
                            if request.is_json:
                                return jsonify({'success': False, 'message': f'La funcionalidad "{feature}" no está habilitada'}), 403
                            return redirect(url_for('dashboard'))
            except Exception as e:
                print(f"Error al verificar feature '{feature}': {e}")
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def permiso_required(vista):
    """Decorador para verificar permisos de vista"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                if request.is_json:
                    return jsonify({'success': False, 'message': 'Sesión expirada', 'redirect': '/login'}), 401
                return redirect(url_for('login'))
            
            # Administradores tienen acceso total
            if session.get('rol', '').lower() == 'administrador':
                return f(*args, **kwargs)
            
            # Verificar permisos del usuario
            usuario_id = session.get('usuario_id')
            if usuario_id:
                # Obtener rol del usuario
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT r.permisos FROM usuarios u
                    JOIN roles r ON u.rol_id = r.id
                    WHERE u.id = %s AND u.estado = 'activo' AND r.estado = 'activo'
                ''', (usuario_id,))
                resultado = cursor.fetchone()
                conn.close()
                
                if resultado:
                    try:
                        val = resultado['permisos'] if isinstance(resultado, dict) else resultado[0]
                        permisos = json.loads(val) if val else []
                        if vista in permisos:
                            return f(*args, **kwargs)
                    except:
                        pass
            
            # No tiene permiso
            if request.is_json:
                return jsonify({'success': False, 'message': 'No tienes permiso para acceder a esta vista'}), 403

            
            return redirect(url_for('perfil'))  # <--- CAMBIA ESTO
        return decorated_function
    return decorator

# ==========================================
# CONTROLADOR: PLANES DE MEMBRESÍA
# ==========================================

def init_planes_controller(app):
    """Inicializa las rutas de planes de membresía"""
    
    @app.route('/membresias')
    @login_required
    def membresias():
        """Página de membresías con planes dinámicos y precios con descuento"""
        eliminadas = promocion_dao.eliminar_promociones_vencidas()
        if eliminadas > 0:
            print(f"[Promociones] Se eliminaron {eliminadas} promoción(es) vencida(s) automáticamente")
        
        planes = plan_dao.obtener_todos()
        
        if planes is None:
            planes = []
        
        stats = {}
        for plan in planes:
            stats[plan['id']] = plan_dao.contar_clientes(plan['id'])
            
            precio_original = float(plan['precio'])
            promocion = promocion_dao.obtener_promocion_principal(plan['id'])
            
            # Inicializar valores por defecto
            plan['precio_original'] = precio_original
            plan['precio_con_descuento'] = precio_original
            plan['descuento'] = 0
            plan['promocion'] = None
            plan['tiene_promocion'] = False
            
            if promocion:
                # VERIFICAR TIPO DE PROMOCIÓN
                tipo_promocion = promocion.get('tipo_promocion', 'normal')
                
                if tipo_promocion == '2x1':
                    # Para promoción 2x1, mostrar el precio 2x1 en el badge
                    precio_2x1 = float(promocion.get('precio_2x1', 0))
                    plan['tiene_promocion'] = True
                    plan['promocion'] = promocion
                    plan['descuento'] = (precio_original * 2) - precio_2x1 if precio_2x1 > 0 else 0
                    # El precio mostrado sigue siendo el original
                    plan['precio_con_descuento'] = precio_original
                    
                else:
                    # Promoción normal (porcentaje o monto)
                    descuento = 0
                    if promocion.get('porcentaje_descuento'):
                        descuento = precio_original * (float(promocion['porcentaje_descuento']) / 100)
                    elif promocion.get('monto_descuento'):
                        descuento = float(promocion['monto_descuento'])
                    
                    precio_descuento = precio_original - descuento
                    if precio_descuento < 0:
                        precio_descuento = 0
                    
                    plan['precio_con_descuento'] = precio_descuento
                    plan['descuento'] = descuento
                    plan['promocion'] = promocion
                    plan['tiene_promocion'] = True
        
        return render_template('membresias.html', planes=planes, stats=stats)
    
    @app.route('/api/planes')
    @login_required
    def api_listar_planes():
        """API para listar todos los planes"""
        planes = plan_dao.obtener_todos()
        # Agregar conteo de clientes a cada plan
        for plan in planes:
            plan['clientes_count'] = plan_dao.contar_clientes(plan['id'])
            plan['permite_aplazamiento'] = plan.get('permite_aplazamiento', 0) == 1
        return jsonify({'success': True, 'data': planes})
    
    @app.route('/api/planes', methods=['POST'])
    @login_required
    def api_crear_plan():
        """API para crear un nuevo plan"""
        try:
            data = request.get_json()
            
            # Validar campos requeridos
            required_fields = ['codigo', 'nombre', 'precio', 'duracion']
            for field in required_fields:
                if not data.get(field):
                    return jsonify({'success': False, 'message': f'El campo {field} es obligatorio'}), 400
            
            # Verificar código único
            if plan_dao.existe_codigo(data['codigo']):
                return jsonify({'success': False, 'message': 'Ya existe un plan con ese código'}), 400
            
            # Agregar el usuario_id de la sesión actual
            data['usuario_id'] = session.get('usuario_id', 1)
            
            # Verificar que no sea None
            if data['usuario_id'] is None:
                data['usuario_id'] = 1
            plan_id = plan_dao.crear_from_dict(data)
            return jsonify({
                'success': True,
                'message': 'Plan creado exitosamente',
                'plan_id': plan_id
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/planes/<int:plan_id>', methods=['GET'])
    @login_required
    def api_obtener_plan(plan_id):
        """API para obtener un plan específico por ID"""
        try:
            plan = plan_dao.obtener_por_id(plan_id)
            if plan:
                return jsonify({'success': True, 'data': plan})
            else:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    

    @app.route('/api/planes/<int:plan_id>', methods=['PUT'])
    @login_required
    def api_actualizar_plan(plan_id):
        """API para actualizar un plan"""
        try:
            data = request.get_json()
            
            # Verificar que el plan existe
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404
            
            # Verificar código único si se cambió
            if 'codigo' in data and data['codigo'] != plan['codigo']:
                if plan_dao.existe_codigo(data['codigo'], exclude_id=plan_id):
                    return jsonify({'success': False, 'message': 'Ya existe un plan con ese código'}), 400
            
            data['usuario_id'] = session.get('usuario_id', 1)
            
            # Verificar que no sea None
            if data['usuario_id'] is None:
                data['usuario_id'] = 1
                
            plan_dao.actualizar(plan_id, data)
            return jsonify({'success': True, 'message': 'Plan actualizado correctamente'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/planes/<int:plan_id>/toggle', methods=['PUT'])
    @login_required
    def api_toggle_plan(plan_id):
        """API para activar/desactivar un plan"""
        try:
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404
            
            plan_dao.toggle_habilitado(plan_id)
            
            # Obtener estado actualizado
            plan_actualizado = plan_dao.obtener_por_id(plan_id)
            estado = 'habilitado' if plan_actualizado['habilitado'] else 'deshabilitado'
            
            return jsonify({
                'success': True,
                'message': f'Plan {estado} correctamente',
                'habilitado': plan_actualizado['habilitado']
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/planes/<int:plan_id>', methods=['DELETE'])
    @login_required
    def api_eliminar_plan(plan_id):
        """API para eliminar un plan (eliminación lógica)"""
        try:
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404
            
            # Nota: Se permite eliminación lógica aunque tenga clientes asociados
            # Los clientes pueden seguir usando el plan hasta que venza su membresía
            # Solo se deja de mostrar el plan para nuevas suscripciones
            
            plan_dao.eliminar(plan_id)
            return jsonify({'success': True, 'message': 'Plan eliminado correctamente'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    # ==========================================
    # RUTAS DE PROMOCIONES
    # ==========================================
    
    @app.route('/api/promociones')
    @login_required
    def api_listar_promociones():
        """API para listar todas las promociones"""
        try:
            # Eliminar automáticamente las promociones cuya fecha_fin haya pasado
            eliminadas = promocion_dao.eliminar_promociones_vencidas()
            if eliminadas > 0:
                print(f"[Promociones] Se eliminaron {eliminadas} promoción(es) vencida(s) automáticamente")
            
            promociones = promocion_dao.obtener_todos()
            return jsonify({'success': True, 'data': promociones})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/promociones/plan/<int:plan_id>')
    @login_required
    def api_listar_promociones_plan(plan_id):
        """API para listar promociones de un plan específico"""
        try:
            # Eliminar automáticamente las promociones cuya fecha_fin haya pasado
            eliminadas = promocion_dao.eliminar_promociones_vencidas()
            if eliminadas > 0:
                print(f"[Promociones] Se eliminaron {eliminadas} promoción(es) vencida(s) automáticamente")
            
            promociones = promocion_dao.obtener_por_plan(plan_id)
            return jsonify({'success': True, 'data': promociones})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/promociones', methods=['POST'])
    @login_required
    def api_crear_promocion():
        """API para crear una nueva promoción"""
        try:
            data = request.get_json()
            
            # Validar campos requeridos
            required_fields = ['plan_id', 'nombre', 'fecha_inicio', 'fecha_fin']
            for field in required_fields:
                if not data.get(field):
                    return jsonify({'success': False, 'message': f'El campo {field} es obligatorio'}), 400
            
            # Obtener el tipo de promoción
            tipo_promocion = data.get('tipo_promocion', 'normal')
            
            # VALIDACIÓN SEGÚN EL TIPO DE PROMOCIÓN
            if tipo_promocion == '2x1':
                # Para promoción 2x1, validar que tenga precio_2x1
                if not data.get('precio_2x1'):
                    return jsonify({'success': False, 'message': 'Debe especificar el precio para la promoción 2x1'}), 400
                
                # Asegurar que los campos de descuento normal sean NULL
                data['porcentaje_descuento'] = None
                data['monto_descuento'] = None
            else:
                # Para promoción normal, validar que tenga porcentaje o monto
                if not data.get('porcentaje_descuento') and not data.get('monto_descuento'):
                    return jsonify({'success': False, 'message': 'Debe especificar un porcentaje o monto de descuento'}), 400
                
                # Asegurar que precio_2x1 sea NULL
                data['precio_2x1'] = None
            
            # Validar que no haya promociones superpuestas
            plan_id = data.get('plan_id')
            fecha_inicio = data.get('fecha_inicio')
            fecha_fin = data.get('fecha_fin')
            sexo_aplicable = data.get('sexo_aplicable', 'todos')
            turno_aplicable = data.get('turno_aplicable', 'todos')
            segmento_promocion = data.get('segmento_promocion', 'todos')
            
            if promocion_dao.existe_promocion_superpuesta(plan_id, fecha_inicio, fecha_fin, sexo_aplicable, turno_aplicable=turno_aplicable, segmento_promocion=segmento_promocion):
                return jsonify({
                    'success': False, 
                    'message': 'Ya existe una promoción para este plan en las mismas fechas y sexo aplicable. No se permiten promociones superpuestas.'
                }), 400
            
            data['usuario_id'] = session.get('usuario_id', 1)
            data['activo'] = data.get('activo', 1)
            
            promocion_id = promocion_dao.crear_from_dict(data)
            return jsonify({
                'success': True,
                'message': 'Promoción creada exitosamente',
                'promocion_id': promocion_id
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/promociones/<int:promocion_id>', methods=['GET'])
    @login_required
    def api_obtener_promocion(promocion_id):
        """API para obtener una promoción específica"""
        try:
            promocion = promocion_dao.obtener_por_id(promocion_id)
            if promocion:
                return jsonify({'success': True, 'data': promocion})
            else:
                return jsonify({'success': False, 'message': 'Promoción no encontrada'}), 404
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/promociones/<int:promocion_id>', methods=['PUT'])
    @login_required
    def api_actualizar_promocion(promocion_id):
        """API para actualizar una promoción"""
        try:
            data = request.get_json()
            
            # Verificar que la promoción existe
            promocion = promocion_dao.obtener_por_id(promocion_id)
            if not promocion:
                return jsonify({'success': False, 'message': 'Promoción no encontrada'}), 404
            
            # Obtener el tipo de promoción
            tipo_promocion = data.get('tipo_promocion', promocion.get('tipo_promocion', 'normal'))
            
            # VALIDACIÓN SEGÚN EL TIPO DE PROMOCIÓN
            if tipo_promocion == '2x1':
                # Para promoción 2x1, validar que tenga precio_2x1
                if not data.get('precio_2x1') and not promocion.get('precio_2x1'):
                    return jsonify({'success': False, 'message': 'Debe especificar el precio para la promoción 2x1'}), 400
                
                # Asegurar que los campos de descuento normal sean NULL
                data['porcentaje_descuento'] = None
                data['monto_descuento'] = None
            else:
                # Para promoción normal, validar que tenga porcentaje o monto
                tiene_porcentaje = data.get('porcentaje_descuento') or promocion.get('porcentaje_descuento')
                tiene_monto = data.get('monto_descuento') or promocion.get('monto_descuento')
                
                if not tiene_porcentaje and not tiene_monto:
                    return jsonify({'success': False, 'message': 'Debe especificar un porcentaje o monto de descuento'}), 400
                
                # Asegurar que precio_2x1 sea NULL
                data['precio_2x1'] = None
            
            # Validar que no haya promociones superpuestas (excluyendo la promoción actual)
            fecha_inicio = data.get('fecha_inicio', promocion.get('fecha_inicio'))
            fecha_fin = data.get('fecha_fin', promocion.get('fecha_fin'))
            sexo_aplicable = data.get('sexo_aplicable', promocion.get('sexo_aplicable', 'todos'))
            turno_aplicable = data.get('turno_aplicable', promocion.get('turno_aplicable', 'todos'))
            segmento_promocion = data.get('segmento_promocion', promocion.get('segmento_promocion', 'todos'))
            
            # Verificar si hay cambios que requieran validación
            cambios_requieren_validacion = (
                data.get('fecha_inicio') is not None or 
                data.get('fecha_fin') is not None or 
                data.get('sexo_aplicable') is not None or
                data.get('turno_aplicable') is not None or
                data.get('segmento_promocion') is not None
            )
            
            if cambios_requieren_validacion:
                if promocion_dao.existe_promocion_superpuesta(
                    promocion['plan_id'], fecha_inicio, fecha_fin, sexo_aplicable,
                    promo_id_actual=promocion_id,
                    turno_aplicable=turno_aplicable,
                    segmento_promocion=segmento_promocion
                ):
                    return jsonify({
                        'success': False, 
                        'message': 'Ya existe una promoción para este plan en las mismas fechas, sexo, turno y segmento. No se permiten promociones superpuestas.'
                    }), 400
            
            promocion_dao.actualizar(promocion_id, data)
            return jsonify({'success': True, 'message': 'Promoción actualizada correctamente'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/promociones/<int:promocion_id>', methods=['DELETE'])
    @login_required
    def api_eliminar_promocion(promocion_id):
        """API para eliminar una promoción"""
        try:
            promocion = promocion_dao.obtener_por_id(promocion_id)
            if not promocion:
                return jsonify({'success': False, 'message': 'Promoción no encontrada'}), 404
            
            promocion_dao.eliminar(promocion_id)
            return jsonify({'success': True, 'message': 'Promoción eliminada correctamente'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/planes/<int:plan_id>/precio-con-descuento')
    @login_required
    def api_calcular_precio_descuento(plan_id):
        """API para calcular precio con descuento según promoción vigente"""
        try:
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404
            
            # Obtener sexo y turno del cliente si se proporciona
            sexo_cliente = request.args.get('sexo', None)
            turno_cliente = request.args.get('turno', None)
            segmento_cliente = request.args.get('segmento', None)

            precio_original = float(plan['precio'])
            precio_final, descuento, promocion = promocion_dao.calcular_precio_con_descuento(
                plan_id, precio_original, sexo_cliente, turno_cliente, segmento_cliente
            )
            
            return jsonify({
                'success': True,
                'precio_original': precio_original,
                'precio_final': precio_final,
                'descuento': descuento,
                'promocion': promocion
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400


# ==========================================
# CONTROLADOR: VENTAS
# ==========================================

def init_ventas_controller(app):
    """Inicializa las rutas de ventas"""
    
    @app.route('/ventas')
    @login_required
    @permiso_required('ventas')

    def ventas():
        """Página de ventas"""
        ventas = venta_dao.obtener_todos()
        # Obtener clientes que accedieron hoy para el desplegable de ventas rápidas
        clientes_de_hoy = acceso_dao.obtener_clientes_de_hoy()
        return render_template('ventas.html', ventas=ventas, clientes_de_hoy=clientes_de_hoy)
    
    @app.route('/api/ventas')
    @login_required
    def api_listar_ventas():
        """API para listar ventas"""
        ventas = venta_dao.obtener_todos()
        return jsonify({'success': True, 'data': ventas})
    
    @app.route('/api/ventas/<int:venta_id>', methods=['GET'])
    @login_required
    def api_obtener_venta(venta_id):
        """API para obtener una venta por ID"""
        venta = venta_dao.obtener_por_id(venta_id)
        if venta:
            detalle = venta_dao.obtener_detalle(venta_id)
            venta['detalle'] = detalle
            return jsonify({'success': True, 'data': venta})
        else:
            return jsonify({'success': False, 'message': 'Venta no encontrada'}), 404
    
    @app.route('/api/ventas/<int:venta_id>/detalles', methods=['GET'])
    @login_required
    def obtener_detalles_venta(venta_id):
        """
        Obtiene los detalles (productos) de una venta específica
        Usado para exportar reportes con detalles de productos vendidos
        """
        try:
            # Obtener detalles usando el DAO existente
            detalles = venta_dao.obtener_detalle(venta_id)
            
            if detalles is None:
                return jsonify({
                    'success': False,
                    'message': 'Venta no encontrada'
                }), 404
            
            # Obtener categorías de productos directamente si el DAO no las incluye
            productos_categoria = {}
            try:
                conn = get_connection()
                cursor = conn.cursor()
                producto_ids = [d.get('producto_id') for d in detalles if d.get('producto_id')]
                if producto_ids:
                    placeholders = ', '.join(['%s'] * len(producto_ids))
                    cursor.execute(f"SELECT id, categoria FROM productos WHERE id IN ({placeholders})", producto_ids)
                    for row in cursor.fetchall():
                        productos_categoria[row['id']] = row['categoria'] or ''
                conn.close()
            except Exception:
                pass  # Si falla, seguimos con lo que devuelve el DAO
            
            # Transformar los datos para que sean más útiles en el reporte
            detalles_list = []
            for detalle in detalles:
                producto_id = detalle.get('producto_id')
                categoria = detalle.get('categoria', '') or productos_categoria.get(producto_id, '')
                detalles_list.append({
                    'id': detalle.get('id'),
                    'producto_id': producto_id,
                    'producto_nombre': detalle.get('producto_nombre', ''),
                    'categoria': categoria,
                    'cantidad': detalle.get('cantidad', 0),
                    'precio_unitario': float(detalle.get('precio_unitario', 0)),
                    'subtotal': float(detalle.get('subtotal', 0))
                })
            
            return jsonify({
                'success': True,
                'data': detalles_list
            })
            
        except Exception as e:
            print(f"Error al obtener detalles de venta: {str(e)}")
            return jsonify({
                'success': False,
                'message': f'Error al obtener detalles: {str(e)}'
            }), 500 
    
    @app.route('/api/ventas', methods=['POST'])
    @login_required
    def api_crear_venta():
        """API para crear una nueva venta"""
        try:
            data = request.get_json()
            usuario_id = session.get('usuario_id', 1)
            
            # Determinar el tipo de venta
            tipo_venta = data.get('tipo_venta', None)
            
            # Validar datos requeridos según el tipo de venta
            # Si tipo_venta es 'usuario', se requiere usuario_id, de lo contrario se requiere cliente_id
            if tipo_venta == 'usuario':
                if 'usuario_id' not in data or not data['usuario_id']:
                    return jsonify({'success': False, 'message': 'El campo usuario_id es obligatorio para ventas a usuarios'}), 400
                if 'metodo_pago' not in data or not data['metodo_pago']:
                    return jsonify({'success': False, 'message': 'El campo metodo_pago es obligatorio'}), 400
                if 'detalles' not in data or not data['detalles']:
                    return jsonify({'success': False, 'message': 'El campo detalles es obligatorio'}), 400
            else:
                # Venta normal a cliente
                if 'cliente_id' not in data or not data['cliente_id']:
                    return jsonify({'success': False, 'message': 'El campo cliente_id es obligatorio'}), 400
                if 'metodo_pago' not in data or not data['metodo_pago']:
                    return jsonify({'success': False, 'message': 'El campo metodo_pago es obligatorio'}), 400
                if 'detalles' not in data or not data['detalles']:
                    return jsonify({'success': False, 'message': 'El campo detalles es obligatorio'}), 400
            
            # Validar detalles
            detalles = data.get('detalles', [])
            if not detalles or len(detalles) == 0:
                return jsonify({'success': False, 'message': 'Debe agregar al menos un producto'}), 400
            
            # Calcular total
            total = sum(float(item.get('subtotal', 0)) for item in detalles)
            
            # Verificar stock
            # OPTIMIZADO: 1 query con IN en lugar de 1 query por producto
            ids = [int(item.get('producto_id')) for item in detalles]
            placeholders = ','.join(['%s'] * len(ids))
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f'SELECT id, nombre, stock FROM productos WHERE id IN ({placeholders})', ids)
            productos_map = {r['id']: r for r in cursor.fetchall()}
            conn.close()

            for item in detalles:
                producto_id = int(item.get('producto_id'))
                cantidad = int(item.get('cantidad', 0))
                producto = productos_map.get(producto_id)
                if not producto:
                    return jsonify({'success': False, 'message': f'Producto ID {producto_id} no encontrado'}), 404
                if producto['stock'] < cantidad:
                    return jsonify({'success': False, 'message': f'Stock insuficiente para {producto["nombre"]}'}), 400
            
            # Crear objeto venta según el tipo
            if tipo_venta == 'usuario':
                # Venta a usuario (personal del gimnasio)
                venta_obj = Venta(
                    cliente_id=None,  # No se usa para ventas a usuarios
                    total=total,
                    metodo_pago=data['metodo_pago'],
                    fecha_venta=data.get('fecha_venta'),
                    usuario_id=data.get('usuario_id'),  # El usuario que compra
                    tipo_venta='usuario',
                    usuario_registro_id=usuario_id  # El empleado que registra la venta
                )
            else:
                # Venta normal a cliente
                venta_obj = Venta(
                    cliente_id=data.get('cliente_id'),
                    total=total,
                    metodo_pago=data['metodo_pago'],
                    fecha_venta=data.get('fecha_venta'),
                    usuario_id=None,  # Ventas a cliente no usan usuario_id
                    tipo_venta=None,
                    usuario_registro_id=usuario_id  # El empleado que registra la venta
                )
            
            # Crear venta con detalles
            venta_id = venta_dao.crear_con_detalle(venta_obj, detalles)
            
            return jsonify({
                'success': True,
                'message': 'Venta creada exitosamente',
                'venta_id': venta_id,
                'total': total
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/ventas/<int:venta_id>', methods=['DELETE'])
    @login_required
    def api_eliminar_venta(venta_id):
        """API para eliminar una venta (eliminación lógica)"""
        try:
            # Obtener venta para verificar que existe
            venta = venta_dao.obtener_por_id(venta_id)
            if not venta:
                return jsonify({'success': False, 'message': 'Venta no encontrada'}), 404
            
            # Verificar si ya está eliminada
            if venta.get('estado') == 'eliminado':
                return jsonify({'success': False, 'message': 'La venta ya está eliminada'}), 400
            
            # Obtener detalles de la venta para restaurar stock
            detalles = venta_dao.obtener_detalle(venta_id)
            
            conn = get_connection()
            cursor = conn.cursor()
            
            try:
                # Restaurar stock de los productos
                for detalle in detalles:
                    cursor.execute('''
                        UPDATE productos 
                        SET stock = stock + %s 
                        WHERE id = %s
                    ''', (detalle['cantidad'], detalle['producto_id']))
                
                # Marcar la venta como eliminada (eliminación lógica)
                # Obtener timestamp en hora peruana
                timestamp_peru = obtener_timestamp_peru()
                
                cursor.execute('''
                    UPDATE ventas 
                    SET estado = 'eliminado',
                        fecha_modificacion = %s
                    WHERE id = %s
                ''', (timestamp_peru, venta_id,))
                
                conn.commit()
                
                return jsonify({
                    'success': True, 
                    'message': 'Venta eliminada correctamente'
                })
                
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()
                
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
        
    @app.route('/api/ventas/diagnostico')
    @login_required
    def api_ventas_diagnostico():
        """Endpoint para diagnóstico del sistema de ventas"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Verificar tablas (MySQL - INFORMATION_SCHEMA)
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ventas'")
            tabla_ventas = cursor.fetchone() is not None
            
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'detalle_ventas'")
            tabla_detalle_ventas = cursor.fetchone() is not None
            
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'productos'")
            tabla_productos = cursor.fetchone() is not None
            
            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM ventas")
            total_ventas = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
            
            cursor.execute("SELECT COUNT(*) FROM productos WHERE stock > 0 AND estado = 'activo'")
            productos_disponibles = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
            
            conn.close()
            
            return jsonify({
                'success': True,
                'diagnostico': {
                    'tabla_ventas': tabla_ventas,
                    'tabla_detalle_ventas': tabla_detalle_ventas,
                    'tabla_productos': tabla_productos,
                    'total_ventas': total_ventas,
                    'productos_disponibles': productos_disponibles,
                    'dao_ventas': 'VentaDAO' in globals() or 'venta_dao' in globals(),
                    'conexion_db': True
                }
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500   
    
    @app.route('/api/ventas/productos-disponibles')
    @login_required
    def api_productos_disponibles():
        """API para obtener productos disponibles para venta"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, nombre, precio, stock, categoria
                FROM productos 
                WHERE stock > 0 
                AND estado = 'activo'
                ORDER BY nombre
            ''')
            
            productos = []
            for row in cursor.fetchall():
                productos.append({
                    'id': row['id'],
                    'nombre': row['nombre'],
                    'precio': float(row['precio']),
                    'stock': row['stock'],
                    'categoria': row['categoria'],
                    'precio_formateado': f"S/. {float(row['precio']):.2f}"
                })
            
            conn.close()
            
            return jsonify({'success': True, 'data': productos})
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400

    @app.route('/api/ventas/<int:venta_id>', methods=['PUT'])
    @login_required
    def api_actualizar_venta(venta_id):
        """API para actualizar una venta existente"""
        try:
            data = request.get_json()
            usuario_registro_id = session.get('usuario_id', 1)

            # Solo validar los campos que se pueden editar
            # cliente_id y usuario_id no se tocan al editar
            if not data.get('metodo_pago'):
                return jsonify({'success': False, 'message': 'El campo metodo_pago es obligatorio'}), 400
            if not data.get('total'):
                return jsonify({'success': False, 'message': 'El campo total es obligatorio'}), 400

            # Validar detalles
            detalles = data.get('detalles', [])
            if not detalles or len(detalles) == 0:
                return jsonify({'success': False, 'message': 'Debe agregar al menos un producto'}), 400

            # Obtener venta actual para verificar que existe
            venta_actual = venta_dao.obtener_por_id(venta_id)
            if not venta_actual:
                return jsonify({'success': False, 'message': 'Venta no encontrada'}), 404

            # Restaurar stock de productos anteriores
            detalles_anteriores = venta_dao.obtener_detalle(venta_id)

            conn = get_connection()
            cursor = conn.cursor()

            for detalle in detalles_anteriores:
                cursor.execute('''
                    UPDATE productos 
                    SET stock = stock + %s 
                    WHERE id = %s
                ''', (detalle['cantidad'], detalle['producto_id']))

            # Eliminar detalles anteriores
            cursor.execute('DELETE FROM detalle_ventas WHERE venta_id = %s', (venta_id,))

            # Verificar stock — 1 sola query con IN
            ids = [int(item.get('producto_id')) for item in detalles]
            placeholders = ','.join(['%s'] * len(ids))
            cursor.execute(f'SELECT id, nombre, stock FROM productos WHERE id IN ({placeholders})', ids)
            productos_map = {r['id']: r for r in cursor.fetchall()}

            for item in detalles:
                producto_id = int(item.get('producto_id'))
                cantidad = int(item.get('cantidad', 0))
                producto = productos_map.get(producto_id)
                if not producto:
                    conn.rollback()
                    conn.close()
                    return jsonify({'success': False, 'message': f'Producto ID {producto_id} no encontrado'}), 404
                if producto['stock'] < cantidad:
                    conn.rollback()
                    conn.close()
                    return jsonify({'success': False, 'message': f'Stock insuficiente para {producto["nombre"]}'}), 400

            # Actualizar stock y agregar nuevos detalles
            for item in detalles:
                producto_id = item.get('producto_id')
                cantidad = int(item.get('cantidad', 0))
                precio_unitario = float(item.get('precio_unitario', 0))
                subtotal = float(item.get('subtotal', 0))

                cursor.execute('UPDATE productos SET stock = stock - %s WHERE id = %s',
                               (cantidad, producto_id))

                cursor.execute('''
                    INSERT INTO detalle_ventas (venta_id, producto_id, cantidad, precio_unitario, subtotal)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (venta_id, producto_id, cantidad, precio_unitario, subtotal))

            # Actualizar la venta:
            # - metodo_pago, total: datos editables
            # - usuario_registro_id: quien hace la edicion (sesion actual)
            # - cliente_id y usuario_id NO se tocan: solo identifican a quien es la venta
            timestamp_peru = obtener_timestamp_peru()

            cursor.execute('''
                UPDATE ventas 
                SET metodo_pago = %s,
                    total = %s,
                    fecha_modificacion = %s,
                    usuario_registro_id = %s
                WHERE id = %s
            ''', (
                data['metodo_pago'],
                float(data['total']),
                timestamp_peru,
                usuario_registro_id,
                venta_id
            ))

            conn.commit()
            conn.close()

            return jsonify({
                'success': True,
                'message': 'Venta actualizada exitosamente',
                'venta_id': venta_id
            })

        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
# ==========================================
# FUNCIONES AUXILIARES
# ==========================================

def serializar_row(row):
    """Convierte una fila de la base de datos a un diccionario"""
    if isinstance(row, dict):
        return row
    # Si es una tupla, convertir a diccionario
    try:
        return dict(row)
    except:
        return {}


def calcular_hace(fecha_str):
    """Calcula hace cuánto tiempo ocurrió un evento"""
    
    # Si es None o datetime.min, devolver indicador
    if fecha_str is None:
        return '—'
    if isinstance(fecha_str, datetime):
        if fecha_str == datetime.min:
            return '—'
    
    # Parsear la fecha
    try:
        # MySQL puede devolver objetos datetime directamente
        if isinstance(fecha_str, datetime):
            fecha = fecha_str
        elif isinstance(fecha_str, str):
            if ' ' in fecha_str:
                fecha = datetime.strptime(fecha_str[:19], '%Y-%m-%d %H:%M:%S')
            elif '-' in fecha_str:
                fecha = datetime.strptime(fecha_str[:10], '%Y-%m-%d')
            else:
                return fecha_str
        else:
            return str(fecha_str)
    except:
        return str(fecha_str) if fecha_str else '—'
    
    ahora = datetime.now()
    diferencia = ahora - fecha
    
    segundos = diferencia.total_seconds()
    
    if segundos < 60:
        return 'Ahora mismo'
    elif segundos < 3600:
        minutos = int(segundos / 60)
        return f'{minutos} min' if minutos == 1 else f'{minutos} mins'
    elif segundos < 86400:
        horas = int(segundos / 3600)
        return f'{horas} hora' if horas == 1 else f'{horas} horas'
    elif segundos < 604800:
        dias = int(segundos / 86400)
        return f'{dias} día' if dias == 1 else f'{dias} días'
    else:
        return fecha.strftime('%d/%m/%Y')


def obtener_nombre_dia(fecha_str):
    """Obtiene el nombre del día de la semana"""
    
    try:
        fecha = datetime.strptime(fecha_str, '%Y-%m-%d')
        dias = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
        return dias[fecha.weekday()]
    except:
        return fecha_str[-2:]


# ==========================================
# CONTROLADOR: INVITADOS
# ==========================================

def init_invitados_controller(app):
    """Inicializa las rutas de invitados"""
    
    @app.route('/api/invitados/<int:cliente_id>/limite')
    @login_required
    def api_limite_invitados(cliente_id):
        """API para obtener el límite de invitados de un cliente"""
        try:
            # Obtener cliente con su plan
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if not cliente:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
            
            # Obtener el plan del cliente
            plan_id = cliente.get('plan_id')
            if not plan_id:
                return jsonify({'success': False, 'message': 'Cliente sin plan asignado'}), 400
            
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404
            
            # Obtener cantidad TOTAL de invitados del cliente (no solo de hoy)
            todos_invitados = invitado_dao.obtener_por_cliente(cliente_id)
            total_invitados = len(todos_invitados)
            
            limite = plan.get('cantidad_invitados', 0)
            permite_invitados = plan.get('permite_invitados', 1)
            
            return jsonify({
                'success': True,
                'data': {
                    'permite_invitados': permite_invitados == 1,
                    'limite': limite,  # 0 = ilimitado
                    'total_invitados': total_invitados,
                    'puede_agregar': permite_invitados == 1 and (limite == 0 or total_invitados < limite)
                }
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/invitados', methods=['POST'])
    @login_required
    def api_crear_invitado():
        """API para registrar un invitado"""
        from datetime import datetime, date, date
        
        try:
            data = request.get_json()
            # Agregar el usuario_id de la sesión actual
            data['usuario_id'] = session.get('usuario_id', 1)
            
            # Verificar que no sea None
            if data['usuario_id'] is None:
                data['usuario_id'] = 1
            cliente_id = data.get('cliente_titular_id')
            
            if not cliente_id:
                return jsonify({'success': False, 'message': 'Cliente titular requerido'}), 400
            
            # Verificar cliente y plan
            cliente = cliente_dao.obtener_por_id(cliente_id)
            if not cliente:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
            
            plan_id = cliente.get('plan_id')
            if not plan_id:
                return jsonify({'success': False, 'message': 'Cliente sin plan asignado'}), 400
            
            plan = plan_dao.obtener_por_id(plan_id)
            if not plan:
                return jsonify({'success': False, 'message': 'Plan no encontrado'}), 404
            
            # Verificar si permite invitados
            if not plan.get('permite_invitados', 1):
                return jsonify({'success': False, 'message': 'Este plan no permite invitados'}), 400

            # Verificar si la membresía del cliente está vencida
            hoy = datetime.now()
            fecha_vencimiento = cliente.get('fecha_vencimiento', '')
            membresia_vencida = False

            if fecha_vencimiento:
                try:
                    if ' ' in fecha_vencimiento:
                        fecha_venc = datetime.strptime(fecha_vencimiento, '%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_venc = datetime.strptime(fecha_vencimiento, '%Y-%m-%d')

                    dias_restantes = (fecha_venc.date() - hoy.date()).days
                    if dias_restantes <= 0:
                        membresia_vencida = True
                except:
                    pass

            if membresia_vencida:
                return jsonify({
                    'success': False,
                    'message': 'No se puede registrar invitados porque la membresía está vencida. Primero debe renovar la membresía.'
                }), 400

            # Verificar límite TOTAL de invitados
            limite = plan.get('cantidad_invitados', 0)
            if limite > 0:
                todos_invitados = invitado_dao.obtener_por_cliente(cliente_id)
                total_invitados = len(todos_invitados)
                
                if total_invitados >= limite:
                    return jsonify({
                        'success': False, 
                        'message': f'Has alcanzado el límite de {limite} invitado(s) permitidos'
                    }), 400
            
            # Crear invitado
            invitado_data = {
                'cliente_titular_id': cliente_id,
                'nombre': data.get('nombre', '').strip(),
                'dni': data.get('dni', '').strip(),
                'telefono': data.get('telefono', '').strip(),
                'fecha_visita': date.today().isoformat(),
                'estado': 'activo',
                'usuario_id': data['usuario_id']
            }
            
            if not invitado_data['nombre']:
                return jsonify({'success': False, 'message': 'El nombre del invitado es requerido'}), 400
            
            invitado_id = invitado_dao.crear_from_dict(invitado_data)
            
            return jsonify({
                'success': True,
                'message': 'Invitado registrado correctamente',
                'invitado_id': invitado_id
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/invitados/<int:cliente_id>')
    @login_required
    def api_listar_invitados_cliente(cliente_id):
        """API para listar invitados de un cliente"""
        try:
            invitados = invitado_dao.obtener_por_cliente(cliente_id)
            return jsonify({'success': True, 'data': invitados})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/invitados/<int:invitado_id>', methods=['DELETE'])
    @login_required
    def api_eliminar_invitado(invitado_id):
        """API para eliminar un invitado"""
        try:
            invitado_dao.eliminar(invitado_id)
            return jsonify({'success': True, 'message': 'Invitado eliminado correctamente'})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/invitados/<int:invitado_id>', methods=['PUT'])
    @login_required
    def api_actualizar_invitado(invitado_id):
        """API para actualizar un invitado"""
        try:
            data = request.get_json()
            
            datos_actualizar = {}
            if 'nombre' in data:
                nombre = data['nombre'].strip()
                if not nombre:
                    return jsonify({'success': False, 'message': 'El nombre es requerido'}), 400
                datos_actualizar['nombre'] = nombre
            if 'dni' in data:
                datos_actualizar['dni'] = data['dni'].strip()
            if 'telefono' in data:
                datos_actualizar['telefono'] = data['telefono'].strip()

            
            datos_actualizar['usuario_id'] = session.get('usuario_id', 1)

            if datos_actualizar:
                invitado_dao.actualizar(invitado_id, datos_actualizar)
            
            
            return jsonify({'success': True, 'message': 'Invitado actualizado correctamente'})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400


# ==========================================
# CONTROLADOR: PERFIL
# ==========================================
def init_perfil_controller(app):
    """Inicializa las rutas del perfil de usuario"""
    
    @app.route('/perfil')
    @login_required
    def perfil():
        """Página de perfil del usuario"""
        usuario_id = session.get('usuario_id')
        if usuario_id:
            usuario = usuario_dao.obtener_por_id(usuario_id)
            if usuario:
                return render_template('perfil.html', 
                                      usuario=usuario,
                                      usuario_id=usuario_id,
                                      foto_usuario=usuario.get('foto'))
            else:
                flash('Usuario no encontrado', 'error')
                return redirect(url_for('dashboard'))
        return redirect(url_for('login'))
    
    @app.route('/api/perfil/actualizar', methods=['POST'])
    @login_required
    def api_actualizar_perfil():
        """API para actualizar información del perfil"""
        try:
            data = request.get_json()
            usuario_id = session.get('usuario_id')
            
            if not usuario_id:
                return jsonify({'success': False, 'message': 'Usuario no autenticado'}), 401
            
            campos_permitidos = ['email', 'telefono', 'dni','nombre_completo'] 
            datos_actualizar = {}
            
            for campo in campos_permitidos:
                if campo in data and data[campo] is not None:
                    valor = str(data[campo]).strip() # Convertimos a string por seguridad
                    if valor:
                        datos_actualizar[campo] = valor
            
            if not datos_actualizar:
                return jsonify({'success': False, 'message': 'No hay datos para actualizar'}), 400
            
            # Actualizar en base de datos usando tu DAO
            usuario_dao.actualizar(usuario_id, datos_actualizar)
        
            if 'nombre_completo' in datos_actualizar:
                session['usuario'] = datos_actualizar['nombre_completo']

            return jsonify({
                'success': True, 
                'message': 'Perfil actualizado correctamente'
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/perfil/cambiar-password', methods=['POST'])
    @login_required
    def api_cambiar_password():
        """API para cambiar contraseña del usuario (CON ENCRIPTACIÓN)"""
        try:
            data = request.get_json()
            usuario_id = session.get('usuario_id')
            
            current_password = data.get('currentPassword')
            new_password = data.get('newPassword')
            confirm_password = data.get('confirmPassword')
            
            # Validaciones básicas
            if not current_password or not new_password or not confirm_password:
                return jsonify({'success': False, 'message': 'Todos los campos son requeridos'}), 400
            
            if new_password != confirm_password:
                return jsonify({'success': False, 'message': 'Las contraseñas no coinciden'}), 400
            
            if len(new_password) < 6:
                return jsonify({'success': False, 'message': 'La nueva contraseña debe tener al menos 6 caracteres'}), 400
            
            # Obtener usuario actual (con contraseña)
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id, password FROM usuarios WHERE id = %s', (usuario_id,))
            usuario = cursor.fetchone()
            
            if not usuario:
                conn.close()
                return jsonify({'success': False, 'message': 'Usuario no encontrado'}), 404
            
            stored_password = usuario['password']
            
            # Verificar contraseña actual usando el DAO (que ya maneja bcrypt)
            from dao import usuario_dao
            if not usuario_dao._check_password(current_password, stored_password):
                conn.close()
                return jsonify({'success': False, 'message': 'Contraseña actual incorrecta'}), 200
            
            # Actualizar contraseña (el DAO la encriptará automáticamente)
            usuario_dao.actualizar_password(usuario_id, new_password)
            conn.close()
            
            return jsonify({
                'success': True, 
                'message': 'Contraseña cambiada correctamente'
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/perfil/datos', methods=['GET'])
    @login_required
    def api_obtener_datos_perfil():
        """API para obtener datos del perfil"""
        try:
            usuario_id = session.get('usuario_id')
            if not usuario_id:
                return jsonify({'success': False, 'message': 'Usuario no autenticado'}), 401
            
            usuario = usuario_dao.obtener_por_id(usuario_id)
            if not usuario:
                return jsonify({'success': False, 'message': 'Usuario no encontrado'}), 404
            
            return jsonify({
                'success': True,
                'data': {
                    # 3. CAMBIO: Enviamos 'dni' en lugar de 'nombre_completo'
                    'dni': usuario.get('dni', ''), 
                    'email': usuario.get('email', ''),
                    'telefono': usuario.get('telefono', ''),
                    'username': usuario.get('username', ''),
                    'rol': usuario.get('rol', ''),
                    'fecha_registro': str(usuario.get('fecha_registro', ''))[:19] if usuario.get('fecha_registro') else '',
                    'ultimo_login': str(usuario.get('ultimo_login', ''))[:19] if usuario.get('ultimo_login') else ''
                }
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400


# ==========================================
# CONTROLADOR: FOTOS DE PERFIL
# ==========================================

def init_fotos_controller(app):
    """Inicializa las rutas para gestión de fotos de perfil"""
    
    @app.route('/api/perfil/subir-foto', methods=['POST'])
    @login_required
    def api_subir_foto_perfil():
        """API para subir foto de perfil"""
        try:
            usuario_id = session.get('usuario_id')
            if not usuario_id:
                return jsonify({'success': False, 'message': 'Usuario no autenticado'}), 401
            
            # Verificar si se envió archivo
            if 'foto' not in request.files:
                return jsonify({'success': False, 'message': 'No se seleccionó archivo'}), 400
            
            file = request.files['foto']
            
            # Verificar si se seleccionó un archivo
            if file.filename == '':
                return jsonify({'success': False, 'message': 'No se seleccionó archivo'}), 400
            
            # Validar extensión del archivo
            allowed_extensions = {'png', 'jpg', 'jpeg', 'gif'}
            if '.' not in file.filename or file.filename.split('.')[-1].lower() not in allowed_extensions:
                return jsonify({'success': False, 'message': 'Formato no permitido. Use PNG, JPG o JPEG'}), 400
            
            # Validar tamaño del archivo (máximo 5MB)
            file.seek(0, 2)  # Ir al final del archivo
            file_size = file.tell()
            file.seek(0)  # Volver al inicio
            
            if file_size > 5 * 1024 * 1024:  # 5MB
                return jsonify({'success': False, 'message': 'La imagen es muy grande (máximo 5MB)'}), 400
            
            if file_size < 1024:  # 1KB mínimo
                return jsonify({'success': False, 'message': 'La imagen es muy pequeña'}), 400
            
            # Crear nombre único para el archivo
            import uuid
            import os
            
            # Obtener extensión
            ext = file.filename.split('.')[-1].lower()
            
            # Crear nombre único: usuario_id_timestamp_uuid.ext
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_filename = f"{usuario_id}_{timestamp}_{uuid.uuid4().hex[:8]}.{ext}"
            
            # Crear directorio si no existe
            upload_folder = os.path.join(app.root_path, 'static', 'uploads', 'perfiles')
            if not os.path.exists(upload_folder):
                os.makedirs(upload_folder)
            
            # Ruta completa del archivo
            filepath = os.path.join(upload_folder, unique_filename)
            
            # Guardar archivo
            file.save(filepath)
            
            # Obtener foto anterior para eliminarla después
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT foto FROM usuarios WHERE id = %s', (usuario_id,))
            old_photo = cursor.fetchone()
            
            # Actualizar base de datos
            cursor.execute('UPDATE usuarios SET foto = %s WHERE id = %s', 
                         (unique_filename, usuario_id))
            conn.commit()
            conn.close()
            
            # Eliminar foto anterior si existe
            old_photo_val = (old_photo.get('foto') if isinstance(old_photo, dict) else old_photo[0]) if old_photo else None
            if old_photo_val:
                old_photo_path = os.path.join(upload_folder, old_photo_val)
                if os.path.exists(old_photo_path):
                    try:
                        os.remove(old_photo_path)
                    except:
                        pass  # Ignorar error si no se puede eliminar
            
            # Actualizar sesión — invalidar el caché para que base.html la refleje al instante
            session['foto'] = unique_filename
            session['_foto_usuario_cache'] = unique_filename  # FIX: esta es la key que usa inject_config
            session.modified = True
            
            return jsonify({
                'success': True,
                'message': 'Foto de perfil actualizada correctamente',
                'foto_url': f"/static/uploads/perfiles/{unique_filename}",
                'foto_filename': unique_filename
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
    
    @app.route('/api/perfil/eliminar-foto', methods=['POST'])
    @login_required
    def api_eliminar_foto_perfil():
        """API para eliminar foto de perfil"""
        try:
            usuario_id = session.get('usuario_id')
            if not usuario_id:
                return jsonify({'success': False, 'message': 'Usuario no autenticado'}), 401
            
            conn = get_connection()
            cursor = conn.cursor()
            
            # Obtener foto actual
            cursor.execute('SELECT foto FROM usuarios WHERE id = %s', (usuario_id,))
            result = cursor.fetchone()
            current_photo = (result['foto'] if isinstance(result, dict) else result[0]) if result else None
            
            if current_photo:
                # Eliminar archivo físico
                import os
                upload_folder = os.path.join(app.root_path, 'static', 'uploads', 'perfiles')
                photo_path = os.path.join(upload_folder, current_photo)
                
                if os.path.exists(photo_path):
                    os.remove(photo_path)
                
                # Actualizar base de datos
                cursor.execute('UPDATE usuarios SET foto = NULL WHERE id = %s', (usuario_id,))
                conn.commit()
                
                # Actualizar sesión — invalidar el caché para que base.html lo refleje al instante
                session['foto'] = None
                session['_foto_usuario_cache'] = None  # FIX: esta es la key que usa inject_config
                session.modified = True
            
            conn.close()
            
            return jsonify({
                'success': True,
                'message': 'Foto de perfil eliminada correctamente',
                'foto_eliminada': True  # Indicar que se eliminó
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
    
    # Agregar nueva ruta para obtener foto del usuario actual
    @app.route('/api/obtener-foto-usuario')
    @login_required
    def api_obtener_foto_usuario():
        """API para obtener la foto del usuario actual"""
        try:
            usuario_id = session.get('usuario_id')
            if not usuario_id:
                return jsonify({'foto': None})
            
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT foto FROM usuarios WHERE id = %s', (usuario_id,))
            result = cursor.fetchone()
            conn.close()
            
            foto = (result.get('foto') if isinstance(result, dict) else result[0]) if result else None
            return jsonify({'foto': foto})
        except:
            return jsonify({'foto': None})

# ==========================================
# CONTROLADOR: ACCESO (CONTROL DE ACCESO)
# ==========================================

def init_acceso_controller(app):
    """Inicializa las rutas de control de acceso"""
    
    # Nota: La página de acceso ya está definida en app.py
    # Esta función solo registra las APIs de control de acceso
    
    @app.route('/api/acceso/qr/<qr_code>')
    @login_required
    @permiso_required('acceso')

    def api_buscar_qr(qr_code):
        """API para buscar cliente por código QR - MODIFICADA"""
        try:
            # Obtener fecha del parámetro (del navegador del usuario)
            fecha_param = request.args.get('fecha')
            
            # INTENTAR EXTRAER DNI DEL QR
            dni = None
            
            import re
            dni_match = re.search(r'(\d{8})', qr_code)
            if dni_match:
                dni = dni_match.group(1)
                print(f"✓ DNI extraído del QR: {dni}")
            
            if not dni:
                numeros = re.findall(r'\d+', qr_code)
                for num in numeros:
                    if len(num) >= 8:
                        dni = num[:8]
                        print(f"✓ DNI aproximado extraído: {dni}")
                        break
            
            if not dni:
                return jsonify({
                    'success': False,
                    'message': 'No se pudo extraer DNI del código QR',
                    'encontrado': False
                }), 404
            
            # Buscar cliente por DNI
            cliente = cliente_dao.buscar_por_dni(dni)
            
            if not cliente:
                return jsonify({
                    'success': False,
                    'message': f'Cliente con DNI {dni} no encontrado',
                    'encontrado': False
                }), 404
            
            # Obtener plan
            plan_id = cliente.get('plan_id')
            plan = plan_dao.obtener_por_id(plan_id) if plan_id else None
            
            # Calcular estado de membresía
            ahora = datetime.now()
            
            fecha_vencimiento_raw = cliente.get('fecha_vencimiento', '')
            dias_restantes = 0
            estado_membresia = 'sin_membresia'
            fecha_vencimiento = ''

            if fecha_vencimiento_raw:
                try:
                    fv = fecha_vencimiento_raw
                    if hasattr(fv, 'date'):        # objeto datetime de MySQL
                        fecha_venc = fv
                    elif ' ' in str(fv):
                        fecha_venc = datetime.strptime(str(fv), '%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_venc = datetime.strptime(str(fv)[:10], '%Y-%m-%d')

                    fecha_vencimiento = fecha_venc.strftime('%Y-%m-%d')  # siempre string limpio
                    dias_restantes = (fecha_venc.date() - ahora.date()).days

                    if dias_restantes > 0:
                        estado_membresia = 'pagado'
                    else:
                        estado_membresia = 'vencido'
                except Exception as e:
                    print(f"Error parseando fecha_vencimiento: {e}")
                    fecha_vencimiento = str(fecha_vencimiento_raw)[:10]
            
            # Verificar si ya accedió hoy (usando nuevo método)
            ya_accedio_hoy = cliente_dao.verificar_acceso_hoy(cliente['id'], fecha_param)

            # ========================================
            # VALIDACIÓN DE LÍMITE SEMANAL DE ACCESOS
            # Esta validación también está en api_registrar_acceso
            # pero la agregamos aquí para denegar ANTES de mostrar el modal
            # ========================================
            limite_semanal_alcanzado = False
            mensaje_limite_semanal = ""

            if plan and plan.get('limite_semanal') is not None:
                try:
                    limite_semanal = int(plan.get('limite_semanal'))
                    # Solo validar si el límite es menor a 7 (diferente de "todos los días")
                    if 0 < limite_semanal < 7:
                        conn = get_connection()
                        cursor = conn.cursor()
                        # Obtener el inicio y fin de la semana actual (lunes a domingo)
                        cursor.execute("""
                            SELECT
                                DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY) as semana_inicio,
                                DATE_ADD(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY), INTERVAL 6 DAY) as semana_fin
                        """)
                        semana_info = cursor.fetchone()
                        semana_inicio = semana_info['semana_inicio'] if semana_info else None
                        semana_fin = semana_info['semana_fin'] if semana_info else None

                        if semana_inicio and semana_fin:
                            cursor.execute("""
                                SELECT COUNT(DISTINCT DATE(fecha_hora_entrada)) as dias_unicos
                                FROM accesos
                                WHERE cliente_id = %s
                                AND (tipo = 'cliente' OR tipo IS NULL)
                                AND DATE(fecha_hora_entrada) BETWEEN %s AND %s
                            """, (cliente['id'], semana_inicio, semana_fin))
                            dias_info = cursor.fetchone()
                            dias_unicos = dias_info['dias_unicos'] if dias_info else 0
                            conn.close()

                            # Si ya alcanzó el límite, marcar para denegar
                            if dias_unicos >= limite_semanal:
                                limite_semanal_alcanzado = True
                                mensaje_limite_semanal = f'Límite semanal alcanzado. Este plan permite hasta {limite_semanal} días por semana. Ya accedió {dias_unicos} días esta semana (lunes a domingo).'
                except Exception as e:
                    print(f"Error al validar límite semanal en búsqueda QR: {e}")

            # Verificar acceso denegado por límite semanal
            if limite_semanal_alcanzado:
                return jsonify({
                    'success': True,
                    'encontrado': True,
                    'acceso_denegado': True,
                    'razon_denegado': mensaje_limite_semanal,
                    'data': {
                        'id': cliente['id'],
                        'nombre_completo': cliente['nombre_completo'],
                        'dni': cliente['dni'],
                        'plan': plan['nombre'] if plan else 'Sin plan',
                        'plan_id': plan_id,
                        'fecha_vencimiento': fecha_vencimiento,
                        'dias_restantes': dias_restantes,
                        'ya_accedio_hoy': ya_accedio_hoy
                    }
                })

            precio_base = float(plan['precio']) if plan else 0
            precio_con_descuento = precio_base
            try:
                precio_con_descuento, _, _ = promocion_dao.calcular_precio_con_descuento(
                    plan_id, precio_base, sexo_cliente=cliente.get('sexo'), turno_cliente=cliente.get('turno'), segmento_cliente=cliente.get('segmento_promocion')
                )
            except Exception:
                pass

            return jsonify({
                'success': True,
                'encontrado': True,
                'data': {
                    'id': cliente['id'],
                    'nombre_completo': cliente['nombre_completo'],
                    'dni': cliente['dni'],
                    'telefono': cliente.get('telefono', ''),
                    'plan': plan['nombre'] if plan else 'Sin plan',
                    'plan_id': plan_id,
                    'precio_plan': precio_base,
                    'precio_con_descuento': precio_con_descuento,
                    'tiene_promocion': precio_con_descuento != precio_base,
                    'qr_habilitado': plan.get('qr_habilitado', 1) if plan else 1,
                    'fecha_vencimiento': fecha_vencimiento,
                    'dias_restantes': dias_restantes,
                    'estado_membresia': estado_membresia,
                    'qr_code': qr_code,
                    'ya_accedio_hoy': ya_accedio_hoy,
                    'dni_extraido': dni
                }
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    
    @app.route('/api/acceso/dni/<dni>')
    @login_required
    def api_buscar_dni(dni):
        """API para buscar cliente por DNI"""
        try:
            # Obtener fecha del parámetro
            fecha_param = request.args.get('fecha')
            
            # Buscar cliente por DNI
            cliente = cliente_dao.buscar_por_dni(dni)
            
            if not cliente:
                # Buscar si el DNI pertenece a un invitado activo
                invitado = invitado_dao.buscar_por_dni(dni)
                if invitado and invitado.get('estado') != 'eliminado':
                    # Es un invitado activo: devolver el cliente titular
                    titular_id = invitado.get('cliente_titular_id')
                    titular = cliente_dao.obtener_por_id(titular_id) if titular_id else None
                    if titular:
                        return jsonify({
                            'success': True,
                            'encontrado': True,
                            'es_invitado': True,
                            'invitado': {
                                'id': invitado.get('id'),
                                'nombre': invitado.get('nombre'),
                                'dni': invitado.get('dni')
                            },
                            'data': {
                                'id': titular['id'],
                                'nombre_completo': titular['nombre_completo'],
                                'dni': titular['dni'],
                                'telefono': titular.get('telefono', ''),
                                'plan': titular.get('plan_nombre', 'Sin plan'),
                                'plan_id': titular.get('plan_id'),
                                'qr_code': titular.get('qr_code', ''),
                                'fecha_vencimiento': str(titular.get('fecha_vencimiento', '') or ''),
                                'dni_buscado': dni
                            }
                        })

                return jsonify({
                    'success': True,
                    'encontrado': False,
                    'message': 'Cliente no encontrado',
                    'dni': dni
                })
            
            # Obtener plan
            plan_id = cliente.get('plan_id')
            plan = plan_dao.obtener_por_id(plan_id) if plan_id else None
            
            # Calcular estado de membresía
            ahora = datetime.now()
            
            fecha_vencimiento_raw = cliente.get('fecha_vencimiento', '')
            dias_restantes = 0
            estado_membresia = 'sin_membresia'
            fecha_vencimiento = ''

            if fecha_vencimiento_raw:
                try:
                    fv = fecha_vencimiento_raw
                    if hasattr(fv, 'date'):        # objeto datetime de MySQL
                        fecha_venc = fv
                    elif ' ' in str(fv):
                        fecha_venc = datetime.strptime(str(fv), '%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_venc = datetime.strptime(str(fv)[:10], '%Y-%m-%d')

                    fecha_vencimiento = fecha_venc.strftime('%Y-%m-%d')  # siempre string limpio
                    dias_restantes = (fecha_venc.date() - ahora.date()).days

                    if dias_restantes > 0:
                        estado_membresia = 'pagado'
                    else:
                        estado_membresia = 'vencido'
                except Exception as e:
                    print(f"Error parseando fecha_vencimiento: {e}")
                    fecha_vencimiento = str(fecha_vencimiento_raw)[:10]
            
            # Verificar si ya accedió hoy
            ya_accedio_hoy = cliente_dao.verificar_acceso_hoy(cliente['id'], fecha_param)
            
            # ========================================
            # VALIDACIÓN DE LÍMITE SEMANAL DE ACCESOS
            # Esta validación también está en api_registrar_acceso
            # pero la agregamos aquí para denegar ANTES de mostrar el modal
            # ========================================
            limite_semanal_alcanzado = False
            mensaje_limite_semanal = ""
            
            if plan and plan.get('limite_semanal') is not None:
                try:
                    limite_semanal = int(plan.get('limite_semanal'))
                    # Solo validar si el límite es menor a 7 (diferente de "todos los días")
                    if 0 < limite_semanal < 7:
                        conn = get_connection()
                        cursor = conn.cursor()
                        # Obtener el inicio y fin de la semana actual (lunes a domingo)
                        cursor.execute("""
                            SELECT 
                                DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY) as semana_inicio,
                                DATE_ADD(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY), INTERVAL 6 DAY) as semana_fin
                        """)
                        semana_info = cursor.fetchone()
                        semana_inicio = semana_info['semana_inicio'] if semana_info else None
                        semana_fin = semana_info['semana_fin'] if semana_info else None

                        if semana_inicio and semana_fin:
                            cursor.execute("""
                                SELECT COUNT(DISTINCT DATE(fecha_hora_entrada)) as dias_unicos
                                FROM accesos
                                WHERE cliente_id = %s
                                AND (tipo = 'cliente' OR tipo IS NULL)
                                AND DATE(fecha_hora_entrada) BETWEEN %s AND %s
                            """, (cliente['id'], semana_inicio, semana_fin))
                            dias_info = cursor.fetchone()
                            dias_unicos = dias_info['dias_unicos'] if dias_info else 0
                            conn.close()
                            
                            # Si ya alcanzó el límite, marcar para denegar
                            if dias_unicos >= limite_semanal:
                                limite_semanal_alcanzado = True
                                mensaje_limite_semanal = f'Límite semanal alcanzado. Este plan permite hasta {limite_semanal} días por semana. Ya accedió {dias_unicos} días esta semana (lunes a domingo).'
                except Exception as e:
                    print(f"Error al validar límite semanal en búsqueda: {e}")
            
            # Verificar acceso denegado por límite semanal
            if limite_semanal_alcanzado:
                return jsonify({
                    'success': True,
                    'encontrado': True,
                    'acceso_denegado': True,
                    'razon_denegado': mensaje_limite_semanal,
                    'data': {
                        'id': cliente['id'],
                        'nombre_completo': cliente['nombre_completo'],
                        'dni': cliente['dni'],
                        'plan': plan['nombre'] if plan else 'Sin plan',
                        'plan_id': plan_id,
                        'fecha_vencimiento': fecha_vencimiento,
                        'dias_restantes': dias_restantes,
                        'ya_accedio_hoy': ya_accedio_hoy
                    }
                })
            
            precio_base = float(plan['precio']) if plan else 0
            precio_con_descuento = precio_base
            try:
                precio_con_descuento, _, _ = promocion_dao.calcular_precio_con_descuento(
                    plan_id, precio_base, sexo_cliente=cliente.get('sexo'), turno_cliente=cliente.get('turno'), segmento_cliente=cliente.get('segmento_promocion')
                )
            except Exception:
                pass

            return jsonify({
                'success': True,
                'encontrado': True,
                'data': {
                    'id': cliente['id'],
                    'nombre_completo': cliente['nombre_completo'],
                    'dni': cliente['dni'],
                    'telefono': cliente.get('telefono', ''),
                    'plan': plan['nombre'] if plan else 'Sin plan',
                    'plan_id': plan_id,
                    'precio_plan': precio_base,
                    'precio_con_descuento': precio_con_descuento,
                    'tiene_promocion': precio_con_descuento != precio_base,
                    'qr_habilitado': plan.get('qr_habilitado', 1) if plan else 1,
                    'fecha_vencimiento': fecha_vencimiento,
                    'dias_restantes': dias_restantes,
                    'estado_membresia': estado_membresia,
                    'qr_code': cliente.get('qr_code', ''),
                    'ya_accedio_hoy': ya_accedio_hoy
                }
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/acceso/registrar', methods=['POST'])
    @login_required
    def api_registrar_acceso():
        """API para registrar un acceso de cliente y opcionalmente sus invitados"""
        try:
            data = request.get_json()
            cliente_id = data.get('cliente_id')
            tipo = data.get('tipo', 'cliente')
            dni = data.get('dni')
            metodo = data.get('metodo', 'manual')
            invitados_ids = data.get('invitados_ids', [])
            usuario_id = session.get('usuario_id', 1)

            # Obtener fecha del parámetro (DEBE IR PRIMERO)
            fecha_param = request.args.get('fecha')

            # Validación de membresía vencida
            if tipo == 'cliente' and cliente_id:
                membresia_info = cliente_dao.verificar_membresia_vencida(cliente_id)
                
                if membresia_info['vencida']:
                    return jsonify({
                        'success': False,
                        'message': f'No se puede registrar acceso porque la membresía está vencida. Primero debe renovar la membresía del plan "{membresia_info["plan_nombre"]}".'
                    }), 200
            
            # Validación de límite semanal de accesos
            if tipo == 'cliente' and cliente_id:
                # Obtener información del plan del cliente para verificar límite semanal
                cliente_info = cliente_dao.obtener_por_id(cliente_id)
                if cliente_info and cliente_info.get('plan_id'):
                    plan = plan_dao.obtener_por_id(cliente_info['plan_id'])
                    # NULL en BD = sin límite (7). Solo bloquear si está entre 1 y 6.
                    limite_semanal_raw = plan.get('limite_semanal')
                    if limite_semanal_raw is None:
                        limite_semanal = 7
                    else:
                        try:
                            limite_semanal = int(limite_semanal_raw)
                        except (ValueError, TypeError):
                            limite_semanal = 7

                    if plan and 0 < limite_semanal < 7:
                        conn = get_connection()
                        cursor = conn.cursor()
                        cursor.execute(f"""
                            SELECT
                                DATE_SUB({get_current_date_expression()}, INTERVAL WEEKDAY({get_current_date_expression()}) DAY) as semana_inicio,
                                DATE_ADD(DATE_SUB({get_current_date_expression()}, INTERVAL WEEKDAY({get_current_date_expression()}) DAY), INTERVAL 6 DAY) as semana_fin
                        """)
                        semana_info = cursor.fetchone()
                        semana_inicio = semana_info['semana_inicio'] if semana_info else None
                        semana_fin = semana_info['semana_fin'] if semana_info else None

                        if semana_inicio and semana_fin:
                            cursor.execute("""
                                SELECT COUNT(DISTINCT DATE(fecha_hora_entrada)) as dias_unicos
                                FROM accesos
                                WHERE cliente_id = %s
                                AND (tipo = 'cliente' OR tipo IS NULL)
                                AND DATE(fecha_hora_entrada) BETWEEN %s AND %s
                            """, (cliente_id, semana_inicio, semana_fin))
                            dias_info = cursor.fetchone()
                            dias_unicos = dias_info['dias_unicos'] if dias_info else 0
                            conn.close()
                            if dias_unicos >= limite_semanal:
                                mensaje = f'Límite semanal alcanzado. Este plan permite hasta {limite_semanal} días por semana. Ya accedió {dias_unicos} días esta semana (lunes a domingo).'
                                return jsonify({'success': False, 'message': mensaje}), 200
                        else:
                            conn.close()

            # Validar datos requeridos
            if tipo == 'cliente':
                if cliente_id is None:
                    return jsonify({'success': False, 'message': 'ID de cliente requerido'}), 400
                try:
                    cliente_id = int(cliente_id)
                except (ValueError, TypeError):
                    return jsonify({'success': False, 'message': 'ID de cliente inválido'}), 400
                
                # Verificar acceso existente usando el método del DAO (control diario)
                acceso_existente = cliente_dao.verificar_acceso_hoy(cliente_id, fecha_param)
                
                # Si ya accedió sin invitados, bloquear. Con invitados, continuar para registrarlos.
                if acceso_existente and not invitados_ids:
                    return jsonify({
                        'success': False, 
                        'message': 'Este cliente ya accedió hoy'
                    }), 200
            
            # Si hay invitados_ids, verificar que no hayan accedido hoy
            invitados_ya_accedieron = []
            if invitados_ids:
                conn = get_connection()
                cursor = conn.cursor()
                
                invitados_a_registrar = []
                
                for inv_id in invitados_ids:
                    if fecha_param:
                        cursor.execute("""
                            SELECT id FROM accesos 
                            WHERE cliente_id = %s AND tipo = 'invitado'
                            AND DATE(fecha_hora_entrada) = DATE(%s)
                            LIMIT 1
                        """, (inv_id, fecha_param))
                    else:
                        cursor.execute(f"""
                            SELECT id FROM accesos 
                            WHERE cliente_id = %s AND tipo = 'invitado'
                            AND DATE(fecha_hora_entrada) = {get_current_date_expression()}
                            LIMIT 1
                        """, (inv_id,))
                    
                    if cursor.fetchone():
                        invitados_ya_accedieron.append(inv_id)
                    else:
                        invitados_a_registrar.append(inv_id)
                
                conn.close()
                
                # Si todos los invitados ya accedieron, mostrar error
                if invitados_ya_accedieron and not invitados_a_registrar:
                    return jsonify({
                        'success': False, 
                        'message': 'Todos los invitados seleccionados ya registraron acceso hoy'
                    }), 200
                
                # Si algunos ya accedieron, continuar con los demás
                if invitados_ya_accedieron:
                    invitados_ids = invitados_a_registrar
            
            # Registrar el acceso del cliente titular (SOLO si no ha accedido hoy)
            acceso_id = None
            if tipo == 'cliente' and not acceso_existente:
                # Obtener cliente para el DNI
                cliente = cliente_dao.obtener_por_id(cliente_id)
                acceso_id = acceso_dao.registrar_entrada(
                    cliente_id=cliente_id,
                    dni=cliente['dni'] if cliente else None,
                    tipo=tipo,
                    metodo=metodo,
                    usuario_id=usuario_id
                )
            
            # Registrar accesos de los invitados
            accesos_invitados = []
            if invitados_ids:
                conn = get_connection()
                cursor = conn.cursor()
                placeholders = ','.join(['%s'] * len(invitados_ids))
                cursor.execute(f'SELECT id, nombre, dni FROM invitados WHERE id IN ({placeholders})', invitados_ids)
                invitados_info = {row['id']: row for row in cursor.fetchall()}
                conn.close()
                
                for inv_id in invitados_ids:
                    inv = invitados_info.get(inv_id)
                    if inv:
                        inv_acceso_id = acceso_dao.registrar_entrada(
                            cliente_id=inv_id,
                            dni=inv['dni'],
                            tipo='invitado',
                            metodo=metodo,
                            usuario_id=usuario_id
                        )
                        accesos_invitados.append(inv_acceso_id)
            
            # Preparar mensaje de respuesta
            mensaje = 'Acceso registrado correctamente'
            if acceso_existente and invitados_ids:
                mensaje = 'Invitados registrados correctamente (el cliente titular ya había accedido hoy)'
            elif invitados_ya_accedieron and invitados_a_registrar:
                mensaje = f'Algunos invitados registrados (otros ya habían accedido)'
            
            return jsonify({
                'success': True,
                'message': mensaje,
                'acceso_id': acceso_id,
                'accesos_invitados': accesos_invitados,
                'cliente_ya_accedio': acceso_existente,
                'invitados_ya_accedieron': invitados_ya_accedieron
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/acceso/hoy')
    @login_required
    def api_accesos_hoy():
        """API para obtener los accesos de hoy"""
        try:
            # Obtener fecha del parámetro o usar la del servidor
            fecha_param = request.args.get('fecha')
            accesos = acceso_dao.obtener_hoy(fecha_param)
            
            # Contador también debe usar la fecha correcta
            if fecha_param:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM accesos WHERE DATE(fecha_hora_entrada) = DATE(%s)', (fecha_param,))
                row = cursor.fetchone()
                contador = list(row.values())[0] if isinstance(row, dict) else row[0]
                conn.close()
            else:
                contador = acceso_dao.contar_entradas_hoy()
            
            return jsonify({
                'success': True,
                'data': accesos,
                'contador': contador
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/acceso/invitados/<int:cliente_id>')
    @login_required
    def api_invitados_cliente(cliente_id):
        """API para obtener los invitados de un cliente titular"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Obtener configuración de invitados del plan del cliente titular
            cursor.execute('''
                SELECT p.permite_invitados, p.cantidad_invitados
                FROM clientes c
                JOIN planes_membresia p ON c.plan_id = p.id
                WHERE c.id = %s
            ''', (cliente_id,))
            plan = cursor.fetchone()
            
            if not plan or not plan['permite_invitados']:
                conn.close()
                return jsonify({
                    'success': True,
                    'data': {
                        'permite_invitados': False,
                        'invitados_permitidos': 0,
                        'invitados_creados': 0,
                        'invitados_registrados': []
                    }
                })
            
            # Obtener fecha del parámetro
            fecha_param = request.args.get('fecha')
            if not fecha_param:
                fecha_param = datetime.now().strftime('%Y-%m-%d')
            
            # Contar cuántos invitados tiene creados el cliente
            cursor.execute('''
                SELECT COUNT(*) FROM invitados 
                WHERE cliente_titular_id = %s AND estado = 'activo'
            ''', (cliente_id,))
            row_count = cursor.fetchone()
            invitados_creados = list(row_count.values())[0] if isinstance(row_count, dict) else row_count[0]
            
            # Obtener los invitados que ya registraron acceso hoy
            cursor.execute('''
                SELECT i.id, i.nombre, i.dni, a.id as acceso_id
                FROM invitados i
                LEFT JOIN accesos a ON i.id = a.cliente_id AND a.tipo = 'invitado' 
                    AND DATE(a.fecha_hora_entrada) = DATE(%s)
                WHERE i.cliente_titular_id = %s AND i.estado = 'activo'
            ''', (fecha_param, cliente_id))
            
            invitados = []
            for row in cursor.fetchall():
                invitados.append({
                    'id': row['id'],
                    'nombre': row['nombre'],
                    'dni': row['dni'],
                    'ya_accedio': row['acceso_id'] is not None
                })
            
            conn.close()
            
            return jsonify({
                'success': True,
                'data': {
                    'permite_invitados': True,
                    'invitados_permitidos': plan['cantidad_invitados'],
                    'invitados_creados': invitados_creados,
                    'invitados_registrados': invitados
                }
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/clientes/invitado', methods=['POST'])
    @login_required
    def api_crear_invitado_acceso():
        """API para crear un nuevo invitado desde el módulo de acceso con validación de límite"""
        try:
            data = request.get_json()
            data['usuario_id'] = session.get('usuario_id', 1)
            cliente_titular_id = data.get('cliente_titular_id')
            # El frontend envía nombre_completo pero la BD espera nombre
            nombre = data.get('nombre_completo') or data.get('nombre')
            dni = data.get('dni')
            telefono = data.get('telefono', '')
            
            # Validar datos requeridos
            if not cliente_titular_id or not nombre or not dni:
                return jsonify({'success': False, 'message': 'Datos incompletos'}), 400
            
            conn = get_connection()
            cursor = conn.cursor()
            
            # Verificar el límite de invitados del plan
            cursor.execute('''
                SELECT p.cantidad_invitados, p.permite_invitados, c.nombre_completo
                FROM clientes c
                JOIN planes_membresia p ON c.plan_id = p.id
                WHERE c.id = %s
            ''', (cliente_titular_id,))
            plan = cursor.fetchone()
            
            if not plan:
                conn.close()
                return jsonify({'success': False, 'message': 'Cliente titular no encontrado'}), 400
            
            if not plan['permite_invitados']:
                conn.close()
                return jsonify({'success': False, 'message': 'Su plan no permite invitados'}), 400

            # Verificar si la membresía del cliente está vencida
            cursor.execute('SELECT fecha_vencimiento FROM clientes WHERE id = %s', (cliente_titular_id,))
            cliente_data = cursor.fetchone()
            membresia_vencida = False

            if cliente_data and cliente_data['fecha_vencimiento']:
                hoy = datetime.now()
                try:
                    fecha_venc_str = cliente_data['fecha_vencimiento']
                    if ' ' in fecha_venc_str:
                        fecha_venc = datetime.strptime(fecha_venc_str, '%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_venc = datetime.strptime(fecha_venc_str, '%Y-%m-%d')

                    dias_restantes = (fecha_venc.date() - hoy.date()).days
                    if dias_restantes <= 0:
                        membresia_vencida = True
                except:
                    pass

            if membresia_vencida:
                conn.close()
                return jsonify({
                    'success': False,
                    'message': 'No se puede registrar invitados porque la membresía está vencida. Primero debe renovar la membresía.'
                }), 400

            # Contar invitados actuales
            cursor.execute('''
                SELECT COUNT(*) FROM invitados 
                WHERE cliente_titular_id = %s AND estado = 'activo'
            ''', (cliente_titular_id,))
            invitados_actuales = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
            
            if invitados_actuales >= plan['cantidad_invitados']:
                conn.close()
                return jsonify({
                    'success': False, 
                    'message': f'Ha alcanzado el límite de {plan["cantidad_invitados"]} invitados de su plan "{plan["nombre_completo"]}"'
                }), 400
            
            # Crear el invitado
            fecha_visita = get_current_timestamp_peru_value()[:10]
            
            cursor.execute('''
                INSERT INTO invitados (cliente_titular_id, nombre, dni, telefono, fecha_visita, estado,usuario_id)
                VALUES (%s, %s, %s, %s, %s, 'activo',%s)
            ''', (cliente_titular_id, nombre, dni, telefono, fecha_visita, data['usuario_id']))
            
            invitado_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            return jsonify({
                'success': True,
                'message': 'Invitado creado correctamente',
                'invitado_id': invitado_id
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/acceso/ya-escaneado', methods=['POST'])
    @login_required
    def api_ya_escaneado():
        """API para verificar y marcar un código como escaneado hoy"""
        try:
            data = request.get_json()
            identificador = data.get('identificador')  # QR code o 'dni_XXX'
            
            if not identificador:
                return jsonify({'success': False, 'message': 'Identificador requerido'}), 400
            
            hoy = get_current_timestamp_peru_value()[:10]
            
            conn = get_connection()
            cursor = conn.cursor()
            
            # Verificar si ya fue escaneado hoy
            cursor.execute('''
                SELECT id FROM codigos_escaneados 
                WHERE identificador = %s AND fecha = %s
            ''', (identificador, hoy))
            
            ya_existe = cursor.fetchone() is not None
            
            if not ya_existe:
                # Marcar como escaneado
                cursor.execute('''
                    INSERT INTO codigos_escaneados (identificador, fecha)
                    VALUES (%s, %s)
                ''', (identificador, hoy))
                conn.commit()
            
            conn.close()
            
            return jsonify({
                'success': True,
                'ya_escaneado': ya_existe
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/acceso/pagar', methods=['POST'])
    @login_required
    def api_acceso_pagar():
        """API para cobrar y registrar acceso de un cliente sin QR"""
        try:
            data = request.get_json()
            cliente_id = data.get('cliente_id')
            metodo_pago = data.get('metodo_pago', 'efectivo')
            marcar_pagado = data.get('marcar_pagado', False)
            
            if not cliente_id:
                return jsonify({'success': False, 'message': 'ID de cliente requerido'}), 400
            
            conn = get_connection()
            cursor = conn.cursor()
            
            try:
                # Obtener datos del cliente y su plan
                cursor.execute('''
                    SELECT c.*, p.nombre as plan_nombre, p.precio, p.duracion, p.qr_habilitado
                    FROM clientes c
                    JOIN planes_membresia p ON c.plan_id = p.id
                    WHERE c.id = %s
                ''', (cliente_id,))
                cliente = cursor.fetchone()
                
                if not cliente:
                    return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
                
                # Si marcar_pagado es True, registrar pago y actualizar fechas
                if marcar_pagado:
                    # Calcular fecha_vencimiento basada en la duración del plan
                    fecha_actual = datetime.now()
                    duracion = cliente['duracion']
                    
                    # Parsear duración
                    duracion_lower = duracion.lower()
                    if 'dia' in duracion_lower:
                        match = re.search(r'(\d+)', duracion)
                        dias = int(match.group(1)) if match else 30
                        fecha_vencimiento = fecha_actual + timedelta(days=dias)
                    elif 'mes' in duracion_lower:
                        match = re.search(r'(\d+)', duracion)
                        meses = int(match.group(1)) if match else 1
                        # Sumar meses sin exceder el último día del mes
                        año = fecha_actual.year + (fecha_actual.month + meses - 1) // 12
                        mes_final = (fecha_actual.month + meses - 1) % 12 + 1
                        dia_final = min(fecha_actual.day, [31, 29 if año % 4 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mes_final - 1])
                        fecha_vencimiento = datetime(año, mes_final, dia_final, fecha_actual.hour, fecha_actual.minute, fecha_actual.second)
                    elif 'hora' in duracion_lower:
                        match = re.search(r'(\d+)', duracion)
                        horas = int(match.group(1)) if match else 24
                        fecha_vencimiento = fecha_actual + timedelta(hours=horas)
                    else:
                        fecha_vencimiento = fecha_actual + timedelta(days=30)
                    
                    fecha_vencimiento_str = fecha_vencimiento.strftime('%Y-%m-%d %H:%M:%S')
                    fecha_actual_str = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Registrar pago en la tabla pagos
                    cursor.execute('''
                        INSERT INTO pagos (cliente_id, plan_id, monto, metodo_pago, fecha_pago, estado, usuario_registro)
                        VALUES (%s, %s, %s, %s, %s, 'completado', %s)
                    ''', (
                        cliente_id,
                        cliente['plan_id'],
                        cliente['precio'] or 0,
                        metodo_pago,
                        fecha_actual_str,
                        session.get('usuario_id')
                    ))
                    pago_id = cursor.lastrowid
                    
                    # Actualizar cliente
                    cursor.execute('''
                        UPDATE clientes 
                        SET fecha_inicio = %s, fecha_vencimiento = %s
                        WHERE id = %s
                    ''', (fecha_actual_str, fecha_vencimiento_str, cliente_id))
                    
                    conn.commit()
                else:
                    # Si no se marca como pagado, solo obtener las fechas actuales del cliente
                    fecha_vencimiento_str = cliente['fecha_vencimiento'] or ''
                    pago_id = None
                
                # Registrar el acceso (siempre se registra el acceso)
                acceso_id = acceso_dao.registrar_entrada(
                    cliente_id=cliente_id,
                    dni=cliente['dni'],
                    tipo='cliente',
                    metodo='manual'
                )
                
                return jsonify({
                    'success': True,
                    'message': 'Acceso registrado correctamente',
                    'pago_id': pago_id,
                    'acceso_id': acceso_id,
                    'fecha_vencimiento': fecha_vencimiento_str,
                    'marcar_pagado': marcar_pagado
                })
            finally:
                conn.close()
                
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/acceso/pendiente', methods=['POST'])
    @login_required
    def api_acceso_pendiente():
        """API para registrar acceso sin cobrar (pendiente)"""
        try:
            data = request.get_json()
            cliente_id = data.get('cliente_id')
            
            if not cliente_id:
                return jsonify({'success': False, 'message': 'ID de cliente requerido'}), 400
            
            # Obtener datos del cliente
            cliente = cliente_dao.obtener_por_id(cliente_id)
            
            if not cliente:
                return jsonify({'success': False, 'message': 'Cliente no encontrado'}), 404
            
            # Registrar el acceso (sin cobrar)
            acceso_id = acceso_dao.registrar_entrada(
                cliente_id=cliente_id,
                dni=cliente['dni'],
                tipo='cliente',
                metodo='manual'
            )
            
            return jsonify({
                'success': True,
                'message': 'Acceso registrado (pendiente de pago)',
                'acceso_id': acceso_id
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400


    @app.route('/configuraciones')
    @login_required
    @permiso_required('configuracion')
    def configuraciones():
        """Panel de configuraciones"""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM configuraciones ORDER BY id DESC LIMIT 1')
        config = cursor.fetchone()
        conn.close()
        
        if not config:
            return redirect(url_for('configuracion_inicial'))
        
        return render_template('configuraciones.html', config=config)


# ==========================================
# CONTROLADOR: NOTIFICACIONES
# ==========================================

# ============================================================
# CACHÉ DE NOTIFICACIONES EN MEMORIA
# Evita consultar MySQL en cada poll de 3 segundos.
# Flask responde desde RAM; la BD solo se toca cada 4 segundos
# por usuario como máximo. Funcionalidad 100% idéntica.
# ============================================================
import time as _time

_notif_cache = {}          # { usuario_id: {'data': list, 'ts': float} }
_notif_todas_cache = {}    # caché separado para /todas
_NOTIF_TTL = 4             # segundos entre consultas reales a la BD


def _get_notif_no_leidas(usuario_id):
    """Retorna notificaciones no leídas desde caché o BD."""
    ahora = _time.time()
    entrada = _notif_cache.get(usuario_id)
    if entrada and (ahora - entrada['ts']) < _NOTIF_TTL:
        return entrada['data']
    resultado = notificacion_dao.obtener_no_leidas(usuario_id)
    _notif_cache[usuario_id] = {'data': resultado, 'ts': ahora}
    return resultado


def _get_notif_todas(usuario_id):
    """Retorna todas las notificaciones desde caché o BD."""
    ahora = _time.time()
    entrada = _notif_todas_cache.get(usuario_id)
    if entrada and (ahora - entrada['ts']) < _NOTIF_TTL:
        return entrada['data']
    resultado = notificacion_dao.obtener_todas(usuario_id, limit=100)
    _notif_todas_cache[usuario_id] = {'data': resultado, 'ts': ahora}
    return resultado


def _invalidar_cache_notif(usuario_id=None):
    """
    Limpia el caché de notificaciones.
    - Si usuario_id es None: limpia TODO el caché (usar al crear notificaciones nuevas).
    - Si usuario_id tiene valor: limpia solo ese usuario (usar al marcar como leída).
    """
    if usuario_id is None:
        _notif_cache.clear()
        _notif_todas_cache.clear()
    else:
        _notif_cache.pop(usuario_id, None)
        _notif_todas_cache.pop(usuario_id, None)


def init_notificaciones_controller(app):
    """Inicializa las rutas de notificaciones"""
    
    @app.route('/api/notificaciones')
    @login_required
    def api_obtener_notificaciones():
        """API para obtener notificaciones del usuario actual"""
        try:
            usuario_id = session.get('usuario_id')
            notificaciones = _get_notif_no_leidas(usuario_id)  # OPTIMIZADO: desde caché
            
            # Formatear para el frontend
            notificaciones_formateadas = []
            for notif in notificaciones:
                # Determinar icono según tipo
                iconos = {
                    'payment': 'dollar-sign',
                    'membership': 'credit-card',
                    'client': 'user-plus',
                    'vencimiento': 'calendar-xmark',
                    'vencimiento_proximo': 'clock',
                    'moroso': 'user-slash',
                    'stock': 'triangle-exclamation',
                    'sistema': 'bell'
                }
                
                icono = iconos.get(notif['tipo'], 'bell')
                
                # Calcular tiempo transcurrido
                fecha_creacion = notif['fecha_creacion']
                tiempo = calcular_tiempo_relativo(fecha_creacion)
                
                notificaciones_formateadas.append({
                    'id': notif['id'],
                    'type': notif['tipo'],
                    'title': notif['titulo'],
                    'message': notif['mensaje'],
                    'time': tiempo,
                    'unread': notif['leida'] == 0,
                    'cliente_nombre': notif.get('cliente_nombre'),
                    'icon': icono
                })
            
            return jsonify({'success': True, 'data': notificaciones_formateadas})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
        
    @app.route('/api/notificaciones/contador')
    @login_required
    def api_contar_notificaciones():
        """API para contar notificaciones no leídas"""
        try:
            usuario_id = session.get('usuario_id')
            count = notificacion_dao.contar_no_leidas(usuario_id)
            return jsonify({'success': True, 'count': count})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400

    
    @app.route('/api/notificaciones/todas')
    @login_required
    def api_obtener_todas_notificaciones():
        """API para obtener todas las notificaciones"""
        try:
            usuario_id = session.get('usuario_id')
            notificaciones = _get_notif_todas(usuario_id)  # OPTIMIZADO: desde caché
            
            # Formatear para el frontend
            notificaciones_formateadas = []
            for notif in notificaciones:
                # Determinar icono según tipo
                iconos = {
                    'payment': 'dollar-sign',
                    'membership': 'credit-card',
                    'client': 'user-plus',
                    'vencimiento': 'calendar-xmark',
                    'vencimiento_proximo': 'clock',
                    'moroso': 'user-slash',
                    'stock': 'triangle-exclamation',
                    'sistema': 'bell'
                }
                
                icono = iconos.get(notif['tipo'], 'bell')
                
                # Calcular tiempo transcurrido
                fecha_creacion = notif['fecha_creacion']
                tiempo = calcular_tiempo_relativo(fecha_creacion)
                
                notificaciones_formateadas.append({
                    'id': notif['id'],
                    'type': notif['tipo'],
                    'title': notif['titulo'],
                    'message': notif['mensaje'],
                    'time': tiempo,
                    'unread': notif['leida'] == 0,
                    'cliente_nombre': notif.get('cliente_nombre'),
                    'icon': icono
                })
            
            return jsonify({'success': True, 'data': notificaciones_formateadas})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/notificaciones/<int:notificacion_id>/leer', methods=['POST'])
    @login_required
    def api_marcar_notificacion_leida(notificacion_id):
        """API para marcar una notificación como leída"""
        try:
            success = notificacion_dao.marcar_como_leida(notificacion_id)
            if success:
                _invalidar_cache_notif(session.get('usuario_id'))  # forzar recarga desde BD
                return jsonify({'success': True, 'message': 'Notificación marcada como leída'})
            else:
                return jsonify({'success': False, 'message': 'Error al marcar notificación'}), 400
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/notificaciones/leer-todas', methods=['POST'])
    @login_required
    def api_marcar_todas_leidas():
        """API para marcar todas las notificaciones como leídas"""
        try:
            usuario_id = session.get('usuario_id')
            success = notificacion_dao.marcar_todas_como_leidas(usuario_id)
            if success:
                _invalidar_cache_notif(usuario_id)  # forzar recarga desde BD
                return jsonify({'success': True, 'message': 'Todas las notificaciones marcadas como leídas'})
            else:
                return jsonify({'success': False, 'message': 'Error al marcar notificaciones'}), 400
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400

    @app.route('/api/notificaciones/verificar-vencimientos', methods=['POST'])
    @login_required
    def api_verificar_vencimientos():
        """
        Verifica membresías próximas a vencer (≤ 3 días) y ya vencidas,
        y genera notificaciones automáticas evitando duplicados diarios.
        Se llama desde el frontend al cargar la página y periódicamente.
        """
        try:
            resultado = notificacion_dao.verificar_vencimientos_y_notificar()

            # Si se crearon notificaciones nuevas, invalida el caché para
            # que el contador del navbar se actualice de inmediato.
            if resultado.get('total', 0) > 0:
                _invalidar_cache_notif()  # limpia caché de todos los usuarios

            return jsonify({
                'success': True,
                'proximos_creados': resultado['proximos_creados'],
                'vencidos_creados': resultado['vencidos_creados'],
                'total_nuevas': resultado['total']
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500


def calcular_tiempo_relativo(fecha_str):
    """Calcula el tiempo relativo para mostrar en notificaciones"""
    try:
        # MySQL puede devolver datetime object o string — manejar ambos casos
        if hasattr(fecha_str, 'total_seconds'):
            # Es un timedelta — no debería pasar pero por si acaso
            fecha = datetime.now() - fecha_str
        elif hasattr(fecha_str, 'strftime'):
            # Ya es un objeto datetime de MySQL
            fecha = fecha_str
        elif fecha_str and ' ' in str(fecha_str):
            fecha = datetime.strptime(str(fecha_str), '%Y-%m-%d %H:%M:%S')
        elif fecha_str:
            fecha = datetime.strptime(str(fecha_str)[:10], '%Y-%m-%d')
        else:
            return 'Fecha desconocida'

        ahora = datetime.now()
        diferencia = ahora - fecha
        segundos = diferencia.total_seconds()

        if segundos < 0:
            return 'Ahora mismo'
        elif segundos < 60:
            return 'Hace unos segundos'
        elif segundos < 3600:
            minutos = int(segundos / 60)
            return f'Hace {minutos} min' if minutos == 1 else f'Hace {minutos} mins'
        elif segundos < 86400:
            horas = int(segundos / 3600)
            return f'Hace {horas} hora' if horas == 1 else f'Hace {horas} horas'
        elif segundos < 604800:
            dias = int(segundos / 86400)
            return f'Hace {dias} día' if dias == 1 else f'Hace {dias} días'
        else:
            semanas = int(segundos / 604800)
            return f'Hace {semanas} semana' if semanas == 1 else f'Hace {semanas} semanas'
    except Exception:
        # Si todo falla, formatear la fecha de forma legible en español
        try:
            if hasattr(fecha_str, 'strftime'):
                return fecha_str.strftime('%d/%m/%Y %H:%M')
            return str(fecha_str)[:16]
        except Exception:
            return 'Fecha desconocida'

# ==========================================
# CONTROLADOR: RECUPERACIÓN DE CONTRASEÑA
# ==========================================

def init_password_recovery_controller(app):
    """Inicializa las rutas de recuperación de contraseña"""
    
    @app.route('/recuperar-password', methods=['GET', 'POST'])
    def recuperar_password():
        """Página de recuperación de contraseña con protección contra solicitudes repetidas"""
        if request.method == 'POST':
            email = request.form.get('email', '').strip().lower()
            
            # Verificar si el email existe en la base de datos
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, nombre_completo, email 
                FROM usuarios 
                WHERE LOWER(email) = %s AND estado = 'activo'
            ''', (email,))
            
            usuario = cursor.fetchone()
            
            if not usuario:
                conn.close()
                return render_template('recuperar_password.html', 
                                    error_message='No se encontró un usuario activo con ese correo electrónico.')
            
            # ================================================
            # VERIFICACIÓN: ¿Ya hay una solicitud reciente (últimas 24h)?
            # ================================================

            # Buscar TODAS las solicitudes recientes (usadas o no) en las últimas 24 horas
            cursor.execute(f'''
                SELECT id, fecha_creacion, usado 
                FROM password_reset_tokens 
                WHERE usuario_id = %s 
                AND fecha_creacion > NOW() - INTERVAL 24 HOUR
                ORDER BY fecha_creacion DESC
                LIMIT 1
            ''', (usuario['id'],))

            solicitud_reciente = cursor.fetchone()

            if solicitud_reciente:
                # Calcular cuándo puede solicitar nuevamente
                fecha_solicitud_str = solicitud_reciente['fecha_creacion']
                
                try:
                    # Parsear la fecha de creación (MySQL puede devolver datetime object)
                    fcs = fecha_solicitud_str
                    if hasattr(fcs, 'date'):
                        fecha_solicitud = fcs
                    elif ' ' in str(fcs):
                        fecha_solicitud = datetime.strptime(str(fcs), '%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_solicitud = datetime.strptime(str(fcs)[:10], '%Y-%m-%d')
                    
                    ahora = datetime.now()
                    diferencia = ahora - fecha_solicitud
                    horas_transcurridas = diferencia.total_seconds() / 3600
                    
                    # Si han pasado menos de 24 horas, mostrar error (sin importar si fue usada o no)
                    if horas_transcurridas < 24:
                        horas_restantes = 24 - horas_transcurridas
                        
                        # Formatear el mensaje de tiempo
                        if horas_restantes >= 1:
                            horas_enteras = int(horas_restantes)
                            minutos_restantes = int((horas_restantes - horas_enteras) * 60)
                            
                            mensaje_tiempo = ""
                            if horas_enteras > 0:
                                mensaje_tiempo += f"{horas_enteras} hora{'s' if horas_enteras > 1 else ''}"
                            if minutos_restantes > 0:
                                if horas_enteras > 0:
                                    mensaje_tiempo += " y "
                                mensaje_tiempo += f"{minutos_restantes} minuto{'s' if minutos_restantes > 1 else ''}"
                        else:
                            # Menos de 1 hora restante, mostrar solo minutos
                            minutos_restantes = int(horas_restantes * 60)
                            mensaje_tiempo = f"{minutos_restantes} minuto{'s' if minutos_restantes > 1 else ''}"
                        
                        # Mensaje diferente si ya usó el token
                        if solicitud_reciente['usado'] == 1:
                            mensaje_base = 'Ya restableciste tu contraseña recientemente'
                        else:
                            mensaje_base = 'Ya se envió un enlace de recuperación a este correo recientemente'
                        
                        conn.close()
                        return render_template('recuperar_password.html', 
                                            error_message=f'{mensaje_base}. '
                                                        f'Espere {mensaje_tiempo} antes de solicitar uno nuevo.')
                except Exception as e:
                    print(f"Error al parsear fecha: {e}")
                    # Si hay error en el parsing, continuar con el proceso
            # ================================================
            
            # Generar token de recuperación
            import secrets
            
            token = secrets.token_urlsafe(32)
            # Asegurar que el encoding sea consistente
            token_hash = hashlib.sha256(token.encode('utf-8')).hexdigest()
            
            # Fecha de expiración (1 hora) - ajustar zona horaria
            expiracion = datetime.now() + timedelta(hours=1)
            expiracion_str = expiracion.strftime('%Y-%m-%d %H:%M:%S')
            
            # Fecha actual para creación
            fecha_actual_str = get_current_timestamp_peru_value()
            
            # Guardar token en la base de datos
            cursor.execute('''
                INSERT INTO password_reset_tokens (usuario_id, token_hash, expiracion, usado, fecha_creacion)
                VALUES (%s, %s, %s, 0, %s)
            ''', (usuario['id'], token_hash, expiracion_str, fecha_actual_str))
            
            conn.commit()
            conn.close()
            
            # Crear enlace de recuperación
            reset_link = f"{request.host_url}restablecer-password/{token}"
            
            try:
                # Enviar correo electrónico
                config = configuracion_dao.obtener_actual()
                nombre_empresa = config.get('empresa_nombre', 'Sistema') if config else 'Sistema'
                
                msg = Message(
                subject="Recuperación de Contraseña",
                recipients=[usuario['email']],
                html=f'''
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="utf-8">
                    <style>
                        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                        .header {{ background: linear-gradient(135deg, #2E9D36 0%, #3BA847 100%); 
                                color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
                        .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
                        .button {{ display: inline-block; background: linear-gradient(135deg, #2E9D36 0%, #3BA847 100%); 
                                color: white !important; /* ¡IMPORTANTE: Forzar texto blanco! */
                                padding: 12px 24px; text-decoration: none; border-radius: 5px; 
                                font-weight: bold; margin: 20px 0; border: none; cursor: pointer; }}
                        .button:hover {{ background: linear-gradient(135deg, #25802C 0%, #2E9D36 100%); }}
                        .warning {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; margin: 15px 0; }}
                        .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; 
                                color: #666; font-size: 12px; text-align: center; }}
                        /* Estilo específico para enlaces dentro del botón */
                        a.button {{ color: white !important; text-decoration: none; }}
                        a.button:visited {{ color: white !important; }}
                        a.button:hover {{ color: white !important; }}
                        a.button:active {{ color: white !important; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h2>Recuperación de Contraseña</h2>
                        </div>
                        <div class="content">
                            <h3>Hola {usuario['nombre_completo']},</h3>
                            <p>Recibimos una solicitud para restablecer la contraseña de tu cuenta en <strong>{nombre_empresa}</strong>.</p>
                            
                            <p>Para restablecer tu contraseña, haz clic en el siguiente botón:</p>
                            
                            <p style="text-align: center;">
                                <a href="{reset_link}" class="button" style="color: white !important; text-decoration: none;">
                                    Restablecer Contraseña
                                </a>
                            </p>
                            
                            <p>O copia y pega este enlace en tu navegador:</p>
                            <p style="word-break: break-all; background: #eee; padding: 10px; border-radius: 4px;">
                                {reset_link}
                            </p>
                            
                            <div class="warning">
                                <strong>⚠️ Importante:</strong> 
                                <ul>
                                    <li>Este enlace expirará en 1 hora.</li>
                                    <li>Si no solicitaste este cambio, puedes ignorar este correo.</li>
                                    <li>Por seguridad, solo puedes solicitar un enlace cada 24 horas.</li>
                                </ul>
                            </div>
                            
                            <p>Si tienes problemas con el botón anterior, copia y pega la URL en tu navegador.</p>
                            
                            <div class="footer">
                                <p>Este es un correo automático, por favor no respondas a este mensaje.</p>
                                <p>&copy; 2025 {nombre_empresa}. Todos los derechos reservados.</p>
                            </div>
                        </div>
                    </div>
                </body>
                </html>
                    '''
                )
                
                mail.send(msg)
                
                return render_template('recuperar_password.html', 
                                    success_message='Se ha enviado un enlace de recuperación a tu correo electrónico. Revisa tu bandeja de entrada y spam.')
                
            except Exception as e:
                print(f"Error al enviar correo: {e}")
                # Si falla el envío del correo, eliminar el token creado
                try:
                    conn = get_connection()
                    cursor = conn.cursor()
                    cursor.execute('DELETE FROM password_reset_tokens WHERE token_hash = %s', (token_hash,))
                    conn.commit()
                    conn.close()
                except:
                    pass
                
                return render_template('recuperar_password.html', 
                                    error_message='Error al enviar el correo. Por favor, contacta al administrador.')
        
        # GET request
        return render_template('recuperar_password.html')
    
    @app.route('/restablecer-password/<token>', methods=['GET', 'POST'])
    def restablecer_password(token):
        """Página para restablecer la contraseña con token válido"""
        import datetime
        
        # Verificar token con encoding consistente
        token_hash = hashlib.sha256(token.encode('utf-8')).hexdigest()
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Consultar el token sin filtrar por fecha en el SQL para manejarlo en Python
        cursor.execute('''
            SELECT prt.*, u.nombre_completo, u.email 
            FROM password_reset_tokens prt
            JOIN usuarios u ON prt.usuario_id = u.id
            WHERE prt.token_hash = %s 
                AND u.estado = 'activo'
        ''', (token_hash,))
        
        token_data = cursor.fetchone()
        
        # Validaciones robustas en Python
        if not token_data:
            conn.close()
            return render_template('restablecer_password.html', 
                                error_message='El enlace de recuperación no es válido.')
        
        if token_data['usado'] == 1:
            conn.close()
            return render_template('restablecer_password.html', 
                                error_message='Este enlace ya ha sido utilizado anteriormente.')
        
        # Validación de expiración en Python (más segura que en SQL)
        ahora = datetime.datetime.now()
        expiracion = token_data['expiracion']
        
        # Asegurar que expiracion sea un objeto datetime
        if isinstance(expiracion, str):
            try:
                expiracion = datetime.datetime.strptime(expiracion, '%Y-%m-%d %H:%M:%S')
            except:
                pass

        if expiracion < ahora:
            conn.close()
            return render_template('restablecer_password.html', 
                                error_message=f'El enlace ha expirado. (Expiró el: {expiracion.strftime("%d/%m/%Y %H:%M")})')
        
        if request.method == 'POST':
            nueva_password = request.form.get('password', '')
            confirmar_password = request.form.get('confirm_password', '')
            
            # Validaciones
            if not nueva_password:
                conn.close()
                return render_template('restablecer_password.html', 
                                    token=token,
                                    error_message='La contraseña es requerida.')
            
            if nueva_password != confirmar_password:
                conn.close()
                return render_template('restablecer_password.html', 
                                    token=token,
                                    error_message='Las contraseñas no coinciden.')
            
            if len(nueva_password) < 6:
                conn.close()
                return render_template('restablecer_password.html', 
                                    token=token,
                                    error_message='La contraseña debe tener al menos 6 caracteres.')
            
            # ==========================================
            # ACTUALIZAR CONTRASEÑA ENCRIPTADA
            # ==========================================
            from dao import usuario_dao
            
            # Usar el método del DAO que ya encripta la contraseña
            usuario_dao.actualizar_password(token_data['usuario_id'], nueva_password)
            
            # Marcar token como usado
            cursor.execute('UPDATE password_reset_tokens SET usado = 1 WHERE id = %s', 
                        (token_data['id'],))
            
            conn.commit()
            conn.close()
            
            # Enviar correo de confirmación
            try:
                config = configuracion_dao.obtener_actual()
                nombre_empresa = config.get('empresa_nombre', 'Sistema') if config else 'Sistema'
                
                msg = Message(
                    subject=f"Contraseña Actualizada - {nombre_empresa}",
                    recipients=[token_data['email']],
                    html=f'''
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <meta charset="utf-8">
                        <style>
                            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                            .header {{ background: linear-gradient(135deg, #2E9D36 0%, #3BA847 100%); 
                                    color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
                            .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; }}
                            .success {{ background: #d4edda; border-left: 4px solid #28a745; padding: 10px; margin: 15px 0; }}
                            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; 
                                    color: #666; font-size: 12px; text-align: center; }}
                        </style>
                    </head>
                    <body>
                        <div class="container">
                            <div class="header">
                                <h2>Contraseña Actualizada Exitosamente</h2>
                            </div>
                            <div class="content">
                                <h3>Hola {token_data['nombre_completo']},</h3>
                                <p>Tu contraseña ha sido restablecida exitosamente.</p>
                                
                                <div class="success">
                                    <strong>✅ Cambio completado:</strong> Ya puedes iniciar sesión con tu nueva contraseña.
                                </div>
                                
                                <p><strong>Fecha y hora del cambio:</strong> {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
                                
                                <div class="warning">
                                    <strong>🔒 Seguridad:</strong> 
                                    <ul>
                                        <li>No compartas tu contraseña con nadie</li>
                                        <li>Utiliza una contraseña única y segura</li>
                                        <li>Cierra sesión cuando uses dispositivos compartidos</li>
                                        <li>Recuerda que solo puedes solicitar un enlace de recuperación cada 24 horas</li>
                                    </ul>
                                </div>
                                
                                <p>Si no realizaste este cambio, por favor contacta inmediatamente al administrador del sistema.</p>
                                
                                <div class="footer">
                                    <p>Este es un correo automático, por favor no respondas a este mensaje.</p>
                                    <p>&copy; 2025 {nombre_empresa}. Todos los derechos reservados.</p>
                                </div>
                            </div>
                        </div>
                    </body>
                    </html>
                    '''
                )
                mail.send(msg)
            except Exception as e:
                print(f"Error al enviar correo de confirmación: {e}")
            
            return render_template('restablecer_password.html', 
                                success_message='¡Contraseña restablecida exitosamente! Ya puedes iniciar sesión con tu nueva contraseña.')
        
        conn.close()
        
        return render_template('restablecer_password.html', token=token)

def limpiar_tokens_expirados():
    """Limpia tokens de recuperación expirados (ejecutar periódicamente)"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Limpiar tokens expirados (más de 1 hora)
    cursor.execute(f'''
        DELETE FROM password_reset_tokens 
        WHERE expiracion < NOW()
    ''')
    
    # Limpiar tokens usados (más de 7 días)
    cursor.execute(f'''
        DELETE FROM password_reset_tokens 
        WHERE usado = 1 AND fecha_creacion < NOW()
    ''')
    
    # Limpiar solicitudes muy antiguas sin usar (más de 7 días)
    cursor.execute(f'''
        DELETE FROM password_reset_tokens 
        WHERE usado = 0 AND fecha_creacion < NOW()
    ''')
    
    conn.commit()
    conn.close()

# ==========================================
# CONTROLADOR: REPORTES
# ==========================================

def init_reportes_controller(app):
    """Inicializa las rutas de reportes"""
    
    @app.route('/reportes')
    @login_required
    @permiso_required('reportes')
    def reportes():
        """Página de reportes - vista simplificada con selector de tarjetas"""
        return render_template('reportes.html')

    @app.route('/api/reportes/estadisticas')
    @login_required
    def api_reportes_estadisticas():
        """API para obtener estadísticas en tiempo real para el dashboard de reportes"""
        try:
            estadisticas = obtener_estadisticas_reporte()
            return jsonify({
                'success': True,
                'data': estadisticas
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/reportes/generar', methods=['POST'])
    @login_required
    def api_generar_reporte():
        """API para generar reportes dinámicos según filtros"""

        def serializar_valor(v):
            """Convierte tipos MySQL no serializables a string"""
            from datetime import timedelta, date, datetime as dt
            if isinstance(v, timedelta):
                total = int(v.total_seconds())
                return f"{total//3600:02d}:{(total%3600)//60:02d}:{total%60:02d}"
            if isinstance(v, (date, dt)):
                return v.isoformat()
            return v

        def serializar_row(row):
            """Aplica serializar_valor a todos los campos de un dict"""
            return {k: serializar_valor(v) for k, v in row.items()}

        try:
            data = request.get_json()
            tipo_reporte = data.get('tipo_reporte', 'general')
            fecha_inicio = data.get('fecha_inicio')
            fecha_fin = data.get('fecha_fin')
            
            if not fecha_inicio or not fecha_fin:
                return jsonify({'success': False, 'message': 'Fechas requeridas'}), 400
            
            conn = get_connection()
            cursor = conn.cursor()
            
            resultado = {}
            
            if tipo_reporte == 'clientes':
                # Reporte de clientes - datos completos para la tabla
                sub_tipo = data.get('sub_tipo', 'todos')  # 'todos' o ID de plan específico
                
                # Obtener planes activos para mostrar como botones
                cursor.execute(f'''
                    SELECT id, nombre, permite_aplazamiento 
                    FROM planes_membresia 
                    WHERE habilitado = 1 
                    ORDER BY nombre
                ''')
                planes_activos = [dict(row) for row in cursor.fetchall()]
                
                if sub_tipo == 'membresias':
                    # Todas las membresías existentes (planes de membresía)
                    cursor.execute(f'''
                        SELECT 
                            id,
                            nombre,
                            codigo,
                            precio,
                            duracion,
                            CASE WHEN habilitado = 1 THEN 'Activo' ELSE 'Inactivo' END as estado,
                            fecha_creacion as fecha_inicio,
                            '' as fecha_vencimiento,
                            '' as dni,
                            '' as telefono,
                            u.nombre_completo as usuario_registro,
                            permite_aplazamiento
                        FROM planes_membresia p
                        LEFT JOIN usuarios u ON p.usuario_id = u.id        
                        ORDER BY id DESC
                    ''')
                elif sub_tipo.isdigit():  # Si es un ID de plan específico
                    cursor.execute('SELECT permite_aplazamiento FROM planes_membresia WHERE id = %s', (int(sub_tipo),))
                    plan_info = cursor.fetchone()
                    permite_aplazamiento = plan_info['permite_aplazamiento'] == 1 if plan_info else False
                    # Clientes de un plan específico - CON MÉTODO Y ESTADO CORREGIDOS
                    cursor.execute(f'''
                        SELECT 
                            c.id,
                            c.nombre_completo as nombre,
                            c.dni,
                            c.telefono,
                            p.nombre as plan,
                            p.permite_aplazamiento,
                            CASE 
                                WHEN c.activo = 1 AND (c.fecha_vencimiento IS NULL OR DATE(c.fecha_vencimiento) >= {get_current_date_expression()}) THEN 'Activo'
                                ELSE 'Inactivo'
                            END as estado,
                            c.fecha_inicio,
                            c.fecha_vencimiento,
                            u.nombre_completo as usuario_registro,
                            -- Método del último pago completado
                            (SELECT pa.metodo_pago FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado' ORDER BY pa.fecha_pago DESC LIMIT 1) as metodo_pago,
                            CASE
                                -- 1. Tiene pago pendiente explícito → Pendiente
                                WHEN EXISTS (SELECT 1 FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'pendiente') THEN 'Pendiente'
                                -- 2. Tiene completado Y no tiene pendiente → Pagado
                                WHEN EXISTS (SELECT 1 FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado') THEN 'Pagado'
                                -- 3. Sin ningún pago y fecha vencida → Vencido
                                WHEN c.fecha_vencimiento IS NOT NULL AND DATE(c.fecha_vencimiento) < {get_current_date_expression()} THEN 'Vencido'
                                -- 4. Sin pagos y sin vencer → Pendiente
                                ELSE 'Pendiente'
                            END as estado_pago
                        FROM clientes c
                        JOIN planes_membresia p ON c.plan_id = p.id
                        LEFT JOIN usuarios u ON c.usuario_id = u.id           
                        WHERE c.plan_id = %s AND c.activo = 1
                        AND DATE(c.fecha_registro) BETWEEN %s AND %s
                        ORDER BY c.nombre_completo
                    ''', (int(sub_tipo), fecha_inicio, fecha_fin))
                else:
                    # Todos los clientes - CON MÉTODO Y ESTADO CORREGIDOS
                    cursor.execute(f'''
                        SELECT 
                            c.id,
                            c.nombre_completo as nombre,
                            c.dni,
                            c.telefono,
                            p.nombre as plan,
                            p.permite_aplazamiento,
                            CASE 
                                WHEN c.activo = 1 AND (c.fecha_vencimiento IS NULL OR DATE(c.fecha_vencimiento) >= {get_current_date_expression()}) THEN 'Activo'
                                ELSE 'Inactivo'
                            END as estado,
                            c.fecha_inicio,
                            c.fecha_vencimiento,
                            u.nombre_completo as usuario_registro,
                            -- Método del último pago completado
                            (SELECT pa.metodo_pago FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado' ORDER BY pa.fecha_pago DESC LIMIT 1) as metodo_pago,
                            CASE
                                -- 1. Tiene pago pendiente explícito → Pendiente
                                WHEN EXISTS (SELECT 1 FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'pendiente') THEN 'Pendiente'
                                -- 2. Tiene completado Y no tiene pendiente → Pagado
                                WHEN EXISTS (SELECT 1 FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado') THEN 'Pagado'
                                -- 3. Sin ningún pago y fecha vencida → Vencido
                                WHEN c.fecha_vencimiento IS NOT NULL AND DATE(c.fecha_vencimiento) < {get_current_date_expression()} THEN 'Vencido'
                                -- 4. Sin pagos y sin vencer → Pendiente
                                ELSE 'Pendiente'
                            END as estado_pago
                        FROM clientes c
                        LEFT JOIN planes_membresia p ON c.plan_id = p.id
                        LEFT JOIN usuarios u ON c.usuario_id = u.id           
                        WHERE DATE(c.fecha_registro) BETWEEN %s AND %s
                        ORDER BY c.id DESC
                    ''', (fecha_inicio, fecha_fin))
                
                clientes = [serializar_row(r) for r in cursor.fetchall()]
                # Obtener información del plan si es específico
                permite_aplazamiento_plan = False
                if sub_tipo.isdigit():
                    cursor.execute('SELECT permite_aplazamiento FROM planes_membresia WHERE id = %s', (int(sub_tipo),))
                    plan_result = cursor.fetchone()
                    if plan_result:
                        permite_aplazamiento_plan = plan_result['permite_aplazamiento'] == 1
                            
                resultado = {
                    'sub_tipo': sub_tipo,
                    'planes_activos': planes_activos,  # Agregar esta línea
                    'clientes': [
                        {
                            'id': row['id'],
                            'nombre': row['nombre'],
                            'dni': row['dni'],
                            'telefono': row['telefono'] or '',
                            'plan': row['plan'] or 'Sin plan',
                            'estado': row['estado'],
                            'fecha_inicio': row['fecha_inicio'] or '',
                            'fecha_vencimiento': row['fecha_vencimiento'] or '',
                            'usuario_registro': row['usuario_registro'],
                            'permite_aplazamiento': row.get('permite_aplazamiento', 0) == 1 if 'permite_aplazamiento' in row else False,
                            # NUEVOS CAMPOS AGREGADOS
                            'metodo_pago': row['metodo_pago'] if row['metodo_pago'] else '',
                            'estado_pago': row['estado_pago'] if 'estado_pago' in row else row['estado']
                        } for row in clientes
                    ],
                    'permite_aplazamiento_plan': permite_aplazamiento_plan
                }
                
            elif tipo_reporte == 'ventas':
                # Reporte de ventas de productos - datos completos para la tabla
                cursor.execute('''
                    SELECT 
                        v.id,
                        DATE(v.fecha_venta) as fecha,
                        TIME(v.fecha_venta) as hora,
                        v.metodo_pago,
                        CASE 
                            WHEN v.tipo_venta = 'usuario' THEN COALESCE(u_usuario.nombre_completo, 'Usuario')
                            ELSE COALESCE(c.nombre_completo, 'Cliente General')
                        END as cliente,
                        v.total,
                        v.estado,
                        COALESCE(u_registro.nombre_completo, u_usuario.nombre_completo, 'Sistema') as empleado
                    FROM ventas v
                    LEFT JOIN clientes c ON v.cliente_id = c.id
                    LEFT JOIN usuarios u_usuario ON v.usuario_id = u_usuario.id
                    LEFT JOIN usuarios u_registro ON v.usuario_registro_id = u_registro.id
                    WHERE v.estado = 'completado'
                    AND v.fecha_venta BETWEEN %s AND %s
                    ORDER BY v.fecha_venta DESC
                ''', (fecha_inicio + ' 00:00:00', fecha_fin + ' 23:59:59'))
                
                ventas_rows = [serializar_row(r) for r in cursor.fetchall()]
                ventas_list = []
                
                for v_row in ventas_rows:
                    # Obtener detalles para el tooltip
                    cursor.execute('''
                        SELECT p.nombre, dv.cantidad, dv.precio_unitario, dv.subtotal
                        FROM detalle_ventas dv
                        JOIN productos p ON dv.producto_id = p.id
                        WHERE dv.venta_id = %s
                    ''', (v_row['id'],))
                    detalles = cursor.fetchall()
                    detalles_list = [dict(d) for d in detalles]
                    
                    v_dict = dict(v_row)
                    v_dict['detalles'] = detalles_list
                    ventas_list.append(v_dict)
                
                resultado = {
                    'ventas': ventas_list
                }
                
            elif tipo_reporte == 'membresias':
                # Reporte de membresías - información de planes de membresía
                cursor.execute('''
                    SELECT 
                        p.id,
                        p.codigo,
                        p.nombre,
                        p.precio,
                        p.duracion,
                        p.fecha_creacion,
                        CASE WHEN p.qr_habilitado = 1 THEN 'X' ELSE '' END as tiene_qr,
                        CASE WHEN p.permite_aplazamiento = 1 THEN 'X' ELSE '' END as permite_aplazamiento,
                        CASE WHEN p.permite_invitados = 1 THEN 'X' ELSE '' END as permite_invitados,
                        CASE WHEN p.permite_invitados = 1 THEN p.cantidad_invitados ELSE '' END as numero_invitados,
                        CASE WHEN p.envia_whatsapp = 1 THEN 'X' ELSE '' END as permite_whatsapp,
                        u.nombre_completo as usuario_registro
                    FROM planes_membresia p
                    LEFT JOIN usuarios u ON p.usuario_id = u.id
                    WHERE p.habilitado = 1
                    AND DATE(p.fecha_creacion) BETWEEN %s AND %s
                    ORDER BY p.id DESC
                ''', (fecha_inicio, fecha_fin))
                
                membresias = cursor.fetchall()
                
                resultado = {
                    'membresias': [
                        {
                            'id': row['id'],
                            'codigo': row['codigo'] or '',
                            'nombre': row['nombre'],
                            'precio': float(row['precio']) if row['precio'] else 0,
                            'duracion': row['duracion'] or '',
                            'fecha_creacion': row['fecha_creacion'] or '',
                            'tiene_qr': row['tiene_qr'],
                            'permite_aplazamiento': row['permite_aplazamiento'],
                            'permite_invitados': row['permite_invitados'],
                            'numero_invitados': row['numero_invitados'],
                            'permite_whatsapp': row['permite_whatsapp'],
                            'usuario_registro': row['usuario_registro']
                        } for row in membresias
                    ]
                }
                
            elif tipo_reporte == 'asistencia':
                # Reporte de acceso/asistencia - datos completos para la tabla
                cursor.execute('''
                    SELECT 
                        a.id,   
                        DATE(a.fecha_hora_entrada) as fecha,
                        TIME(a.fecha_hora_entrada) as hora,
                        CASE 
                            WHEN a.tipo = 'invitado' THEN COALESCE(i.nombre, 'Invitado')
                            ELSE COALESCE(c.nombre_completo, 'Cliente')
                        END as nombre,
                        CASE 
                            WHEN a.tipo = 'invitado' THEN COALESCE(i.dni, '')
                            ELSE COALESCE(c.dni, '')
                        END as dni,
                        a.tipo as tipo_acceso,
                        a.metodo_acceso as metodo,
                        COALESCE(u.nombre_completo, 'Sistema') as usuario_registro
                    FROM accesos a
                    LEFT JOIN clientes c ON a.cliente_id = c.id AND a.tipo = 'cliente'
                    LEFT JOIN invitados i ON a.cliente_id = i.id AND a.tipo = 'invitado'
                    LEFT JOIN usuarios u ON a.usuario_id = u.id
                    WHERE a.fecha_hora_entrada BETWEEN %s AND %s
                    ORDER BY a.fecha_hora_entrada DESC
                    ''', (fecha_inicio + ' 00:00:00', fecha_fin + ' 23:59:59'))
                    
                accesos = [serializar_row(r) for r in cursor.fetchall()]
                
                resultado = {
                    'accesos': [
                        {
                            'id': row['id'],
                            'fecha': str(row['fecha']) if row['fecha'] is not None else '',
                            'hora': str(row['hora']) if row['hora'] is not None else '',
                            'nombre': row['nombre'] or 'Desconocido',
                            'dni': row['dni'] or '',
                            'tipo': row['tipo_acceso'] or 'entrada',
                            'metodo': row['metodo'] or 'Manual',
                            'usuario_registro': row['usuario_registro'] or 'Sistema'
                        } for row in accesos
                    ]
                }
            
            elif tipo_reporte == 'invitados':
                # Reporte de invitados - datos completos para la tabla
                cursor.execute('''
                    SELECT 
                        i.id,
                        i.nombre,
                        i.dni,
                        i.telefono,
                        c.nombre_completo as cliente_que_invita,
                        p.nombre as plan_cliente,
                        i.fecha_visita as fecha_registro,
                        i.estado,
                        u.nombre_completo as usuario_registro
                    FROM invitados i
                    LEFT JOIN clientes c ON i.cliente_titular_id = c.id
                    LEFT JOIN planes_membresia p ON c.plan_id = p.id
                    left join usuarios u on i.usuario_id = u.id
                    WHERE i.estado = 'activo'
                    AND DATE(i.fecha_visita) BETWEEN %s AND %s
                    ORDER BY i.id DESC
                ''', (fecha_inicio, fecha_fin))
                
                invitados = cursor.fetchall()
                
                resultado = {
                    'invitados': [
                        {
                            'id': row['id'],
                            'nombre': row['nombre'],
                            'dni': row['dni'],
                            'telefono': row['telefono'] or '',
                            'cliente_que_invita': row['cliente_que_invita'] or '',
                            'plan_cliente': row['plan_cliente'] or '',
                            'fecha_registro': row['fecha_registro'] or '',
                            'estado': row['estado'] or 'activo',
                            'usuario_registro': row['usuario_registro']
                        } for row in invitados
                    ]
                }
            
            elif tipo_reporte == 'productos':
                # Reporte de productos (inventario) - datos completos para la tabla
                cursor.execute('''
                    SELECT 
                        p.id,
                        p.nombre,
                        p.categoria,
                        p.stock,
                        p.precio as precio_compra,
                        p.precio as precio_venta,
                        CASE 
                            WHEN p.stock <= p.stock_minimo THEN 'Bajo Stock'
                            ELSE 'Normal'
                        END as estado_stock,
                        u.nombre_completo as usuario_registro       
                    FROM productos p
                    left join usuarios u on p.usuario_id = u.id
                    WHERE p.estado = 'activo'
                    AND DATE(p.fecha_registro) BETWEEN %s AND %s
                    ORDER BY p.id DESC
                ''', (fecha_inicio, fecha_fin))
                
                productos = cursor.fetchall()
                
                resultado = {
                    'productos': [
                        {
                            'id': row['id'],
                            'nombre': row['nombre'],
                            'codigo': '',
                            'categoria': row['categoria'] or 'Sin categoría',
                            'stock': row['stock'],
                            'precio_compra': float(row['precio_compra']) if row['precio_compra'] else 0,
                            'precio': float(row['precio_venta']) if row['precio_venta'] else 0,
                            'proveedor': '',
                            'estado_stock': row['estado_stock'],
                            'usuario_registro': row['usuario_registro']
                        } for row in productos
                    ]
                }
                

            elif tipo_reporte == 'empleados':
                # Reporte de empleados - datos completos para la tabla
                cursor.execute(f'''
                    SELECT 
                        u.id,
                        u.nombre_completo as nombre,
                        u.dni,
                        u.telefono,
                        u.email,
                        r.nombre as rol,
                        u.fecha_registro as fecha_contratacion,
                        u.estado,
                        uc.nombre_completo as usuario_registro
                    FROM usuarios u
                    LEFT JOIN roles r ON u.rol_id = r.id
                    LEFT JOIN usuarios uc ON u.usuario_creador_id = uc.id
                    WHERE u.estado != 'eliminado'
                    AND DATE(u.fecha_registro) BETWEEN %s AND %s
                    ORDER BY u.id DESC
                ''', (fecha_inicio, fecha_fin))
                
                empleados = cursor.fetchall()
                
                resultado = {
                    'empleados': [
                        {
                            'id': row['id'],
                            'nombre': row['nombre'],
                            'dni': row['dni'],
                            'telefono': row['telefono'] or '',
                            'email': row['email'] or '',
                            'direccion': '',
                            'rol': row['rol'] or 'Entrenador',
                            'fecha_contratacion': str(row['fecha_contratacion'])[:10] if row['fecha_contratacion'] else '',
                            'sueldo': 0,
                            'estado': 'Activo' if row['estado'] == 'activo' else ('Inactivo' if row['estado'] == 'inactivo' else row['estado']),
                            'usuario_registro': row['usuario_registro'] or 'Sistema'
                        } for row in empleados
                    ]
                }
                
            elif tipo_reporte == 'pagos':
                # Reporte de pagos - Mostrar todos los clientes activos y su estado de pago REAL
                # Lógica alineada con cliente_dao:
                #   Pagado  = tiene completado Y no tiene pendiente (sin filtro de mes)
                #   Pendiente = tiene pendiente explícito
                #   Vencido = fecha_vencimiento pasada (sin pendiente ni completado activo)
                cursor.execute(f'''
                    SELECT 
                        c.id as cliente_id,
                        c.nombre_completo as cliente,
                        c.dni,
                        p.nombre as plan,
                        c.fecha_vencimiento,
                        (SELECT pa.fecha_pago FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado' ORDER BY pa.fecha_pago DESC LIMIT 1) as ultima_fecha_pago,
                        (SELECT pa.monto FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado' ORDER BY pa.fecha_pago DESC LIMIT 1) as ultimo_monto,
                        (SELECT pa.metodo_pago FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado' ORDER BY pa.fecha_pago DESC LIMIT 1) as ultimo_metodo,
                        (SELECT u.nombre_completo FROM pagos pa JOIN usuarios u ON pa.usuario_registro = u.id WHERE pa.cliente_id = c.id AND pa.estado = 'completado' ORDER BY pa.fecha_pago DESC LIMIT 1) as usuario_registro,
                        CASE
                            -- 1. Tiene pago pendiente explícito → Pendiente
                            WHEN EXISTS (SELECT 1 FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'pendiente') THEN 'Pendiente'
                            -- 2. Tiene completado Y no tiene pendiente → Pagado
                            WHEN EXISTS (SELECT 1 FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado') THEN 'Pagado'
                            -- 3. Sin ningún pago y fecha vencida → Vencido
                            WHEN c.fecha_vencimiento IS NOT NULL AND DATE(c.fecha_vencimiento) < {get_current_date_expression()} THEN 'Vencido'
                            -- 4. Sin pagos y sin vencer → Pendiente
                            ELSE 'Pendiente'
                        END as estado_pago
                    FROM clientes c
                    LEFT JOIN planes_membresia p ON c.plan_id = p.id
                    WHERE c.activo = 1
                    ORDER BY c.nombre_completo ASC
                ''')
                
                pagos_data = [serializar_row(r) for r in cursor.fetchall()]
                
                resultado = {
                    'pagos': [
                        {
                            'cliente': row['cliente'],
                            'dni': row['dni'],
                            'plan': row['plan'] or 'Sin plan',
                            'fecha_vencimiento': row['fecha_vencimiento'] or 'N/A',
                            'ultima_fecha_pago': row['ultima_fecha_pago'] or 'N/A',
                            'monto': float(row['ultimo_monto']) if row['ultimo_monto'] else 0,
                            'metodo_pago': row['ultimo_metodo'] or 'N/A',
                            'usuario': row['usuario_registro'] or 'N/A',
                            'estado': row['estado_pago']
                        } for row in pagos_data
                    ]
                }

            elif tipo_reporte == 'promociones':
                # Reporte de promociones - filtrado por rango de fechas
                cursor.execute(f'''
                    SELECT 
                        p.id,
                        p.nombre,
                        p.descripcion,
                        pm.nombre as plan_nombre,
                        p.porcentaje_descuento,
                        p.monto_descuento,
                        p.fecha_inicio,
                        p.fecha_fin,
                        p.sexo_aplicable,
                        p.activo
                    FROM promociones p
                    LEFT JOIN planes_membresia pm ON p.plan_id = pm.id
                    WHERE (
                        DATE(p.fecha_inicio) <= %s
                        AND DATE(p.fecha_fin) >= %s
                    )
                    ORDER BY p.fecha_inicio ASC
                ''', (fecha_fin, fecha_inicio))

                promos = [serializar_row(r) for r in cursor.fetchall()]

                resultado = {
                    'promociones': [
                        {
                            'id': row['id'],
                            'nombre': row['nombre'] or '',
                            'descripcion': row['descripcion'] or '',
                            'plan_nombre': row['plan_nombre'] or '-',
                            'porcentaje_descuento': float(row['porcentaje_descuento']) if row['porcentaje_descuento'] else None,
                            'monto_descuento': float(row['monto_descuento']) if row['monto_descuento'] else None,
                            'fecha_inicio': str(row['fecha_inicio'])[:10] if row['fecha_inicio'] else '',
                            'fecha_fin': str(row['fecha_fin'])[:10] if row['fecha_fin'] else '',
                            'sexo_aplicable': row['sexo_aplicable'] or 'todos',
                            'activo': row['activo']
                        } for row in promos
                    ]
                }

            elif tipo_reporte == 'general':
                # Reporte general consolidado
                cursor.execute(f'''
                    SELECT 
                        (SELECT COUNT(*) FROM clientes WHERE activo = 1) as total_clientes,
                        (SELECT COALESCE(SUM(monto), 0) FROM pagos WHERE estado = 'completado' AND fecha_pago BETWEEN %s AND %s) as ingresos_pagos,
                        (SELECT COALESCE(SUM(total), 0) FROM ventas WHERE estado = 'completado' AND fecha_venta BETWEEN %s AND %s) as ingresos_ventas,
                        (SELECT COUNT(*) FROM usuarios WHERE estado = 'activo' AND rol != 'administrador') as total_empleados,
                        (SELECT COUNT(*) FROM accesos WHERE fecha_hora_entrada BETWEEN %s AND %s) as total_accesos
                ''', (fecha_inicio, fecha_fin, fecha_inicio, fecha_fin, fecha_inicio, fecha_fin))
                
                stats = cursor.fetchone()
                
                # Ingresos por mes (últimos 6 meses)
                cursor.execute(f'''
                    WITH meses AS (
                        SELECT DATE_SUB({get_current_date_expression()}, INTERVAL 5 MONTH) as mes
                        UNION ALL SELECT DATE_SUB({get_current_date_expression()}, INTERVAL 4 MONTH)
                        UNION ALL SELECT DATE_SUB({get_current_date_expression()}, INTERVAL 3 MONTH)
                        UNION ALL SELECT DATE_SUB({get_current_date_expression()}, INTERVAL 2 MONTH)
                        UNION ALL SELECT DATE_SUB({get_current_date_expression()}, INTERVAL 1 MONTH)
                        UNION ALL SELECT {get_current_date_expression()}
                    )
                    SELECT 
                        DATE_FORMAT(meses.mes, '%Y-%m') as mes,
                        COALESCE(SUM(p.monto), 0) as ingresos_pagos,
                        COALESCE(SUM(v.total), 0) as ingresos_ventas
                    FROM meses
                    LEFT JOIN pagos p ON DATE_FORMAT(p.fecha_pago, '%Y-%m') = DATE_FORMAT(meses.mes, '%Y-%m') AND p.estado = 'completado'
                    LEFT JOIN ventas v ON DATE_FORMAT(v.fecha_venta, '%Y-%m') = DATE_FORMAT(meses.mes, '%Y-%m') AND v.estado = 'completado'
                    GROUP BY DATE_FORMAT(meses.mes, '%Y-%m')
                    ORDER BY meses.mes
                ''')
                
                ingresos_mensuales = cursor.fetchall()
                
                # Clientes por plan
                cursor.execute('''
                    SELECT 
                        p.nombre as plan,
                        COUNT(c.id) as cantidad
                    FROM clientes c
                    JOIN planes_membresia p ON c.plan_id = p.id
                    WHERE c.activo = 1
                    GROUP BY p.id
                    ORDER BY cantidad DESC
                ''')
                
                clientes_por_plan = cursor.fetchall()
                
                resultado = {
                    'total_clientes': stats['total_clientes'] or 0,
                    'ingresos_pagos': float(stats['ingresos_pagos']) if stats['ingresos_pagos'] else 0,
                    'ingresos_ventas': float(stats['ingresos_ventas']) if stats['ingresos_ventas'] else 0,
                    'total_empleados': stats['total_empleados'] or 0,
                    'total_accesos': stats['total_accesos'] or 0,
                    'ingresos_mensuales': [
                        {
                            'mes': row['mes'][5:7],  # Solo mes
                            'ingresos_pagos': float(row['ingresos_pagos']) if row['ingresos_pagos'] else 0,
                            'ingresos_ventas': float(row['ingresos_ventas']) if row['ingresos_ventas'] else 0
                        } for row in ingresos_mensuales
                    ],
                    'clientes_por_plan': [
                        {
                            'plan': row['plan'],
                            'cantidad': row['cantidad']
                        } for row in clientes_por_plan
                    ]
                }
            
            conn.close()
            
            return jsonify({
                'success': True,
                'data': resultado,
                'tipo_reporte': tipo_reporte,
                'fecha_inicio': fecha_inicio,
                'fecha_fin': fecha_fin
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/reportes/exportar', methods=['POST'])
    @login_required
    def api_exportar_reporte():
        """API para exportar reporte a PDF (simulado - puedes integrar una librería PDF real)"""
        try:
            data = request.get_json()
            tipo_reporte = data.get('tipo_reporte', 'general')
            fecha_inicio = data.get('fecha_inicio')
            fecha_fin = data.get('fecha_fin')
            
            # Aquí iría la lógica real para generar PDF
            # Por ahora devolvemos un mensaje de éxito simulado
            
            return jsonify({
                'success': True,
                'message': f'Reporte de {tipo_reporte} exportado exitosamente',
                'filename': f'reporte_{tipo_reporte}_{fecha_inicio}_a_{fecha_fin}.pdf'
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400

    # ==========================================
    # NUEVAS RUTAS: EXPORTAR PDF CON DATOS DE LA TABLA
    # ==========================================
    
    @app.route('/api/reportes/exportar/pdf', methods=['POST'])
    @login_required
    def exportar_pdf_datos():
        """
        Exporta reportes a PDF usando los datos de la tabla actual
        MEJORAS: Logo de empresa, orientación landscape, estructura jerárquica con detalles
        INCLUYE: Soporte para historial de membresías con diseño diferenciado
        """
        try:
            data = request.get_json()
            
            tipo_reporte = data.get('tipo_reporte')
            datos_tabla = data.get('datos_tabla')  # Datos de la tabla actual
            incluir_historial = data.get('incluir_historial', False)  # NUEVO: Flag para incluir historial
            historial_membresias = data.get('historial_membresias', {})  # NUEVO: Datos del historial
            incluir_entradas = data.get('incluir_entradas', False)
            entradas_inventario = data.get('entradas_inventario', {})
            # Log para debugging
            print(f"[PDF] Tipo reporte: {tipo_reporte}")
            print(f"[PDF] Tiene datos_tabla: {datos_tabla is not None}")
            print(f"[PDF] Incluir historial: {incluir_historial}")
            if incluir_historial:
                print(f"[PDF] Historial membresías: {len(historial_membresias)} clientes con historial")
            
            if datos_tabla:
                rows_count = len(datos_tabla.get('rows', []))
                rows_detalles_count = len(datos_tabla.get('rows_con_detalles', []))
                print(f"[PDF] Rows count: {rows_count}")
                print(f"[PDF] Rows con detalles: {rows_detalles_count}")
                print(f"[PDF] Tiene checkboxes: {datos_tabla.get('tiene_checkboxes', False)}")
            
            # Importar el generador de reportes
            from report_generator import ReporteGenerator
            generator = ReporteGenerator()
            
            # Si vienen datos de la tabla directamente, usarlos
            if datos_tabla and datos_tabla.get('rows'):
                rows = datos_tabla.get('rows', [])
                rows_con_detalles = datos_tabla.get('rows_con_detalles', [])
                tiene_checkboxes = datos_tabla.get('tiene_checkboxes', False)
                
                # Verificar que hay datos válidos
                if not rows or len(rows) == 0:
                    return jsonify({'success': False, 'message': 'No hay datos para exportar'}), 400
                
                # NUEVO: Si se solicita incluir historial de membresías, usar método especial
                if incluir_historial and historial_membresias and len(historial_membresias) > 0:
                    print(f"[PDF] Generando PDF CON HISTORIAL DE MEMBRESÍAS para {len(historial_membresias)} clientes...")
                    
                    # Generar HTML con historial de membresías
                    html = generator.generar_html_con_historial(
                        tipo_reporte=tipo_reporte,
                        datos_tabla=datos_tabla,
                        historial_membresias=historial_membresias,
                        landscape=True
                    )

                elif tipo_reporte == 'ventas' and rows_con_detalles and len(rows_con_detalles) > 0:
                    # Extraer IDs de ventas seleccionadas
                    venta_ids = [r['id'] for r in rows_con_detalles if r.get('id')]
                    
                    if venta_ids and len(venta_ids) > 0:
                        print(f"[PDF] Generando PDF de VENTAS con detalles para {len(venta_ids)} ventas...")
                        print(f"[PDF] IDs de ventas seleccionadas: {venta_ids}")
                        
                        # Obtener detalles de productos vendidos para cada venta
                        detalles_ventas = generator.obtener_detalles_ventas_por_ids(venta_ids)
                        print(f"[PDF] Detalles obtenidos para {len(detalles_ventas)} ventas")
                        
                        # Generar HTML con detalles de productos vendidos
                        html = generator.generar_html_con_detalles_ventas(
                            tipo_reporte='ventas',
                            datos_tabla=datos_tabla,
                            detalles_ventas=detalles_ventas,
                            landscape=True
                        )
                        print(f"[PDF] HTML generado con detalles de ventas")
                    else:
                        print(f"[PDF] No hay ventas seleccionadas, generando reporte estándar...")
                        # Sin ventas seleccionadas, usar método estándar
                        headers = datos_tabla.get('headers', [])
                        html = generator.generar_html_desde_tabla(
                            tipo_reporte='ventas',
                            headers=headers,
                            rows=rows,
                            details=[],
                            landscape=True
                        )

                elif incluir_entradas and entradas_inventario and len(entradas_inventario) > 0:
                    print(f"[PDF] Generando PDF CON ENTRADAS DE INVENTARIO para {len(entradas_inventario)} productos...")
                    html = generator.generar_html_con_entradas(
                        tipo_reporte=tipo_reporte,
                        datos_tabla=datos_tabla,
                        entradas_inventario=entradas_inventario,
                        landscape=True
                    )
                
                    
                # Si hay estructura jerárquica con detalles, usar el nuevo método
                elif rows_con_detalles and len(rows_con_detalles) > 0:
                    print(f"[PDF] Generando PDF con estructura jerárquica, {len(rows)} filas y {len(rows_con_detalles)} con detalles...")
                    
                    # Generar HTML con estructura jerárquica (siempre landscape para mejor visualización)
                    html = generator.generar_html_con_detalles(
                        tipo_reporte=tipo_reporte,
                        datos_tabla=datos_tabla,
                        landscape=True  # ORIENTACIÓN HORIZONTAL PARA TABLAS ANCHAS
                    )
                else:
                    # Usar método legacy para compatibilidad
                    headers = datos_tabla.get('headers', [])
                    details = datos_tabla.get('details', [])
                    
                    print(f"[PDF] Generando PDF estándar con {len(rows)} filas...")
                    
                    html = generator.generar_html_desde_tabla(
                        tipo_reporte=tipo_reporte,
                        headers=headers,
                        rows=rows,
                        details=details,
                        landscape=True
                    )
                
                print(f"[PDF] HTML generado, largo: {len(html)} caracteres")
                
                # Generar PDF
                pdf_buffer = generator.generar_pdf(
                    html, 
                    f"Reporte_{tipo_reporte}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                )
                
                print(f"[PDF] PDF generado exitosamente")
                
                return send_file(
                    pdf_buffer,
                    mimetype='application/pdf',
                    as_attachment=True,
                    download_name=f"Reporte_{tipo_reporte}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                )
            
            else:
                return jsonify({
                    'success': False, 
                    'message': 'No se encontraron datos para exportar. Genera un reporte primero.'
                }), 400
        
        except Exception as e:
            print(f"[PDF] Error en exportar_pdf: {str(e)}")
            traceback.print_exc()
            return jsonify({
                'success': False,
                'message': f'Error al generar PDF: {str(e)}'
            }), 500
    
    # ==========================================
    # NUEVAS RUTAS: EXPORTAR EXCEL CON DATOS DE LA TABLA
    # ==========================================
    
    @app.route('/api/reportes/exportar/excel', methods=['POST'])
    @login_required
    def exportar_excel_datos():
        """
        Exporta reportes a Excel usando los datos de la tabla actual
        MEJORAS: Soporte para historial de membresías y entradas de inventario
        """
        try:
            data = request.get_json()
            
            tipo_reporte = data.get('tipo_reporte')
            datos_tabla = data.get('datos_tabla')  # Datos de la tabla actual
            incluir_historial = data.get('incluir_historial', False)  # Flag para incluir historial de membresías
            historial_membresias = data.get('historial_membresias', {})  # Datos del historial de membresías
            incluir_entradas = data.get('incluir_entradas', False)  # Flag para incluir entradas de inventario
            entradas_inventario = data.get('entradas_inventario', {})  # Datos de entradas de inventario
            incluir_detalles = data.get('incluir_detalles', False)  # Flag para incluir detalles de ventas
            detalles_ventas = data.get('detalles_ventas', {})  # Datos de detalles de ventas
            
            # Log para debugging
            print(f"[EXCEL] Tipo reporte: {tipo_reporte}")
            print(f"[EXCEL] Tiene datos_tabla: {datos_tabla is not None}")
            print(f"[EXCEL] Incluir historial: {incluir_historial}")
            print(f"[EXCEL] Incluir entradas: {incluir_entradas}")
            print(f"[EXCEL] Incluir detalles: {incluir_detalles}")
            if incluir_historial:
                print(f"[EXCEL] Historial membresías: {len(historial_membresias)} clientes con historial")
            if incluir_entradas:
                print(f"[EXCEL] Entradas inventario: {len(entradas_inventario)} productos con entradas")
            if incluir_detalles:
                print(f"[EXCEL] Detalles ventas: {len(detalles_ventas)} ventas con detalles")
            
            if datos_tabla:
                print(f"[EXCEL] Headers: {datos_tabla.get('headers')}")
                print(f"[EXCEL] Rows count: {len(datos_tabla.get('rows', []))}")
                print(f"[EXCEL] Rows con detalles: {len(datos_tabla.get('rows_con_detalles', []))}")
                if datos_tabla.get('rows'):
                    print(f"[EXCEL] First row: {datos_tabla['rows'][0]}")
            
            # Importar el generador de reportes
            from report_generator import ReporteGenerator
            generator = ReporteGenerator()
            
            # Si vienen datos de la tabla directamente, usarlos
            if datos_tabla and datos_tabla.get('rows'):
                rows = datos_tabla.get('rows', [])
                rows_con_detalles = datos_tabla.get('rows_con_detalles', [])
                
                # Verificar que hay datos válidos
                if not rows or len(rows) == 0:
                    return jsonify({'success': False, 'message': 'No hay datos para exportar'}), 400
                
                # NUEVO: Si se solicita incluir historial de membresías, usar método especial
                if incluir_historial and historial_membresias and len(historial_membresias) > 0:
                    print(f"[EXCEL] Generando Excel CON HISTORIAL DE MEMBRESÍAS para {len(historial_membresias)} clientes...")
                    
                    # Generar Excel con historial de membresías
                    excel_buffer = generator.generar_excel_con_historial(
                        tipo_reporte=tipo_reporte,
                        datos_tabla=datos_tabla,
                        historial_membresias=historial_membresias,
                        filename=f"Reporte_{tipo_reporte}.xlsx",
                        sheet_name=tipo_reporte.capitalize()
                    )
                
                # NUEVO: Si se solicita incluir entradas de inventario, usar método especial
                elif incluir_entradas and entradas_inventario and len(entradas_inventario) > 0:
                    print(f"[EXCEL] Generando Excel CON ENTRADAS DE INVENTARIO para {len(entradas_inventario)} productos...")
                    
                    # Generar Excel con entradas de inventario
                    excel_buffer = generator.generar_excel_con_entradas(
                        tipo_reporte=tipo_reporte,
                        datos_tabla=datos_tabla,
                        entradas_inventario=entradas_inventario,
                        filename=f"Reporte_{tipo_reporte}.xlsx",
                        sheet_name=tipo_reporte.capitalize()
                    )
                
                # NUEVO: Si se solicita incluir detalles de ventas, usar método especial
                elif incluir_detalles and detalles_ventas and len(detalles_ventas) > 0:
                    print(f"[EXCEL] Generando Excel CON DETALLES DE VENTAS para {len(detalles_ventas)} ventas...")
                    
                    # Generar Excel con detalles de ventas
                    excel_buffer = generator.generar_excel_con_detalles_ventas(
                        tipo_reporte=tipo_reporte,
                        datos_tabla=datos_tabla,
                        detalles_ventas=detalles_ventas,
                        filename=f"Reporte_{tipo_reporte}.xlsx",
                        sheet_name=tipo_reporte.capitalize()
                    )
                
                # Si hay estructura jerárquica con detalles, usar el nuevo método
                elif rows_con_detalles and len(rows_con_detalles) > 0:
                    print(f"[EXCEL] Generando Excel con estructura jerárquica, {len(rows)} filas y {len(rows_con_detalles)} con detalles...")
                    
                    # Generar Excel con estructura jerárquica
                    excel_buffer = generator.generar_excel_con_detalles(
                        datos_tabla=datos_tabla,
                        filename=f"Reporte_{tipo_reporte}.xlsx",
                        sheet_name=tipo_reporte.capitalize()
                    )
                else:
                    # Usar método legacy para compatibilidad
                    headers = datos_tabla.get('headers', [])
                    
                    print(f"[EXCEL] Generando Excel estándar con {len(rows)} filas y {len(headers)} columnas...")
                    
                    # Generar Excel con los datos de la tabla
                    excel_buffer = generator.generar_excel(
                        data=rows,
                        headers=headers,
                        filename=f"Reporte_{tipo_reporte}.xlsx",
                        sheet_name=tipo_reporte.capitalize()
                    )
                
                print(f"[EXCEL] Excel generado exitosamente")
                
                return send_file(
                    excel_buffer,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    as_attachment=True,
                    download_name=f"Reporte_{tipo_reporte}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                )
            
            else:
                return jsonify({
                    'success': False, 
                    'message': 'No se encontraron datos para exportar. Genera un reporte primero.'
                }), 400
        
        except Exception as e:
            print(f"[EXCEL] Error en exportar_excel: {str(e)}")
            traceback.print_exc()
            return jsonify({
                'success': False,
                'message': f'Error al generar Excel: {str(e)}'
            }), 500

    @app.route('/api/reportes/ingresos-mensuales')
    @login_required
    def api_reportes_ingresos_mensuales():
        """API para obtener datos de ingresos mensuales"""
        try:
            datos = obtener_ingresos_mensuales()
            return jsonify({
                'success': True,
                'data': datos
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/reportes/clientes-por-plan')
    @login_required
    def api_reportes_clientes_por_plan():
        """API para obtener distribución de clientes por plan"""
        try:
            datos = clientes_por_plan()
            return jsonify({
                'success': True,
                'data': datos
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
    
    @app.route('/api/reportes/membresias-por-vencer')
    @login_required
    def api_reportes_membresias_por_vencer():
        """API para obtener membresías por vencer"""
        try:
            dias = request.args.get('dias', 7, type=int)
            datos = membresias_por_vencer(dias)
            return jsonify({
                'success': True,
                'data': datos
            })
        except Exception as e:
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 400
        
# Agrega estas funciones en init_reportes_controller, ANTES de la función @app.route('/reportes'):

def obtener_estadisticas_reporte():
    """Obtiene estadísticas reales para las tarjetas del dashboard de reportes"""
    
    conn = get_connection()
    cursor = conn.cursor()
    
    hoy = datetime.now()
    mes_pasado = hoy - timedelta(days=30)
    
    # 1. Ingresos Totales (este mes vs mes anterior)
    cursor.execute(f'''
        SELECT 
            COALESCE(SUM(CASE WHEN DATE_FORMAT(fecha_pago, '%Y-%m') = {get_current_month_expression()} 
                THEN monto ELSE 0 END), 0) as ingresos_mes_actual,
            COALESCE(SUM(CASE WHEN DATE_FORMAT(fecha_pago, '%Y-%m') = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 MONTH), '%Y-%m') 
                THEN monto ELSE 0 END), 0) as ingresos_mes_anterior
        FROM pagos 
        WHERE estado = 'completado'
    ''')
    ingresos = cursor.fetchone()
    ingresos_mes_actual = ingresos['ingresos_mes_actual'] or 0
    ingresos_mes_anterior = ingresos['ingresos_mes_anterior'] or 0
    
    # Calcular porcentaje de cambio
    if ingresos_mes_anterior > 0:
        cambio_ingresos = ((ingresos_mes_actual - ingresos_mes_anterior) / ingresos_mes_anterior) * 100
        cambio_ingresos_str = f"{'+' if cambio_ingresos >= 0 else ''}{cambio_ingresos:.1f}%"
    else:
        cambio_ingresos_str = "+0%"
    
    # 2. Total Clientes (nuevos este mes)
    cursor.execute(f'''
        SELECT 
            COUNT(*) as total_clientes,
            COUNT(CASE WHEN fecha_registro >= DATE_SUB({get_current_date_expression()}, INTERVAL 30 DAY) THEN 1 END) as nuevos_este_mes
        FROM clientes 
        WHERE activo = 1
    ''')
    clientes = cursor.fetchone()
    total_clientes = clientes['total_clientes'] or 0
    nuevos_clientes = clientes['nuevos_este_mes'] or 0
    
    # 3. Ventas de Productos (este mes vs mes anterior)
    cursor.execute(f'''
        SELECT 
            COALESCE(SUM(CASE WHEN DATE_FORMAT(fecha_venta, '%Y-%m') = {get_current_month_expression()} 
                THEN total ELSE 0 END), 0) as ventas_mes_actual,
            COALESCE(SUM(CASE WHEN DATE_FORMAT(fecha_venta, '%Y-%m') = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 MONTH), '%Y-%m') 
                THEN total ELSE 0 END), 0) as ventas_mes_anterior
        FROM ventas 
        WHERE estado = 'completado'
    ''')
    ventas = cursor.fetchone()
    ventas_mes_actual = ventas['ventas_mes_actual'] or 0
    ventas_mes_anterior = ventas['ventas_mes_anterior'] or 0
    
    # Calcular porcentaje de cambio
    if ventas_mes_anterior > 0:
        cambio_ventas = ((ventas_mes_actual - ventas_mes_anterior) / ventas_mes_anterior) * 100
        cambio_ventas_str = f"{'+' if cambio_ventas >= 0 else ''}{cambio_ventas:.1f}%"
    else:
        cambio_ventas_str = "+0%"
    
    conn.close()
    
    return {
        'ingresos_totales': float(ingresos_mes_actual),
        'total_clientes': total_clientes,
        'ventas_productos': float(ventas_mes_actual),
        'cambio_ingresos': cambio_ingresos_str,
        'nuevos_clientes': nuevos_clientes,
        'cambio_ventas': cambio_ventas_str
    }

def obtener_ingresos_mensuales():
    meses_data = pago_dao.obtener_ingresos_mensuales()

    meses_abreviados = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                        'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

    labels = []
    data = []

    for row in meses_data:
        mes_num = int(row['mes_numero'])

        if 1 <= mes_num <= 12:
            labels.append(meses_abreviados[mes_num - 1])
        else:
            labels.append(f"Mes {mes_num}")

        data.append(float(row['ingresos_totales']))

    return jsonify({
        'labels': labels,
        'data': data,
        'raw_data': meses_data
    })

def clientes_por_plan():
    data = cliente_dao.obtener_clientes_por_plan()

    colores = ['#3b82f6', '#10b981', '#f59e0b', '#06b6d4', '#8b5cf6', '#ef4444']
    data['colores'] = colores[:len(data['labels'])]

    return jsonify(data)

def membresias_por_vencer():
    dias = request.args.get('dias', default=7, type=int)
    membresias = cliente_dao.obtener_membresias_por_vencer(dias)
    return jsonify(membresias)