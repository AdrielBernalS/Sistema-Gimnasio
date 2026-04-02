"""
DAO de Cliente
Data Access Object para operaciones de base de datos de Clientes.
Adaptado para usar plan_id como foreign key.
"""

import sqlite3
import json
from datetime import datetime, timedelta

# Importar configuración de base de datos
import db_config
from db_helper import get_db_connection, is_sqlite, is_mysql, get_current_timestamp, get_current_timestamp_peru, get_current_timestamp_peru_value, get_current_date_peru, get_current_date_expression

# Intentar importar ZoneInfo, fallback a datetime si no está disponible
try:
    from zoneinfo import ZoneInfo
    ZONEINFO_DISPONIBLE = True
except ImportError:
    ZONEINFO_DISPONIBLE = False

# Importar DAO de promociones para calcular precios con descuento
try:
    from dao.promocion_dao import PromocionDAO
except ImportError:
    PromocionDAO = None

from models import Cliente


# Mapeo de códigos de país a zonas horarias y formatos
# El formato de fecha varía por país: DD/MM/YYYY (latinoamérica), MM/DD/YYYY (USA)
PAIS_ZONA_HORARIA = {
    '+51': {'zona': 'America/Lima', 'formato': '%d/%m/%Y', 'pais': 'Perú'},
    '+54': {'zona': 'America/Argentina/Buenos_Aires', 'formato': '%d/%m/%Y', 'pais': 'Argentina'},
    '+55': {'zona': 'America/Sao_Paulo', 'formato': '%d/%m/%Y', 'pais': 'Brasil'},
    '+56': {'zona': 'America/Santiago', 'formato': '%d/%m/%Y', 'pais': 'Chile'},
    '+57': {'zona': 'America/Bogota', 'formato': '%d/%m/%Y', 'pais': 'Colombia'},
    '+502': {'zona': 'America/Guatemala', 'formato': '%d/%m/%Y', 'pais': 'Guatemala'},
    '+504': {'zona': 'America/Tegucigalpa', 'formato': '%d/%m/%Y', 'pais': 'Honduras'},
    '+505': {'zona': 'America/Managua', 'formato': '%d/%m/%Y', 'pais': 'Nicaragua'},
    '+506': {'zona': 'America/Costa_Rica', 'formato': '%d/%m/%Y', 'pais': 'Costa Rica'},
    '+507': {'zona': 'America/Panama', 'formato': '%d/%m/%Y', 'pais': 'Panamá'},
    '+591': {'zona': 'America/La_Paz', 'formato': '%d/%m/%Y', 'pais': 'Bolivia'},
    '+593': {'zona': 'America/Guayaquil', 'formato': '%d/%m/%Y', 'pais': 'Ecuador'},
    '+595': {'zona': 'America/Asuncion', 'formato': '%d/%m/%Y', 'pais': 'Paraguay'},
    '+597': {'zona': 'America/Montevideo', 'formato': '%d/%m/%Y', 'pais': 'Uruguay'},
    '+598': {'zona': 'America/Montevideo', 'formato': '%d/%m/%Y', 'pais': 'Uruguay'},
    '+1': {'zona': 'America/New_York', 'formato': '%m/%d/%Y', 'pais': 'USA/Canadá'},
    '+34': {'zona': 'Europe/Madrid', 'formato': '%d/%m/%Y', 'pais': 'España'},
    '+44': {'zona': 'Europe/London', 'formato': '%d/%m/%Y', 'pais': 'Reino Unido'},
    # Predeterminado para Perú si no se reconoce
    None: {'zona': 'America/Lima', 'formato': '%d/%m/%Y', 'pais': 'Perú'}
}


class ClienteDAO:
    """Clase para acceder a datos de Clientes"""
    
    def __init__(self, db_path='sistema.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        # Usar la configuración de base de datos
        conn = get_db_connection()
        # Configurar row_factory para SQLite
        if is_sqlite():
            conn.row_factory = sqlite3.Row
        return conn
    
    def _get_plan_id_from_code(self, plan_codigo):
        """Convierte código de plan a ID numérico"""
        if not plan_codigo:
            return None
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM planes_membresia WHERE codigo = %s', (plan_codigo,))
        result = cursor.fetchone()
        conn.close()
        return result['id'] if result else None
    
    def _get_plan_code_from_id(self, plan_id):
        """Convierte ID de plan a código"""
        if not plan_id:
            return None
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT codigo FROM planes_membresia WHERE id = %s', (plan_id,))
        result = cursor.fetchone()
        conn.close()
        return result['codigo'] if result else None
    
    def _get_plan_nombre_from_id(self, plan_id):
        """Obtiene el nombre del plan"""
        if not plan_id:
            return None
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT nombre FROM planes_membresia WHERE id = %s', (plan_id,))
        result = cursor.fetchone()
        conn.close()
        return result['nombre'] if result else None
    
    def _detectar_pais_por_telefono(self, telefono):
        """
        Detecta el país basándose en el código del teléfono
        Ejemplos: +51, +54, +56, +1, +34, etc.
        """
        if not telefono:
            return PAIS_ZONA_HORARIA[None]
        
        telefono_limpio = telefono.strip()
        
        # Buscar coincidencias con prefijos conocidos (de más largo a más corto)
        for prefijo in sorted(PAIS_ZONA_HORARIA.keys(), key=lambda x: -len(str(x)) if x else 0):
            if prefijo and telefono_limpio.startswith(prefijo):
                return PAIS_ZONA_HORARIA[prefijo]
        
        # Si no encuentra coincidencia, usar predeterminado (Perú)
        return PAIS_ZONA_HORARIA[None]
    
    def _obtener_fecha_actual(self, telefono):
        """
        Obtiene la fecha y hora actual en la zona horaria del país del cliente
        Devuelve: datetime con la hora local del país
        """
        info_pais = self._detectar_pais_por_telefono(telefono)
        zona = info_pais['zona']
        
        # Intentar usar ZoneInfo si está disponible
        if ZONEINFO_DISPONIBLE:
            try:
                return datetime.now(ZoneInfo(zona))
            except Exception:
                # Si falla ZoneInfo, usar datetime local
                pass
        
        # Fallback a datetime local sin zona horaria
        return datetime.now()
    
    def _calcular_fecha_vencimiento(self, telefono, dias=None, duracion=None):
        """
        Calcula la fecha de vencimiento según el país del cliente.
        Por defecto suma 1 mes exacto (mismo día del mes siguiente).
        Si se especifica duracion (dict con 'tipo' y 'cantidad'), usa esa duración.
        Si se especifica dias (int), suma esa cantidad de días (legacy).
        Devuelve: (fecha_formateada, zona_horaria, pais)
        """
        info_pais = self._detectar_pais_por_telefono(telefono)
        zona = info_pais['zona']
        formato = info_pais['formato']
        pais = info_pais['pais']
        
        # Obtener la fecha actual en la zona horaria del cliente
        fecha_actual = self._obtener_fecha_actual(telefono)
        
        # Determinar tipo de duración
        usar_meses = False
        cantidad_meses = 1
        cantidad_dias = None
        cantidad_horas = None
        
        if duracion is not None and isinstance(duracion, dict):
            if duracion.get('tipo') == 'meses':
                usar_meses = True
                cantidad_meses = duracion.get('cantidad', 1)
            elif duracion.get('tipo') == 'horas':
                cantidad_horas = duracion.get('cantidad', 1)
            else:
                cantidad_dias = duracion.get('cantidad', 30)
        elif dias is not None:
            cantidad_dias = dias
        else:
            usar_meses = True  # Por defecto 1 mes
        
        if cantidad_horas is not None:
            # Si se especifica cantidad de horas, usar timedelta
            fecha_futura = fecha_actual + timedelta(hours=cantidad_horas)
        elif cantidad_dias is not None:
            # Si se especifica cantidad de días, usar timedelta
            fecha_futura = fecha_actual + timedelta(days=cantidad_dias)
        elif usar_meses:
            # Sumar la cantidad de meses especificada (mismo día del mes futuro)
            año_objetivo = fecha_actual.year
            mes_objetivo = fecha_actual.month + cantidad_meses
            
            # Ajustar año si los meses superan 12
            while mes_objetivo > 12:
                mes_objetivo -= 12
                año_objetivo += 1
            
            # Determinar el último día del mes de destino
            # Empezamos con el mismo día, pero si no existe en el mes destino, usamos el último día
            dia_objetivo = fecha_actual.day
            
            # Calcular último día del mes de destino
            # Para febrero y otros meses con menos de 31 días
            # Usamos un método simple: ir al primer día del mes siguiente y restar un día
            try:
                if mes_objetivo == 12:
                    fecha_temp = datetime(año_objetivo + 1, 1, 1)
                else:
                    fecha_temp = datetime(año_objetivo, mes_objetivo + 1, 1)
                fecha_temp = fecha_temp - timedelta(days=1)
                ultimo_dia_mes_objetivo = fecha_temp.day
            except ValueError:
                ultimo_dia_mes_objetivo = 28  # Valor seguro para febrero
            
            # Usar el mínimo entre el día objetivo y el último día del mes
            dia_final = min(dia_objetivo, ultimo_dia_mes_objetivo)
            
            # Crear la fecha de vencimiento (sin ZoneInfo para evitar problemas de compatibilidad)
            try:
                fecha_futura = datetime(
                    año_objetivo, 
                    mes_objetivo, 
                    dia_final,
                    fecha_actual.hour,
                    fecha_actual.minute,
                    fecha_actual.second,
                    fecha_actual.microsecond
                )
            except ValueError:
                # Si algo falla, caer a 28 de febrero como fallback
                fecha_futura = datetime(
                    año_objetivo, 
                    mes_objetivo, 
                    28,
                    fecha_actual.hour,
                    fecha_actual.minute,
                    fecha_actual.second,
                    fecha_actual.microsecond
                )
        
        # Formatear según el país (para guardar en la base de datos)
        fecha_formateada_db = fecha_futura.strftime('%Y-%m-%d %H:%M:%S')
        
        # También formatear para mostrar (dependiendo del país)
        fecha_formateada_pais = fecha_futura.strftime(formato)
        
        return fecha_formateada_db, fecha_formateada_pais, zona, pais
    
    def _generar_qr_code(self, cliente, zona_horaria=None):
        """Genera el contenido del QR code como JSON con la zona horaria correcta"""
        # Obtener información del país para el formato correcto
        info_pais = self._detectar_pais_por_telefono(cliente.telefono)
        formato_pais = info_pais['formato']
        
        # Si tenemos la fecha de vencimiento en formato datetime, formatearla según el país
        vencimiento_formateado = cliente.fecha_vencimiento
        if cliente.fecha_vencimiento and ' ' in str(cliente.fecha_vencimiento):
            # Es un datetime, lo formateamos según el país
            try:
                dt = datetime.strptime(str(cliente.fecha_vencimiento), '%Y-%m-%d %H:%M:%S')
                vencimiento_formateado = dt.strftime(formato_pais)
            except:
                pass
        
        qr_content = {
            'cliente': cliente.nombre_completo,
            'dni': cliente.dni,
            'plan': self._get_plan_nombre_from_id(cliente.plan_id),
            'telefono': cliente.telefono,
            'vencimiento': vencimiento_formateado,
            'zona_horaria': zona_horaria if zona_horaria else 'America/Lima'
        }
        return json.dumps(qr_content, ensure_ascii=False)
    
    def obtener_todos(self):
        """Obtiene todos los clientes con información del plan y conteo de invitados"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT c.*, 
                   p.nombre as plan_nombre, 
                   p.codigo as plan_codigo, 
                   p.precio as plan_precio, 
                   p.duracion as plan_duracion,
                   p.qr_habilitado, 
                   p.permite_aplazamiento, 
                   p.permite_invitados,
                   (SELECT COUNT(*) FROM invitados i WHERE i.cliente_titular_id = c.id AND i.estado != 'eliminado') as total_invitados,
                   (SELECT COUNT(*) FROM pagos pa WHERE pa.cliente_id = c.id AND pa.estado = 'completado') as tiene_pagos_completados,
                   (SELECT COUNT(*) FROM accesos ac WHERE ac.cliente_id = c.id AND (ac.tipo = 'cliente' OR ac.tipo IS NULL)) as tiene_accesos
            FROM clientes c
            LEFT JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.activo = 1
            ORDER BY c.fecha_inicio DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        # Convertir a lista de diccionarios
        clientes = [dict(row) for row in rows]
        
        # Calcular precios con descuento para cada cliente
        if PromocionDAO:
            promocion_dao = PromocionDAO()
            for cliente in clientes:
                plan_id = cliente.get('plan_id')
                precio_original = float(cliente.get('plan_precio', 0) or 0)
                sexo_cliente = cliente.get('sexo', None)
                
                # Calcular precio con descuento según el sexo y turno del cliente
                turno_cliente = cliente.get('turno', None)
                precio_descuento, descuento, promocion = promocion_dao.calcular_precio_con_descuento(
                    plan_id, precio_original, sexo_cliente, turno_cliente
                )
                
                cliente['plan_precio_original'] = precio_original
                cliente['plan_precio_descuento'] = precio_descuento
                cliente['plan_descuento'] = descuento
                cliente['plan_promocion'] = promocion
                cliente['tiene_promocion'] = True if promocion else False
        
        return clientes
    
    def obtener_por_id(self, cliente_id):
        """Obtiene un cliente por su ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT c.*, p.nombre as plan_nombre, p.codigo as plan_codigo
            FROM clientes c
            LEFT JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.id = %s
        ''', (cliente_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def obtener_por_dni(self, dni):
        """Obtiene un cliente por su DNI"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM clientes WHERE dni = %s', (dni,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def crear(self, cliente, generar_qr=True):

        # PRIMERO: Verificar si el DNI ya existe
        cliente_existente = self.obtener_por_dni(cliente.dni)
        
        if cliente_existente:
            # Si el cliente existe pero está eliminado (activo = 0), lo reactiva
            if cliente_existente.get('activo') == 0:
                # Calcular nueva fecha de inicio y vencimiento para el cliente reactivado
                plan_id = cliente.plan_id
                if plan_id is None or isinstance(plan_id, str):
                    plan_id = self._get_plan_id_from_code(plan_id)
                
                duracion_plan = {'tipo': 'meses', 'cantidad': 1}
                qr_code = None
                if plan_id:
                    plan_info = self._get_plan_info(plan_id)
                    if plan_info:
                        if plan_info.get('duracion'):
                            duracion_plan = self._parsear_duracion(plan_info.get('duracion'))
                        # Generar nuevo QR si el plan tiene QR habilitado
                        if plan_info.get('qr_habilitado', 0) == 1:
                            cliente_temp = Cliente(
                                id=cliente_existente['id'],
                                dni=cliente.dni,
                                nombre_completo=cliente.nombre_completo,
                                telefono=cliente.telefono,
                                plan_id=plan_id,
                                fecha_inicio=None,
                                fecha_vencimiento=None,
                                qr_code=None
                            )
                            zona_horaria = self._detectar_pais_por_telefono(cliente.telefono)['zona']
                            qr_code = self._generar_qr_code(cliente_temp, zona_horaria)
                
                fecha_actual = self._obtener_fecha_actual(cliente.telefono)
                fecha_inicio = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')
                fecha_registro = fecha_actual.strftime('%Y-%m-%d')  # Solo fecha sin hora
                fecha_vencimiento_db, _, zona_horaria, pais = self._calcular_fecha_vencimiento(cliente.telefono, duracion=duracion_plan)
                
                # Actualizar datos del cliente reactivado
                datos_actualizacion = {
                    'nombre_completo': cliente.nombre_completo,
                    'telefono': cliente.telefono,
                    'plan_id': plan_id,
                    'activo': 1,
                    'fecha_inicio': fecha_inicio,
                    'fecha_registro': fecha_registro,
                    'fecha_vencimiento': fecha_vencimiento_db,
                    'usuario_id': cliente.usuario_id,
                    'sexo': getattr(cliente, 'sexo', 'no_especificado')
                }
                if qr_code:
                    datos_actualizacion['qr_code'] = qr_code
                
                self.actualizar(cliente_existente['id'], datos_actualizacion)
                return cliente_existente['id']
            else:
                # Cliente activo, error
                raise ValueError(f"El DNI {cliente.dni} ya está registrado en el sistema")
        
        # SEGUNDO: Obtener toda la información necesaria ANTES de abrir conexión para INSERT
        # Convertir plan_codigo a plan_id si es necesario
        plan_id = cliente.plan_id
        if plan_id is None or isinstance(plan_id, str):
            # Si viene como código (plan_a, plan_b), convertir a ID
            plan_id = self._get_plan_id_from_code(plan_id)
        
        # Obtener duración del plan
        duracion_plan = {'tipo': 'meses', 'cantidad': 1}  # Por defecto 1 mes
        if plan_id:
            plan_info = self._get_plan_info(plan_id)
            if plan_info and plan_info.get('duracion'):
                duracion_plan = self._parsear_duracion(plan_info.get('duracion'))
        
        # Calcular fecha de inicio y vencimiento según el país del cliente
        fecha_actual = self._obtener_fecha_actual(cliente.telefono)
        fecha_inicio = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')
        fecha_registro = fecha_actual.strftime('%Y-%m-%d')  # Solo fecha sin hora
        
        if cliente.fecha_vencimiento is None:
            fecha_vencimiento_db, _, zona_horaria, pais = self._calcular_fecha_vencimiento(cliente.telefono, duracion=duracion_plan)
        else:
            fecha_vencimiento_db = cliente.fecha_vencimiento
            zona_horaria = self._detectar_pais_por_telefono(cliente.telefono)['zona']
        
        # Generar QR code solo si se solicita y no viene pre-configurado
        qr_code = getattr(cliente, 'qr_url', None) or cliente.qr_code
        if generar_qr and (qr_code is None or qr_code == ''):
            # Crear un objeto cliente temporal con los datos completos
            cliente_temp = Cliente(
                id=None,
                dni=cliente.dni,
                nombre_completo=cliente.nombre_completo,
                telefono=cliente.telefono,
                plan_id=plan_id,
                fecha_inicio=fecha_inicio,
                fecha_vencimiento=fecha_vencimiento_db,
                qr_code=None,
                usuario_id=cliente.usuario_id 
            )
            qr_code = self._generar_qr_code(cliente_temp, zona_horaria)
        elif not generar_qr:
            # Si no se debe generar QR, usar None
            qr_code = None
        
        # AHORA: Abrir conexión solo para el INSERT
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO clientes (dni, nombre_completo, telefono, plan_id, 
                                     fecha_inicio, fecha_vencimiento, qr_code, fecha_registro, usuario_id, turno, sexo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                cliente.dni, 
                cliente.nombre_completo, 
                cliente.telefono,
                plan_id, 
                fecha_inicio,
                fecha_vencimiento_db,
                qr_code,
                fecha_registro,
                cliente.usuario_id,
                cliente.turno,
                getattr(cliente, 'sexo', 'no_especificado')
            ))
            cliente_id = cursor.lastrowid
            conn.commit()
        finally:
            conn.close()
        return cliente_id
    
    def crear_from_dict(self, data):
        """Crea un cliente desde un diccionario"""
        # Convertir plan_actual (código) a plan_id (numérico)
        if 'plan_actual' in data and 'plan_id' not in data:
            data['plan_id'] = self._get_plan_id_from_code(data.get('plan_actual'))
            del data['plan_actual']
        
        if 'usuario_id' not in data:
            # Aquí puedes obtener el usuario_id de la sesión actual
            # Necesitarás importar la sesión desde Flask
            # Ejemplo: from flask import session
            # data['usuario_id'] = session.get('usuario_id')
            # Por ahora, lo dejamos como None si no viene
            pass

        # Verificar si el plan tiene QR habilitado
        plan_qr_habilitado = True
        if 'plan_id' in data and data['plan_id']:
            plan_info = self._get_plan_info(data['plan_id'])
            if plan_info:
                plan_qr_habilitado = plan_info.get('qr_habilitado', 1) == 1
        
        # Si el plan no tiene QR habilitado, no generar QR
        generar_qr = plan_qr_habilitado
        
        # Obtener duración del plan
        duracion_plan = {'tipo': 'meses', 'cantidad': 1}  # Por defecto 1 mes
        if 'plan_id' in data and data['plan_id']:
            plan_info = self._get_plan_info(data['plan_id'])
            if plan_info and plan_info.get('duracion'):
                duracion_plan = self._parsear_duracion(plan_info.get('duracion'))
        
        # Eliminar campos que no existen en la tabla simplificada
        campos_a_eliminar = ['email', 'fecha_nacimiento', 'direccion', 'foto', 'observacion']
        for campo in campos_a_eliminar:
            data.pop(campo, None)
        
        # Calcular fecha de vencimiento según el país y la duración del plan
        if 'fecha_vencimiento' not in data or data['fecha_vencimiento'] is None:
            telefono = data.get('telefono', '')
            fecha_vencimiento_db, _, _, _ = self._calcular_fecha_vencimiento(telefono, duracion=duracion_plan)
            data['fecha_vencimiento'] = fecha_vencimiento_db
        
        cliente = Cliente.from_dict(data)
        return self.crear(cliente, generar_qr=generar_qr)
    
    def _parsear_duracion(self, duracion_str):
        """
        Parsea una cadena de duración y devuelve un diccionario con tipo y cantidad.
        Ejemplos: "1 mes" -> {'tipo': 'meses', 'cantidad': 1}
                  "7 días" -> {'tipo': 'dias', 'cantidad': 7}
                  "2 horas" -> {'tipo': 'horas', 'cantidad': 2}
        """
        if not duracion_str:
            return {'tipo': 'meses', 'cantidad': 1}  # Por defecto 1 mes
        
        duracion_str = duracion_str.lower().strip()
        
        import re
        # Buscar número y unidad
        match = re.search(r'(\d+)\s*(hora|horas|día|dias|días|mes|meses|semana|semanas)', duracion_str)
        
        if match:
            cantidad = int(match.group(1))
            unidad = match.group(2)
            
            if 'hora' in unidad:
                return {'tipo': 'horas', 'cantidad': cantidad}
            elif 'día' in unidad or 'dia' in unidad:
                return {'tipo': 'dias', 'cantidad': cantidad}
            elif 'semana' in unidad:
                return {'tipo': 'dias', 'cantidad': cantidad * 7}
            elif 'mes' in unidad:
                return {'tipo': 'meses', 'cantidad': cantidad}
        
        # Si no se puede parsear, intentar extraer solo el número (asumir días)
        match_num = re.search(r'(\d+)', duracion_str)
        if match_num:
            return {'tipo': 'dias', 'cantidad': int(match_num.group(1))}
        
        return {'tipo': 'meses', 'cantidad': 1}  # Por defecto 1 mes
    
    def _get_plan_info(self, plan_id):
        """Obtiene información de un plan incluyendo flags de configuración"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT qr_habilitado, permite_aplazamiento, permite_invitados, duracion FROM planes_membresia WHERE id = %s', (plan_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def actualizar(self, cliente_id, datos):
        """Actualiza un cliente"""
        # Obtener datos actuales del cliente para poder generar QR si es necesario
        cliente_actual = self.obtener_por_id(cliente_id)
        
        # PRIMERO: Convertir plan_actual a plan_id ANTES de abrir conexión principal
        plan_id = None
        plan_cambio = False
        if 'plan_actual' in datos:
            plan_id = self._get_plan_id_from_code(datos.get('plan_actual'))
            datos['plan_id'] = plan_id
            del datos['plan_actual']
            # Verificar si el plan cambió
            if cliente_actual and cliente_actual.get('plan_id') != plan_id:
                plan_cambio = True
        elif 'plan_id' in datos:
            plan_id = datos['plan_id']
            # Verificar si el plan cambió
            if cliente_actual and cliente_actual.get('plan_id') != plan_id:
                plan_cambio = True
        
        # RECALCULAR FECHA DE VENCIMIENTO SI CAMBIÓ EL PLAN
        if plan_cambio and plan_id:
            plan_info = self._get_plan_info(plan_id)
            if plan_info and plan_info.get('duracion'):
                # Obtener el teléfono para detectar zona horaria
                telefono = datos.get('telefono') or (cliente_actual.get('telefono') if cliente_actual else '')
                
                # Parsear la duración del nuevo plan
                duracion_dict = self._parsear_duracion(plan_info['duracion'])
                
                # Calcular la nueva fecha de vencimiento desde HOY
                # La función retorna 4 valores: fecha_db, fecha_formateada, zona, pais
                fecha_vencimiento_db, fecha_vencimiento_formato, zona_horaria, pais = self._calcular_fecha_vencimiento(
                    telefono=telefono,
                    duracion=duracion_dict
                )
                
                # Actualizar la fecha de vencimiento en los datos (usar formato de base de datos)
                datos['fecha_vencimiento'] = fecha_vencimiento_db
                
                # También actualizar la fecha de inicio a HOY
                fecha_inicio = self._obtener_fecha_actual(telefono)
                # Guardar en formato de base de datos (YYYY-MM-DD HH:MM:SS)
                datos['fecha_inicio'] = fecha_inicio.strftime('%Y-%m-%d %H:%M:%S')
        
        # ELIMINAR INVITADOS SI EL NUEVO PLAN NO PERMITE INVITADOS
        if plan_cambio and plan_id:
            plan_info = self._get_plan_info(plan_id)
            if plan_info and plan_info.get('permite_invitados', 1) == 0:
                # El nuevo plan NO permite invitados, eliminar todos los invitados del cliente
                from dao.invitado_dao import InvitadoDAO
                invitado_dao = InvitadoDAO()
                # Obtener los invitados actuales del cliente
                invitados = invitado_dao.obtener_por_cliente(cliente_id)
                # Eliminar cada invitado (eliminación lógica)
                for invitado in invitados:
                    try:
                        invitado_dao.eliminar(invitado['id'])
                    except Exception as e:
                        print(f"Error al eliminar invitado {invitado['id']}: {e}")
        
        # Verificar si el nuevo plan tiene QR habilitado
        if plan_id:
            plan_info = self._get_plan_info(plan_id)
            if plan_info:
                if plan_info.get('qr_habilitado', 0) == 1:
                    # El plan TIENE QR habilitado → Generar/Actualizar QR
                    # Combinar datos actuales con los nuevos para generar el QR
                    telefono = datos.get('telefono') or (cliente_actual.get('telefono') if cliente_actual else '')
                    nombre = datos.get('nombre_completo') or (cliente_actual.get('nombre_completo') if cliente_actual else '')
                    dni = datos.get('dni') or (cliente_actual.get('dni') if cliente_actual else '')
                    # Usar la fecha de vencimiento recalculada si está disponible
                    fecha_vencimiento = datos.get('fecha_vencimiento') or (cliente_actual.get('fecha_vencimiento') if cliente_actual else None)
                    usuario_id = datos.get('usuario_id') or (cliente_actual.get('usuario_id') if cliente_actual else None)
                    
                    # Crear objeto cliente temporal para generar QR
                    cliente_temp = Cliente(
                        id=cliente_id,
                        dni=dni,
                        nombre_completo=nombre,
                        telefono=telefono,
                        plan_id=plan_id,
                        fecha_inicio=datos.get('fecha_inicio') or (cliente_actual.get('fecha_inicio') if cliente_actual else None),
                        fecha_vencimiento=fecha_vencimiento,
                        qr_code=None,
                        usuario_id=usuario_id
                    )
                    zona_horaria = self._detectar_pais_por_telefono(telefono)['zona']
                    datos['qr_code'] = self._generar_qr_code(cliente_temp, zona_horaria)
                else:
                    # El plan NO tiene QR, limpiar el campo
                    datos['qr_code'] = None
        
        # Eliminar campos que no existen en la tabla
        campos_a_eliminar = ['email', 'fecha_nacimiento', 'direccion', 'foto', 'observacion', 
                            'qr_generado', 'qr_url']
        for campo in campos_a_eliminar:
            datos.pop(campo, None)
        
        # Preparar campos y valores
        campos = []
        valores = []
        for key, value in datos.items():
            if key not in ['id']:
                campos.append(f'{key} = %s')
                valores.append(value)
        
        # AHORA: Abrir conexión solo para el UPDATE
        if campos:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                valores.append(cliente_id)
                query = f"UPDATE clientes SET {', '.join(campos)} WHERE id = %s"
                cursor.execute(query, valores)
                conn.commit()
            finally:
                conn.close()
        
        return True
    
    def eliminar(self, cliente_id):
        """Elimina un cliente (eliminación lógica)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE clientes SET activo = 0 WHERE id = %s', (cliente_id,))
        conn.commit()
        conn.close()
        return True
    
    def contar_por_estado(self, estado=None):
        """Cuenta clientes por estado (sin usar estado_pago)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM clientes WHERE activo = 1')
        count = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
        conn.close()
        return count
    
    def buscar(self, query):
        """Busca clientes por nombre o DNI"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT c.*, p.nombre as plan_nombre
            FROM clientes c
            LEFT JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.nombre_completo LIKE %s OR c.dni LIKE %s
            ORDER BY c.nombre_completo
        ''', (f'%{query}%', f'%{query}%'))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_historial_pagos(self, cliente_id):
        """Obtiene el historial de pagos de un cliente"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT pa.*, p.nombre as plan_nombre
            FROM pagos pa
            JOIN planes_membresia p ON pa.plan_id = p.id
            WHERE pa.cliente_id = %s
            ORDER BY pa.fecha_pago DESC
        ''', (cliente_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_invitados(self, cliente_id):
        """Obtiene los invitados de un cliente"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM invitados 
            WHERE cliente_titular_id = %s
            ORDER BY fecha_visita DESC, hora_entrada DESC
        ''', (cliente_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def obtener_accesos(self, cliente_id):
        """Obtiene los accesos de un cliente"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM accesos 
            WHERE cliente_id = %s
            ORDER BY fecha_hora_entrada DESC
            LIMIT 50
        ''', (cliente_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def buscar_por_dni(self, dni):
        """Busca un cliente activo por DNI exacto"""
        if not dni:
            return None
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM clientes WHERE dni = %s AND activo = 1', (dni,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def buscar_por_telefono(self, telefono):
        """Busca un cliente activo por teléfono exacto"""
        if not telefono:
            return None
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM clientes WHERE telefono = %s AND activo = 1', (telefono,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    # Nuevos métodos para agregar a cliente_dao.py

    def obtener_estadisticas_dashboard(self):
        """Obtiene estadísticas para el dashboard (pendientes y morosos)"""
        from datetime import datetime
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        hoy = datetime.now().strftime('%Y-%m-%d')
        año_actual = str(datetime.now().year)
        mes_actual = str(datetime.now().month).zfill(2)
        
        # 1. Obtener pagos con estado "pendiente" en la tabla PAGOS
        cursor.execute('''
            SELECT DISTINCT cliente_id, SUM(monto) as total
            FROM pagos 
            WHERE estado = 'pendiente'
            GROUP BY cliente_id
        ''')
        pagos_pendientes = cursor.fetchall()
        
        # Obtener IDs de clientes con pagos pendientes
        clientes_con_pendiente = set([row['cliente_id'] for row in pagos_pendientes])
        total_desde_pagos = sum([row['total'] for row in pagos_pendientes])
        
        # 2. Obtener lista de clientes que han pagado este mes (con estado completado)
        cursor.execute('''
            SELECT DISTINCT cliente_id 
            FROM pagos 
            WHERE estado = 'completado'
            AND YEAR(fecha_pago) = %s
            AND LPAD(MONTH(fecha_pago),2,'0') = %s
        ''', (año_actual, mes_actual))
        
        clientes_pagado = set([row['cliente_id'] for row in cursor.fetchall()])
        
        # 3. Obtener clientes vencidos (fecha_vencimiento < hoy)
        # Excluir planes sin permite_aplazamiento (pago diario al entrar)
        cursor.execute('''
            SELECT c.id
            FROM clientes c
            LEFT JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.activo = 1
            AND c.fecha_vencimiento IS NOT NULL
            AND DATE(c.fecha_vencimiento) < DATE(%s)
            AND (p.permite_aplazamiento IS NULL OR p.permite_aplazamiento = 1)
        ''', (hoy,))
        
        clientes_vencidos = set([row['id'] for row in cursor.fetchall()])
        
        # 4. Obtener todos los clientes activos para calcular los pendientes
        # Excluir planes sin permite_aplazamiento (pago diario al entrar)
        cursor.execute('''
            SELECT c.id, c.plan_id, p.precio
            FROM clientes c
            LEFT JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.activo = 1
            AND (p.permite_aplazamiento IS NULL OR p.permite_aplazamiento = 1)
        ''')
        todos_clientes = cursor.fetchall()
        
        # 5. Calcular pendientes: clientes que no han pagado este mes Y no están vencidos
        clientes_pendientes = []
        total_pendiente = 0
        
        for cliente in todos_clientes:
            cliente_id = cliente['id']
            
            # Si ya tiene un pago pendiente, ya está incluido
            if cliente_id in clientes_con_pendiente:
                clientes_pendientes.append(cliente_id)
                continue
            
            # Si ya pagó este mes, skip
            if cliente_id in clientes_pagado:
                continue
            
            # Si está vencido, NO incluir en pendientes (se cuenta en morosos)
            if cliente_id in clientes_vencidos:
                continue
            
            # Es pendiente: no ha pagado este mes Y no está vencido
            clientes_pendientes.append(cliente_id)
            precio_plan = cliente['precio'] or 0
            total_pendiente += precio_plan
        
        # TOTAL PENDIENTE
        total_pendiente_final = total_desde_pagos + total_pendiente
        clientes_pendientes_count = len(clientes_pendientes)
        
        # 6. Calcular CLIENTES MOROSOS (vencidos)
        clientes_morosos = len(clientes_vencidos)
        if clientes_pagado:
            placeholders = ','.join(['%s'] * len(clientes_pagado))
            query = f'''
                SELECT COUNT(*) as count
                FROM clientes c
                WHERE c.activo = 1
                AND c.fecha_vencimiento IS NOT NULL
                AND DATE(c.fecha_vencimiento) < DATE(%s)
                AND c.id NOT IN ({placeholders})
            '''
            params = [hoy] + list(clientes_pagado)
        else:
            query = '''
                SELECT COUNT(*) as count
                FROM clientes c
                WHERE c.activo = 1
                AND c.fecha_vencimiento IS NOT NULL
                AND DATE(c.fecha_vencimiento) < DATE(%s)
            '''
            params = [hoy]
        
        cursor.execute(query, params)
        result_morosos = cursor.fetchone()
        clientes_morosos = result_morosos['count'] if result_morosos else 0
        
        conn.close()
        
        return {
            'total_pendiente': total_pendiente_final,
            'clientes_morosos': clientes_morosos,
            'clientes_pendientes': len(clientes_pendientes),
            'clientes_pagado_ids': list(clientes_pagado)
        }

    def obtener_pagos_por_cliente(self, cliente_id, solo_completados=True):
        """Obtiene pagos de un cliente específico"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if solo_completados:
            cursor.execute('''
                SELECT id, monto, metodo_pago, fecha_pago, estado
                FROM pagos 
                WHERE cliente_id = %s AND estado = 'completado'
                ORDER BY fecha_pago DESC
            ''', (cliente_id,))
        else:
            cursor.execute('''
                SELECT id, monto, metodo_pago, fecha_pago, estado
                FROM pagos 
                WHERE cliente_id = %s
                ORDER BY fecha_pago DESC
            ''', (cliente_id,))
        
        pagos = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in pagos]

    def verificar_estado_pago_actual(self, cliente_id):
        """Verifica el estado de pago actual del cliente"""
        from datetime import datetime
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        fecha_actual = datetime.now()
        
        # Verificar pagos pendientes
        cursor.execute('''
            SELECT id, monto, fecha_pago, estado
            FROM pagos
            WHERE cliente_id = %s
            AND estado = 'pendiente'
            ORDER BY fecha_pago DESC
            LIMIT 1
        ''', (cliente_id,))
        
        pago_pendiente = cursor.fetchone()
        
        if pago_pendiente:
            conn.close()
            return {
                'ha_pagado': False,
                'tiene_pendiente': True,
                'dias_mora': 0,
                'estado': 'pendiente'
            }
        
        # Verificar pago completado este mes
        cursor.execute('''
            SELECT id, monto, fecha_pago, estado
            FROM pagos
            WHERE cliente_id = %s
            AND estado = 'completado'
            AND YEAR(fecha_pago) = %s
            AND LPAD(MONTH(fecha_pago),2,'0') = %s
            ORDER BY fecha_pago DESC
            LIMIT 1
        ''', (cliente_id, str(fecha_actual.year), str(fecha_actual.month).zfill(2)))
        
        pago_completado = cursor.fetchone()
        
        # Obtener fecha de vencimiento
        cursor.execute('''
            SELECT fecha_vencimiento
            FROM clientes
            WHERE id = %s
        ''', (cliente_id,))
        
        cliente = cursor.fetchone()
        conn.close()
        
        ha_pagado = pago_completado is not None
        
        if ha_pagado:
            return {
                'ha_pagado': True,
                'tiene_pendiente': False,
                'dias_mora': 0,
                'estado': 'pagado'
            }
        
        # Verificar vencimiento
        if cliente and cliente['fecha_vencimiento']:
            try:
                fecha_venc_str = cliente['fecha_vencimiento']
                if ' ' in fecha_venc_str:
                    fecha_venc = datetime.strptime(fecha_venc_str, '%Y-%m-%d %H:%M:%S')
                else:
                    fecha_venc = datetime.strptime(fecha_venc_str, '%Y-%m-%d')
                
                dias_mora = (fecha_actual.date() - fecha_venc.date()).days
                
                if dias_mora > 0:
                    return {
                        'ha_pagado': False,
                        'tiene_pendiente': False,
                        'dias_mora': dias_mora,
                        'estado': 'vencido'
                    }
            except:
                pass
        
        return {
            'ha_pagado': False,
            'tiene_pendiente': False,
            'dias_mora': 0,
            'estado': 'pendiente'
        }

    def verificar_pagos_pendientes(self, cliente_id):
        """Verifica si el cliente tiene pagos pendientes"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) as cantidad, COALESCE(SUM(monto), 0) as monto_total 
            FROM pagos 
            WHERE cliente_id = %s AND estado = 'pendiente'
        ''', (cliente_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        cantidad = row['cantidad'] if row else 0
        monto_total = float(row['monto_total']) if row else 0.0
        
        return {
            'tiene_pendiente': cantidad > 0,
            'cantidad': cantidad,
            'monto_total': monto_total
        }

    def obtener_estadisticas_pagos(self):
        """Obtiene estadísticas de pagos (pendientes y vencidos)"""
        from datetime import datetime

        conn = self._get_connection()
        cursor = conn.cursor()

        hoy = datetime.now().strftime('%Y-%m-%d')
        anio_actual = datetime.now().year
        mes_actual  = datetime.now().month

        try:
            # 1. Clientes que ya pagaron este mes
            cursor.execute('''
                SELECT DISTINCT cliente_id
                FROM pagos
                WHERE estado = 'completado'
                AND YEAR(fecha_pago)  = %s
                AND MONTH(fecha_pago) = %s
            ''', (anio_actual, mes_actual))
            clientes_pagado = set(row['cliente_id'] for row in cursor.fetchall())

            # 2. Clientes vencidos (solo planes con permite_aplazamiento)
            cursor.execute('''
                SELECT c.id
                FROM clientes c
                JOIN planes_membresia p ON c.plan_id = p.id
                WHERE c.activo = 1
                AND c.fecha_vencimiento IS NOT NULL
                AND DATE(c.fecha_vencimiento) < DATE(%s)
                AND (p.permite_aplazamiento IS NULL OR p.permite_aplazamiento = 1)
            ''', (hoy,))
            clientes_vencidos = set(row['id'] for row in cursor.fetchall())

            # 3. Todos los clientes activos con planes que permiten aplazamiento
            cursor.execute('''
                SELECT c.id, COALESCE(p.precio, 0) as precio
                FROM clientes c
                JOIN planes_membresia p ON c.plan_id = p.id
                WHERE c.activo = 1
                AND (p.permite_aplazamiento IS NULL OR p.permite_aplazamiento = 1)
            ''')
            todos_clientes = cursor.fetchall()

            # 4. Calcular pendientes y vencidos
            clientes_pendientes = []
            total_pendiente = 0.0
            total_vencido   = 0.0

            for cliente in todos_clientes:
                cid    = cliente['id']
                precio = float(cliente['precio'] or 0)

                if cid in clientes_pagado:
                    continue  # ya pagó, no cuenta

                if cid in clientes_vencidos:
                    total_vencido += precio
                    continue  # vencido, no es pendiente

                # pendiente: activo, no pagó, no vencido
                clientes_pendientes.append(cid)
                total_pendiente += precio

            return {
                'total_pendiente':    total_pendiente,
                'total_vencido':      total_vencido,
                'clientes_pendientes': len(clientes_pendientes),
                'clientes_vencidos':  len(clientes_vencidos),
                'clientes_pagado':    len(clientes_pagado)
            }

        finally:
            conn.close()

    def registrar_pago_cliente(self, cliente_id, metodo_pago='efectivo', usuario_id=None, monto_override=None):
        """Registra un pago para un cliente"""
        from datetime import datetime
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Obtener información del cliente
        cliente = self.obtener_por_id(cliente_id)
        if not cliente:
            conn.close()
            return {'success': False, 'message': 'Cliente no encontrado'}
        
        # Determinar el plan_id
        plan_id = cliente.get('plan_id')
        plan = None
        
        if plan_id:
            cursor.execute('SELECT id, precio FROM planes_membresia WHERE id = %s', (plan_id,))
            plan = cursor.fetchone()
        
        if not plan:
            conn.close()
            return {'success': False, 'message': 'Plan no encontrado'}
        
        plan_id_final = plan['id']

        # Monto: usa lo que manda el frontend (ya tiene promo), o calcula con PromocionDAO, o precio base
        if monto_override is not None:
            monto = float(monto_override)
        else:
            monto = float(plan['precio'])
            if PromocionDAO is not None:
                try:
                    promo_dao = PromocionDAO()
                    precio_final, _, _ = promo_dao.calcular_precio_con_descuento(
                        plan_id_final, monto,
                        sexo_cliente=cliente.get('sexo', 'no_especificado'),
                        turno_cliente=cliente.get('turno', None)
                    )
                    monto = float(precio_final)
                except Exception:
                    pass
        
        # Verificar si hay pago pendiente
        cursor.execute('''
            SELECT id FROM pagos 
            WHERE cliente_id = %s AND estado = 'pendiente'
            ORDER BY id DESC LIMIT 1
        ''', (cliente_id,))
        pago_pendiente = cursor.fetchone()
        
        resultado = {}
        
        # Obtener info del plan para calcular fechas del historial
        cursor.execute('SELECT duracion FROM planes_membresia WHERE id = %s', (plan_id_final,))
        plan_duracion_row = cursor.fetchone()
        plan_duracion_str = plan_duracion_row['duracion'] if plan_duracion_row else '1 mes'

        # Calcular fecha_inicio y fecha_fin para historial_membresia
        telefono_cliente = cliente.get('telefono', '')
        from datetime import datetime as _dt
        fecha_inicio_hist = _dt.now().strftime('%Y-%m-%d')
        # Reusar _calcular_fecha_vencimiento para obtener la fecha fin correcta
        _duracion_dict = self._parsear_duracion(plan_duracion_str)
        fecha_fin_hist_db, _, _, _ = self._calcular_fecha_vencimiento(
            telefono_cliente, duracion=_duracion_dict
        )
        fecha_fin_hist = fecha_fin_hist_db.split(' ')[0]  # Solo la parte de fecha

        if pago_pendiente:
            # Marcar pendiente como completado, actualizando también el monto con la promo vigente
            fecha_pago = get_current_timestamp_peru_value()

            cursor.execute('''
                UPDATE pagos 
                SET estado = 'completado', 
                    metodo_pago = %s,
                    fecha_pago = %s,
                    monto = %s
                WHERE id = %s
            ''', (metodo_pago, fecha_pago, monto, pago_pendiente['id']))

            # Actualizar historial_membresia con el monto correcto (con promoción aplicada)
            cursor.execute('''
                UPDATE historial_membresia
                SET estado = 'activa',
                    metodo_pago = %s,
                    monto_pagado = %s,
                    fecha_inicio = %s,
                    fecha_fin = %s
                WHERE cliente_id = %s
                AND estado = 'pendiente'
                AND id = (
                    SELECT id FROM (
                        SELECT id FROM historial_membresia
                        WHERE cliente_id = %s AND estado = 'pendiente'
                        ORDER BY fecha_registro DESC
                        LIMIT 1
                    ) AS tmp
                )
            ''', (metodo_pago, monto, fecha_inicio_hist, fecha_fin_hist, cliente_id, cliente_id))

            resultado = {
                'success': True,
                'message': 'Pago pendiente marcado como completado',
                'pago_id': pago_pendiente['id']
            }
        else:
            # Crear nuevo pago completado
            fecha_pago = get_current_timestamp_peru_value()

            cursor.execute('''
                INSERT INTO pagos (cliente_id, plan_id, monto, metodo_pago, 
                                usuario_registro, estado, fecha_pago)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                cliente_id,
                plan_id_final,
                monto,
                metodo_pago,
                usuario_id or 1,
                'completado',
                fecha_pago
            ))
            pago_id = cursor.lastrowid

            # Insertar en historial_membresia con el monto real pagado (con promoción)
            cursor.execute('''
                INSERT INTO historial_membresia
                    (cliente_id, plan_id, fecha_inicio, fecha_fin, monto_pagado,
                     metodo_pago, estado, usuario_id, fecha_registro)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                cliente_id,
                plan_id_final,
                fecha_inicio_hist,
                fecha_fin_hist,
                monto,
                metodo_pago,
                'activa',
                usuario_id or 1,
                fecha_pago
            ))

            resultado = {
                'success': True,
                'message': 'Pago registrado correctamente',
                'pago_id': pago_id
            }
        
        conn.commit()
        conn.close()
        return resultado

    def obtener_clientes_para_pagos_optimizado(self, filtro='todos'):
        """
        Obtiene clientes con estado de pago, datos del plan y días de mora
        en UNA SOLA QUERY SQL, eliminando los N+1 requests del frontend.
        
        Reemplaza a obtener_clientes_con_estado_pago() para la vista de pagos.
        """
        from datetime import datetime

        hoy = datetime.now().strftime('%Y-%m-%d')
        anio_actual = str(datetime.now().year)
        mes_actual = str(datetime.now().month).zfill(2)

        conn = self._get_connection()
        cursor = conn.cursor()

        # Consulta corregida - eliminando CONVERT_TZ problemático
        query = '''
            SELECT
                c.*,
                p.nombre          AS plan_nombre,
                p.codigo          AS plan_codigo,
                p.precio          AS plan_precio,
                p.duracion        AS plan_duracion,
                p.qr_habilitado,
                p.permite_aplazamiento,
                p.permite_invitados,
                p.envia_whatsapp,

                /* ¿Ha pagado este mes? */
                CASE WHEN EXISTS (
                    SELECT 1 FROM pagos pa
                    WHERE pa.cliente_id = c.id
                    AND pa.estado = 'completado'
                    AND YEAR(pa.fecha_pago) = %s
                    AND MONTH(pa.fecha_pago) = %s
                ) THEN 1 ELSE 0 END AS ha_pagado,

                /* ¿Tiene pago pendiente? */
                CASE WHEN EXISTS (
                    SELECT 1 FROM pagos pa
                    WHERE pa.cliente_id = c.id
                    AND pa.estado = 'pendiente'
                ) THEN 1 ELSE 0 END AS tiene_pendiente,

                /* Días de mora: positivo = vencido, negativo = días restantes */
                DATEDIFF(%s, DATE(c.fecha_vencimiento)) AS dias_mora,

                /* Estado calculado en SQL */
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM pagos pa
                        WHERE pa.cliente_id = c.id
                        AND pa.estado = 'pendiente'
                    ) THEN 'pendiente'
                    WHEN EXISTS (
                        SELECT 1 FROM pagos pa
                        WHERE pa.cliente_id = c.id
                        AND pa.estado = 'completado'
                        AND YEAR(pa.fecha_pago) = %s
                        AND MONTH(pa.fecha_pago) = %s
                    ) THEN 'pagado'
                    WHEN c.fecha_vencimiento IS NOT NULL
                        AND DATE(c.fecha_vencimiento) < DATE(%s) THEN 'vencido'
                    ELSE 'pendiente'
                END AS estado_pago

            FROM clientes c
            LEFT JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.activo = 1
              AND (p.permite_aplazamiento IS NULL OR p.permite_aplazamiento = 1)
            ORDER BY c.fecha_inicio DESC
        '''
        
        cursor.execute(query, (
            anio_actual, mes_actual,  # Para ha_pagado
            hoy,                       # Para dias_mora
            anio_actual, mes_actual,  # Para estado_pago - pagado
            hoy                        # Para estado_pago - vencido
        ))

        rows = cursor.fetchall()
        conn.close()

        clientes = [dict(row) for row in rows]

        # Calcular precios con descuento (una sola pasada en Python)
        if PromocionDAO:
            promocion_dao = PromocionDAO()
            for cliente in clientes:
                plan_id       = cliente.get('plan_id')
                precio_orig   = float(cliente.get('plan_precio', 0) or 0)
                sexo_cliente  = cliente.get('sexo', None)
                turno_cliente = cliente.get('turno', None)
                precio_desc, descuento, promocion = promocion_dao.calcular_precio_con_descuento(
                    plan_id, precio_orig, sexo_cliente, turno_cliente
                )
                cliente['plan_precio_original']  = precio_orig
                cliente['plan_precio_descuento'] = precio_desc
                cliente['plan_descuento']        = descuento
                cliente['plan_promocion']        = promocion
                cliente['tiene_promocion']       = True if promocion else False

        # Aplicar filtro en Python (ya tenemos todo en memoria)
        if filtro == 'todos':
            return clientes

        resultado = []
        for c in clientes:
            ha_pagado      = bool(c.get('ha_pagado'))
            tiene_pendiente = bool(c.get('tiene_pendiente'))
            estado         = c.get('estado_pago', 'pendiente')

            if filtro == 'pagado' and ha_pagado and not tiene_pendiente:
                resultado.append(c)
            elif filtro == 'pendiente' and (tiene_pendiente or (not ha_pagado and estado != 'vencido')):
                resultado.append(c)
            elif filtro == 'vencido' and estado == 'vencido' and not ha_pagado:
                resultado.append(c)

        return resultado

    def obtener_clientes_con_estado_pago(self, filtro='todos'):
        """Obtiene clientes con información de estado de pago"""
        from datetime import datetime
        
        clientes = self.obtener_todos()

        # Excluir clientes cuyos planes NO permiten aplazamiento
        # (son planes de pago diario — se cobran en el acceso, no en pagos)
        clientes = [c for c in clientes if c.get('permite_aplazamiento') != 0]
        
        if filtro == 'todos':
            return clientes
        
        hoy = datetime.now().strftime('%Y-%m-%d')
        año_actual = str(datetime.now().year)
        mes_actual = str(datetime.now().month).zfill(2)
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Clientes que han pagado este mes (estado completado)
        cursor.execute('''
            SELECT DISTINCT cliente_id 
            FROM pagos 
            WHERE estado = 'completado'
            AND YEAR(fecha_pago) = %s
            AND LPAD(MONTH(fecha_pago),2,'0') = %s
        ''', (año_actual, mes_actual))
        
        clientes_pagado = set([row['cliente_id'] for row in cursor.fetchall()])
        
        # Clientes con pagos pendientes (estado pendiente en tabla PAGOS)
        cursor.execute('''
            SELECT DISTINCT cliente_id 
            FROM pagos 
            WHERE estado = 'pendiente'
        ''')
        
        clientes_con_pendiente = set([row['cliente_id'] for row in cursor.fetchall()])
        
        # Clientes vencidos
        cursor.execute('''
            SELECT id FROM clientes 
            WHERE activo = 1
            AND fecha_vencimiento IS NOT NULL
            AND DATE(fecha_vencimiento) < DATE(%s)
        ''', (hoy,))
        
        clientes_vencidos = set([row['id'] for row in cursor.fetchall()])
        conn.close()
        
        # Filtrar con lógica exclusiva (un cliente solo puede estar en una categoría)
        clientes_filtrados = []
        for cliente in clientes:
            cliente_id = cliente['id']
            ha_pagado = cliente_id in clientes_pagado
            tiene_pendiente = cliente_id in clientes_con_pendiente
            esta_vencido = cliente_id in clientes_vencidos
            
            if filtro == 'pagado':
                # Solo los que han pagado Y NO tienen pagos pendientes
                if ha_pagado and not tiene_pendiente:
                    clientes_filtrados.append(cliente)
                    
            elif filtro == 'pendiente':
                # PRIORIDAD 1: Tiene pago pendiente (de aumento de meses u otro)
                # PRIORIDAD 2: No ha pagado este mes Y no está vencido
                if tiene_pendiente:
                    clientes_filtrados.append(cliente)
                elif not ha_pagado and not esta_vencido:
                    clientes_filtrados.append(cliente)
                    
            elif filtro == 'vencido':
                # Solo los que están vencidos Y NO han pagado este mes
                if esta_vencido and not ha_pagado:
                    clientes_filtrados.append(cliente)
        
        return clientes_filtrados

    def verificar_acceso_hoy(self, cliente_id, fecha_param=None):
        """Verifica si un cliente ya accedió hoy"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if fecha_param:
            cursor.execute('''
                SELECT id FROM accesos 
                WHERE cliente_id = %s 
                AND DATE(fecha_hora_entrada) = DATE(%s)
                AND (tipo = 'cliente' OR tipo IS NULL)
                LIMIT 1
            ''', (cliente_id, fecha_param))
        else:
            cursor.execute(f'''
                SELECT id FROM accesos 
                WHERE cliente_id = %s 
                AND DATE(fecha_hora_entrada) = {get_current_date_expression()}
                AND (tipo = 'cliente' OR tipo IS NULL)
                LIMIT 1
            ''', (cliente_id,))
        
        acceso = cursor.fetchone()
        conn.close()
        
        return acceso is not None

    def verificar_membresia_vencida(self, cliente_id):
        """Verifica si la membresía de un cliente está vencida"""
        from datetime import datetime
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT c.fecha_vencimiento, p.qr_habilitado, p.nombre as plan_nombre
            FROM clientes c
            JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.id = %s
        ''', (cliente_id,))
        
        cliente_info = cursor.fetchone()
        conn.close()
        
        if not cliente_info:
            return {'vencida': False, 'plan_nombre': ''}
        
        # Solo verificar si el plan tiene QR habilitado
        if cliente_info['qr_habilitado'] == 1:
            fecha_vencimiento = cliente_info['fecha_vencimiento']
            if fecha_vencimiento:
                hoy = datetime.now()
                try:
                    if ' ' in fecha_vencimiento:
                        fecha_venc = datetime.strptime(fecha_vencimiento, '%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_venc = datetime.strptime(fecha_vencimiento, '%Y-%m-%d')
                    
                    dias_restantes = (fecha_venc.date() - hoy.date()).days
                    vencida = dias_restantes <= 0
                    return {
                        'vencida': vencida,
                        'plan_nombre': cliente_info['plan_nombre'],
                        'dias_restantes': dias_restantes
                    }
                except:
                    pass
        
        return {'vencida': False, 'plan_nombre': cliente_info['plan_nombre']}
    

    def obtener_membresias_por_vencer(self, dias=7):
        """Obtiene membresías que vencen en los próximos días"""
        
        conn = self._get_connection()
        cursor = conn.cursor()

        hoy = datetime.now().strftime('%Y-%m-%d')
        fecha_fin = (datetime.now() + timedelta(days=dias)).strftime('%Y-%m-%d')

        cursor.execute('''
            SELECT 
                c.id as cliente_id,
                c.nombre_completo,
                p.nombre as plan_nombre,
                c.fecha_vencimiento,
                c.dni,
                c.telefono,
                CASE 
                    WHEN DATE(c.fecha_vencimiento) = DATE(%s) THEN 'Hoy'
                    WHEN DATE(c.fecha_vencimiento) = DATE(%s, '+1 day') THEN 'Mañana'
                    ELSE DATE(c.fecha_vencimiento)
                END as vence_en
            FROM clientes c
            JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.activo = 1
            AND c.fecha_vencimiento IS NOT NULL
            AND DATE(c.fecha_vencimiento) BETWEEN DATE(%s) AND DATE(%s)
            ORDER BY DATE(c.fecha_vencimiento) ASC
        ''', (hoy, hoy, hoy, fecha_fin))

        membresias = cursor.fetchall()
        conn.close()

        return [
            {
                'cliente_id': row['cliente_id'],
                'nombre_completo': row['nombre_completo'],
                'plan': row['plan_nombre'],
                'fecha_vencimiento': row['fecha_vencimiento'],
                'vence_en': row['vence_en'],
                'dni': row['dni'],
                'telefono': row['telefono'],
            } for row in membresias
        ]

    def obtener_clientes_por_plan(self):
        """Obtiene distribución de clientes activos por plan"""

        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT 
                p.nombre as plan_nombre,
                p.codigo,
                COUNT(c.id) as cantidad_clientes,
                COUNT(c.id) * 100.0 / 
                    (SELECT COUNT(*) FROM clientes WHERE activo = 1) as porcentaje
            FROM clientes c
            JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.activo = 1
            GROUP BY p.id
            ORDER BY cantidad_clientes DESC
        ''')

        planes_data = cursor.fetchall()
        conn.close()

        labels = []
        data = []

        for row in planes_data:
            labels.append(row['plan_nombre'])
            data.append(row['cantidad_clientes'])

        return {
            'labels': labels,
            'data': data,
            'detalle': [
                {
                    'plan': row['plan_nombre'],
                    'codigo': row['codigo'],
                    'clientes': row['cantidad_clientes'],
                    'porcentaje': round(row['porcentaje'], 1)
                } for row in planes_data
            ]
        }