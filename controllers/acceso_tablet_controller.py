"""
Módulo de control de acceso para tablets
Extiende el sistema existente con funcionalidad de escaneo QR para tablets
"""

from flask import request, jsonify, session
from datetime import datetime
import db_helper
import json

def init_acceso_tablet_controller(app):
    """Inicializa los endpoints para el control de acceso vía tablet"""
    
    @app.route('/acceso-tablet')
    def acceso_tablet():
        """Vista simplificada para tablets - clientes escanean su QR"""
        # Esta vista no requiere login ya que estará en la tablet de acceso público
        # Pero verificamos que el sistema esté configurado
        from app import verificar_configuracion_inicial
        if not verificar_configuracion_inicial():
            return "Sistema no configurado", 503
        return render_template_acceso_tablet()
    
    @app.route('/api/qr-escaneado', methods=['POST'])
    def api_qr_escaneado():
        """
        Endpoint para recibir QR o DNI escaneados desde la tablet.
        Guarda el acceso pendiente para que la computadora lo apruebe.
        
        Parámetros:
        - qr_code: El código QR escaneado o el número de DNI
        - tipo: 'qr' o 'dni' para indicar el tipo de dato
        - tablet_id: Identificador de la tablet
        """
        try:
            data = request.get_json()
            qr_code = data.get('qr_code', '').strip()
            tipo = data.get('tipo', 'qr')  # 'qr' o 'dni'
            tablet_id = data.get('tablet_id', 'default')
            
            if not qr_code:
                return jsonify({
                    'success': False,
                    'message': 'Código o DNI vacío'
                }), 400
            
            # Variable para almacenar el origen del dato
            origen_dato = tipo
            
            if tipo == 'dni':
                # Es un DNI directo, validar formato (8 dígitos)
                import re
                if not re.match(r'^\d{8}$', qr_code):
                    return jsonify({
                        'success': False,
                        'message': 'DNI no válido - debe tener 8 dígitos'
                    }), 400
                dni = qr_code
            else:
                # Es un código QR, extraer el DNI
                dni = extraer_dni_de_qr(qr_code)
                if not dni:
                    return jsonify({
                        'success': False,
                        'message': 'QR no válido - no contiene DNI'
                    }), 400
            
            # Buscar cliente por DNI
            cliente = buscar_cliente_por_dni(dni)
            if not cliente:
                return jsonify({
                    'success': False,
                    'message': 'Cliente no encontrado con DNI: ' + dni
                }), 404
            
            # Verificar si tiene membresía activa
            tiene_acceso, mensaje, datos_cliente = verificar_acceso_cliente(cliente)
            
            # Crear registro de acceso pendiente
            conn = db_helper.get_db_connection()
            cursor = conn.cursor()
            
            fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Insertar acceso pendiente (agregar tipo_origen)
            cursor.execute('''
                INSERT INTO accesos_pendientes (
                    cliente_id, dni, qr_code, tablet_id, 
                    fecha_creacion, estado, datos_cliente, tipo_origen
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                cliente['id'] if 'id' in cliente else None,
                dni,
                qr_code,
                tablet_id,
                fecha_actual,
                'pendiente',
                json.dumps(datos_cliente) if datos_cliente else None,
                origen_dato
            ))
            
            conn.commit()
            acceso_id = cursor.lastrowid
            conn.close()
            
            return jsonify({
                'success': True,
                'message': mensaje,
                'acceso_id': acceso_id,
                'cliente': datos_cliente,
                'tiene_acceso': tiene_acceso,
                'tipo': origen_dato
            })
            
        except Exception as e:
            print(f"Error en api_qr_escaneado: {e}")
            return jsonify({
                'success': False,
                'message': f'Error del servidor: {str(e)}'
            }), 500
    
    @app.route('/api/accesos-pendientes', methods=['GET'])
    @app.route('/api/accesos-pendientes/<tablet_id>', methods=['GET'])
    def api_accesos_pendientes(tablet_id=None):
        """
        Obtiene los accesos pendientes para la computadora receptora.
        La computadora hace polling para verificar nuevos accesos.
        """
        try:
            if tablet_id:
                # Solo accesos de una tablet específica
                query = '''
                    SELECT * FROM accesos_pendientes 
                    WHERE estado = 'pendiente' 
                    AND tablet_id = %s
                    ORDER BY fecha_creacion DESC
                    LIMIT 10
                '''
                conn = db_helper.get_db_connection()
                cursor = conn.cursor()
                cursor.execute(query, (tablet_id,))
            else:
                # Todos los accesos pendientes (para la computadora)
                query = '''
                    SELECT * FROM accesos_pendientes 
                    WHERE estado = 'pendiente' 
                    ORDER BY fecha_creacion DESC
                    LIMIT 10
                '''
                conn = db_helper.get_db_connection()
                cursor = conn.cursor()
                cursor.execute(query)
            
            rows = cursor.fetchall()
            conn.close()
            
            accesos = []
            for row in rows:
                if isinstance(row, dict):
                    acceso = dict(row)
                else:
                    # Convertir tuple a dict si es necesario
                    acceso = {
                        'id': row[0] if len(row) > 0 else None,
                        'cliente_id': row[1] if len(row) > 1 else None,
                        'dni': row[2] if len(row) > 2 else None,
                        'qr_code': row[3] if len(row) > 3 else None,
                        'tablet_id': row[4] if len(row) > 4 else None,
                        'fecha_creacion': row[5] if len(row) > 5 else None,
                        'estado': row[6] if len(row) > 6 else None,
                        'datos_cliente': row[7] if len(row) > 7 else None
                    }
                
                # Parsear datos del cliente si están en JSON
                if acceso.get('datos_cliente'):
                    try:
                        acceso['datos_cliente'] = json.loads(acceso['datos_cliente'])
                    except:
                        pass
                
                accesos.append(acceso)
            
            return jsonify({
                'success': True,
                'accesos': accesos,
                'count': len(accesos)
            })
            
        except Exception as e:
            print(f"Error en api_accesos_pendientes: {e}")
            return jsonify({
                'success': False,
                'message': str(e),
                'accesos': []
            }), 500
    
    @app.route('/api/acceso-aprobar/<int:acceso_id>', methods=['POST'])
    def api_acceso_aprobar(acceso_id):
        """
        Aprueba un acceso pendiente desde la computadora.
        """
        try:
            conn = db_helper.get_db_connection()
            cursor = conn.cursor()
            
            # Obtener datos del acceso pendiente
            cursor.execute('''
                SELECT * FROM accesos_pendientes WHERE id = %s AND estado = 'pendiente'
            ''', (acceso_id,))
            
            acceso_pendiente = cursor.fetchone()
            
            if not acceso_pendiente:
                conn.close()
                return jsonify({
                    'success': False,
                    'message': 'Acceso no encontrado o ya procesado'
                }), 404
            
            # Parsear datos
            if isinstance(acceso_pendiente, dict):
                datos = acceso_pendiente
            else:
                datos = {
                    'id': acceso_pendiente[0],
                    'cliente_id': acceso_pendiente[1],
                    'dni': acceso_pendiente[2],
                    'qr_code': acceso_pendiente[3],
                    'tablet_id': acceso_pendiente[4],
                    'datos_cliente': acceso_pendiente[7]
                }
            
            # Actualizar estado a aprobado
            cursor.execute('''
                UPDATE accesos_pendientes 
                SET estado = 'aprobado', fecha_aprobacion = %s
                WHERE id = %s
            ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), acceso_id))
            
            conn.commit()
            conn.close()
            
            # El acceso ya fue registrado cuando se creó el pendiente
            # La computadora recibe la notificación y muestra el modal
            
            return jsonify({
                'success': True,
                'message': 'Acceso aprobado'
            })
            
        except Exception as e:
            print(f"Error en api_acceso_aprobar: {e}")
            return jsonify({
                'success': False,
                'message': str(e)
            }), 500
    
    @app.route('/api/acceso-rechazar/<int:acceso_id>', methods=['POST'])
    def api_acceso_rechazar(acceso_id):
        """
        Rechaza un acceso pendiente desde la computadora.
        """
        try:
            conn = db_helper.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE accesos_pendientes 
                SET estado = 'rechazado', fecha_aprobacion = %s
                WHERE id = %s
            ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), acceso_id))
            
            conn.commit()
            conn.close()
            
            return jsonify({
                'success': True,
                'message': 'Acceso rechazado'
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'message': str(e)
            }), 500
    
    @app.route('/api/acceso-pendiente/<int:acceso_id>', methods=['GET'])
    def api_acceso_pendiente_detalle(acceso_id):
        """
        Obtiene detalles de un acceso pendiente específico.
        """
        try:
            conn = db_helper.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM accesos_pendientes WHERE id = %s
            ''', (acceso_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                return jsonify({
                    'success': False,
                    'message': 'Acceso no encontrado'
                }), 404
            
            acceso = dict(row) if isinstance(row, dict) else {
                'id': row[0],
                'cliente_id': row[1],
                'dni': row[2],
                'qr_code': row[3],
                'tablet_id': row[4],
                'fecha_creacion': row[5],
                'estado': row[6],
                'datos_cliente': row[7]
            }
            
            if acceso.get('datos_cliente'):
                try:
                    acceso['datos_cliente'] = json.loads(acceso['datos_cliente'])
                except:
                    pass
            
            return jsonify({
                'success': True,
                'acceso': acceso
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'message': str(e)
            }), 500


def extraer_dni_de_qr(qr_code):
    """
    Extrae el DNI del código QR.
    Soporta múltiples formatos de QR.
    """
    import re
    
    # Intentar extraer 8 dígitos consecutivos (DNI simple)
    dni_match = re.search(r'(\d{8})', qr_code)
    if dni_match:
        return dni_match.group(1)
    
    # Intentar formato ADV-6013
    dni_adv = re.search(r'\[dni\[Ñ\s*\[(\d{8})', qr_code, re.IGNORECASE)
    if dni_adv:
        return dni_adv.group(1)
    
    # Intentar JSON
    try:
        json_match = re.search(r'\{.*\}', qr_code)
        if json_match:
            data = json.loads(json_match.group(0))
            if 'dni' in data:
                return str(data['dni'])
    except:
        pass
    
    return None


def buscar_cliente_por_dni(dni):
    """
    Busca un cliente por su DNI.
    """
    try:
        conn = db_helper.get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT c.*, p.nombre as plan_nombre, p.precio,
                   DATEDIFF(c.fecha_vencimiento, CURDATE()) as dias_restantes
            FROM clientes c
            LEFT JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.dni = %s
        ''', (dni,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        # Convertir a dict
        if isinstance(row, dict):
            return row
        
        columns = ['id', 'dni', 'nombre_completo', 'telefono', 'email', 
                   'plan_id', 'fecha_inicio', 'fecha_vencimiento', 'estado',
                   'qr_code', 'foto', 'observaciones', 'fecha_registro',
                   'usuario_creador_id', 'plan_nombre', 'precio', 'dias_restantes']
        
        cliente = {}
        for i, col in enumerate(columns):
            if i < len(row):
                cliente[col] = row[i]
        
        return cliente
        
    except Exception as e:
        print(f"Error buscando cliente: {e}")
        return None


def verificar_acceso_cliente(cliente):
    """
    Verifica si un cliente tiene acceso válido.
    Retorna: (tiene_acceso, mensaje, datos_cliente)
    Incluye todas las validaciones: estado, vencimiento, y límite semanal
    """
    from datetime import datetime
    
    if not cliente:
        return False, "Cliente no encontrado", None
    
    # Verificar estado del cliente
    if cliente.get('estado') != 'activo':
        return False, "Cliente inactivo", formatear_datos_cliente(cliente)
    
    # Verificar fecha de vencimiento
    fecha_venc = cliente.get('fecha_vencimiento')
    if not fecha_venc:
        return False, "Sin membresía", formatear_datos_cliente(cliente)
    
    try:
        # Parsear fecha de vencimiento
        if isinstance(fecha_venc, str):
            from datetime import datetime
            fecha_venc = datetime.strptime(fecha_venc, '%Y-%m-%d')
        
        hoy = datetime.now().date()
        dias_restantes = (fecha_venc.date() - hoy).days if hasattr(fecha_venc, 'date') else 0
        
        if dias_restantes < 0:
            return False, f"Membresía vencida hace {abs(dias_restantes)} días", formatear_datos_cliente(cliente)
        elif dias_restantes == 0:
            # Verificar límite semanal antes de permitir acceso que vence hoy
            limite_info = verificar_limite_semanal(cliente.get('id'))
            if not limite_info['puede_acceder']:
                return False, limite_info['mensaje'], formatear_datos_cliente(cliente)
            return True, "Membresía vence hoy", formatear_datos_cliente(cliente)
        elif dias_restantes <= 7:
            # Verificar límite semanal
            limite_info = verificar_limite_semanal(cliente.get('id'))
            if not limite_info['puede_acceder']:
                return False, limite_info['mensaje'], formatear_datos_cliente(cliente)
            return True, f"Membresía por vencer ({dias_restantes} días)", formatear_datos_cliente(cliente)
        else:
            # Verificar límite semanal para membresías con muchos días
            limite_info = verificar_limite_semanal(cliente.get('id'))
            if not limite_info['puede_acceder']:
                return False, limite_info['mensaje'], formatear_datos_cliente(cliente)
            return True, f"Membresía activa ({dias_restantes} días restantes)", formatear_datos_cliente(cliente)
            
    except Exception as e:
        print(f"Error verificando acceso: {e}")
        return True, "Acceso permitido", formatear_datos_cliente(cliente)


def verificar_limite_semanal(cliente_id):
    """
    Verifica si el cliente ha alcanzado su límite semanal de accesos.
    Retorna: dict con 'puede_acceder', 'dias_unicos', 'limite', 'mensaje'
    """
    try:
        if not cliente_id:
            return {'puede_acceder': True, 'dias_unicos': 0, 'limite': 7, 'mensaje': ''}
        
        # Obtener el plan del cliente para saber su límite semanal
        conn = db_helper.get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT p.limite_semanal 
            FROM clientes c
            LEFT JOIN planes_membresia p ON c.plan_id = p.id
            WHERE c.id = %s
        ''', (cliente_id,))
        
        plan_row = cursor.fetchone()
        conn.close()
        
        if not plan_row:
            return {'puede_acceder': True, 'dias_unicos': 0, 'limite': 7, 'mensaje': ''}
        
        # Obtener el límite semanal del plan
        if isinstance(plan_row, dict):
            limite_semanal_raw = plan_row.get('limite_semanal')
        else:
            limite_semanal_raw = plan_row[0] if plan_row else None
        
        # NULL en BD = sin límite (equivale a 7 = todos los días)
        if limite_semanal_raw is None:
            limite_semanal = 7
        else:
            try:
                limite_semanal = int(limite_semanal_raw)
            except (ValueError, TypeError):
                limite_semanal = 7
        
        # Si el límite es 7 o más, significa "todos los días" (sin restricción)
        if limite_semanal >= 7:
            return {'puede_acceder': True, 'dias_unicos': 0, 'limite': limite_semanal, 'mensaje': ''}
        
        # Contar días únicos de acceso esta semana (lunes a domingo)
        conn = db_helper.get_db_connection()
        cursor = conn.cursor()
        
        # Obtener inicio de semana (lunes)
        cursor.execute('''
            SELECT DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY) AS inicio_semana,
                   DATE_ADD(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY), INTERVAL 6 DAY) AS fin_semana
        ''')
        fechas_semana = cursor.fetchone()
        
        if isinstance(fechas_semana, dict):
            inicio_semana = fechas_semana['inicio_semana']
            fin_semana = fechas_semana['fin_semana']
        else:
            inicio_semana = fechas_semana[0]
            fin_semana = fechas_semana[1]
        
        # Contar días únicos con accesos esta semana
        cursor.execute('''
            SELECT COUNT(DISTINCT DATE(fecha_hora_entrada)) as dias_unicos
            FROM accesos
            WHERE cliente_id = %s
            AND fecha_hora_entrada >= %s
            AND fecha_hora_entrada <= %s
        ''', (cliente_id, inicio_semana, fin_semana + ' 23:59:59'))
        
        resultado = cursor.fetchone()
        conn.close()
        
        if isinstance(resultado, dict):
            dias_unicos = resultado['dias_unicos'] if resultado else 0
        else:
            dias_unicos = resultado[0] if resultado else 0
        
        # Verificar si puede acceder
        if dias_unicos >= limite_semanal:
            mensaje = f'Límite semanal alcanzado. Este plan permite hasta {limite_semanal} días por semana. Ya accedió {dias_unicos} días esta semana (lunes a domingo).'
            return {
                'puede_acceder': False,
                'dias_unicos': dias_unicos,
                'limite': limite_semanal,
                'mensaje': mensaje
            }
        
        return {
            'puede_acceder': True,
            'dias_unicos': dias_unicos,
            'limite': limite_semanal,
            'mensaje': ''
        }
        
    except Exception as e:
        print(f"Error verificando límite semanal: {e}")
        # En caso de error, permitir acceso (no bloquear por falla técnica)
        return {'puede_acceder': True, 'dias_unicos': 0, 'limite': 7, 'mensaje': ''}


def formatear_datos_cliente(cliente):
    """
    Formatea los datos del cliente para la respuesta.
    """
    if not cliente:
        return None
    
    from datetime import datetime
    
    # Extraer iniciales del nombre
    nombre = cliente.get('nombre_completo', 'Cliente')
    iniciales = ''.join([n[0].upper() for n in nombre.split()[:2]])
    
    # Formatear fecha de vencimiento
    fecha_venc = cliente.get('fecha_vencimiento')
    if fecha_venc:
        try:
            if isinstance(fecha_venc, str):
                fecha_venc_dt = datetime.strptime(fecha_venc, '%Y-%m-%d')
            else:
                fecha_venc_dt = fecha_venc
            
            fecha_venc_str = fecha_venc_dt.strftime('%d/%m/%Y')
        except:
            fecha_venc_str = str(fecha_venc)
    else:
        fecha_venc_str = 'Sin fecha'
    
    return {
        'id': cliente.get('id'),
        'dni': cliente.get('dni'),
        'nombre': nombre,
        'iniciales': iniciales,
        'telefono': cliente.get('telefono'),
        'plan': cliente.get('plan_nombre', 'Sin plan'),
        'fecha_vencimiento': fecha_venc_str,
        'dias_restantes': cliente.get('dias_restantes', 0),
        'foto': cliente.get('foto'),
        'estado': cliente.get('estado')
    }


def render_template_acceso_tablet():
    """
    Genera el HTML para la vista de acceso desde tablet.
    """
    from flask import render_template_string
    
    html = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>Acceso Gimnasio</title>
        <script src="https://unpkg.com/html5-qrcode@2.3.8/html5-qrcode.min.js"></script>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%);
                min-height: 100vh;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                color: white;
            }
            
            .container {
                text-align: center;
                max-width: 500px;
                width: 90%;
                padding: 20px;
            }
            
            .header {
                margin-bottom: 30px;
            }
            
            .header h1 {
                font-size: 28px;
                margin-bottom: 10px;
                color: #2e9d36;
            }
            
            .header p {
                color: #aaa;
                font-size: 16px;
            }
            
            .scanner-container {
                background: #000;
                border-radius: 20px;
                overflow: hidden;
                margin-bottom: 20px;
                position: relative;
                aspect-ratio: 1;
                max-width: 350px;
                margin-left: auto;
                margin-right: auto;
            }
            
            /* Video invertido para ver correctamente (cámara selfie) */
            #qr-reader video {
                transform: scaleX(-1) !important;
                width: 100% !important;
                height: 100% !important;
                object-fit: cover;
            }
            
            #qr-reader {
                border: none !important;
                width: 100% !important;
                height: 100% !important;
            }
            
            #qr-reader img {
                transform: scaleX(-1) !important;
            }
            
            .scanner-frame {
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                width: 200px;
                height: 200px;
                border: 3px solid #2e9d36;
                border-radius: 20px;
                pointer-events: none;
                z-index: 10;
            }
            
            .scanner-frame::before {
                content: '';
                position: absolute;
                top: -3px;
                left: 20%;
                right: 20%;
                height: 3px;
                background: #2e9d36;
                animation: scan 2s ease-in-out infinite;
            }
            
            @keyframes scan {
                0%, 100% { top: -3px; }
                50% { top: calc(100% - 3px); }
            }
            
            .status {
                padding: 15px;
                border-radius: 10px;
                margin-bottom: 20px;
                font-size: 16px;
            }
            
            .status.idle {
                background: rgba(255, 255, 255, 0.1);
                color: #aaa;
            }
            
            .status.scanning {
                background: rgba(46, 157, 54, 0.2);
                color: #2e9d36;
            }
            
            .status.success {
                background: rgba(46, 157, 54, 0.3);
                color: #4ade80;
            }
            
            .status.error {
                background: rgba(239, 68, 68, 0.3);
                color: #f87171;
            }
            
            .instruction {
                background: rgba(255, 255, 255, 0.05);
                padding: 20px;
                border-radius: 15px;
                margin-top: 20px;
            }
            
            .instruction h3 {
                color: #2e9d36;
                margin-bottom: 10px;
                font-size: 18px;
            }
            
            .instruction p {
                color: #888;
                font-size: 14px;
                line-height: 1.6;
            }
            
            .btn {
                padding: 15px 30px;
                border: none;
                border-radius: 10px;
                font-size: 16px;
                cursor: pointer;
                transition: all 0.3s ease;
                margin-top: 20px;
            }
            
            .btn-primary {
                background: #2e9d36;
                color: white;
            }
            
            .btn-primary:hover {
                background: #3cb547;
                transform: scale(1.05);
            }
            
            .logo {
                width: 80px;
                height: 80px;
                border-radius: 50%;
                margin-bottom: 20px;
                object-fit: cover;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>ESCANEA TU QR</h1>
                <p>Muestra el código QR de tu membresía</p>
            </div>
            
            <div class="scanner-container">
                <div id="qr-reader"></div>
                <div class="scanner-frame"></div>
            </div>
            
            <div class="status idle" id="status">
                Enfoca el código QR dentro del recuadro verde
            </div>
            
            <div class="instruction">
                <h3>Cómo usarlo</h3>
                <p>
                    1. Abre la app de tu gimnasio<br>
                    2. Muestra tu código QR personal<br>
                    3. El escáner lo detectará automáticamente<br>
                    4. Espera la confirmación en la computadora
                </p>
            </div>
        </div>
        
        <script>
            const tabletId = 'tablet_' + Math.random().toString(36).substr(2, 9);
            let html5QrCode = null;
            let isScanning = false;
            
            function updateStatus(message, type) {
                const status = document.getElementById('status');
                status.textContent = message;
                status.className = 'status ' + type;
            }
            
            function onScanSuccess(decodedText, decodedResult) {
                if (!isScanning) return;
                isScanning = false;
                
                updateStatus('QR detectado, procesando...', 'scanning');
                
                // Detener el escáner
                if (html5QrCode) {
                    html5QrCode.stop().then(() => {
                        html5QrCode.clear();
                    }).catch(err => console.error(err));
                }
                
                // Enviar QR al servidor
                fetch('/api/qr-escaneado', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        qr_code: decodedText,
                        tablet_id: tabletId
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        updateStatus('QR recibido: ' + data.message, 'success');
                        
                        // Esperar y reiniciar el escáner
                        setTimeout(() => {
                            startScanner();
                        }, 3000);
                    } else {
                        updateStatus('Error: ' + data.message, 'error');
                        
                        setTimeout(() => {
                            startScanner();
                        }, 3000);
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    updateStatus('Error de conexión', 'error');
                    
                    setTimeout(() => {
                        startScanner();
                    }, 3000);
                });
            }
            
            function startScanner() {
                isScanning = true;
                updateStatus('Enfoca el código QR dentro del recuadro verde', 'idle');
                
                html5QrCode = new Html5Qrcode("qr-reader");
                
                // Usar cámara delantera (selfie)
                const config = {
                    fps: 10,
                    qrbox: { width: 200, height: 200 },
                    aspectRatio: 1.0,
                    // No aplicar mirror al escaneo (el QR funciona igual)
                    // Pero la visualización CSS ya lo invierte
                };
                
                html5QrCode.start(
                    { facingMode: "user" }, // Cámara delantera
                    config,
                    onScanSuccess,
                    (errorMessage) => {
                        // Ignorar errores de escaneo constantes
                    }
                ).then(() => {
                    console.log('Escáner iniciado');
                }).catch(err => {
                    console.error('Error iniciando cámara:', err);
                    updateStatus('No se pudo acceder a la cámara', 'error');
                });
            }
            
            // Iniciar cuando la página cargue
            window.addEventListener('DOMContentLoaded', () => {
                // Esperar un poco para que el usuario vea las instrucciones
                setTimeout(startScanner, 1000);
            });
            
            // Prevenir que la pantalla se apague en tablets
            if ('wakeLock' in navigator) {
                navigator.wakeLock.request('screen').catch(() => {});
            }
        </script>
    </body>
    </html>
    """
    
    return render_template_string(html)


# Crear tabla de accesos pendientes si no existe
def crear_tabla_accesos_pendientes():
    """
    Crea la tabla para almacenar accesos pendientes.
    Ejecutar una vez durante la instalación.
    """
    try:
        conn = db_helper.get_db_connection()
        cursor = conn.cursor()
        
        # MySQL
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accesos_pendientes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cliente_id INT,
                dni VARCHAR(20),
                qr_code TEXT,
                tablet_id VARCHAR(50),
                fecha_creacion DATETIME,
                estado ENUM('pendiente', 'aprobado', 'rechazado') DEFAULT 'pendiente',
                fecha_aprobacion DATETIME,
                datos_cliente TEXT,
                INDEX idx_estado (estado),
                INDEX idx_tablet (tablet_id),
                INDEX idx_fecha (fecha_creacion)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        ''')
        
        conn.commit()
        conn.close()
        print("Tabla accesos_pendientes creada exitosamente")
        return True
        
    except Exception as e:
        print(f"Error creando tabla: {e}")
        return False
