"""
DAO para gestionar notificaciones
"""

import sqlite3
import json
from datetime import datetime, timedelta, timezone

# Importar configuración de base de datos
from db_helper import get_db_connection, is_sqlite, is_mysql, get_current_timestamp_peru_value

class NotificacionDAO:
    @staticmethod
    def crear_notificacion(tipo, titulo, mensaje, cliente_id=None, usuario_id=None):
        """Crea una nueva notificación"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Obtener timestamp en hora peruana como valor Python (no expresión SQL)
            fecha_creacion = get_current_timestamp_peru_value()
            
            cursor.execute('''
                INSERT INTO notificaciones (tipo, titulo, mensaje, cliente_id, usuario_id, leida, fecha_creacion)
                VALUES (%s, %s, %s, %s, %s, 0, %s)
            ''', (tipo, titulo, mensaje, cliente_id, usuario_id, fecha_creacion))
            
            notificacion_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            return notificacion_id
        except Exception as e:
            print(f"Error creando notificación: {e}")
            return None

    @staticmethod
    def obtener_no_leidas(usuario_id=None, limit=30):
        """Obtiene notificaciones no leídas"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if usuario_id:
                cursor.execute('''
                    SELECT n.*, c.nombre_completo as cliente_nombre
                    FROM notificaciones n
                    LEFT JOIN clientes c ON n.cliente_id = c.id
                    WHERE n.leida = 0 
                    AND (n.usuario_id IS NULL OR n.usuario_id = %s)
                    ORDER BY n.fecha_creacion DESC
                    LIMIT %s
                ''', (usuario_id, limit))
            else:
                cursor.execute('''
                    SELECT n.*, c.nombre_completo as cliente_nombre
                    FROM notificaciones n
                    LEFT JOIN clientes c ON n.cliente_id = c.id
                    WHERE n.leida = 0 
                    AND n.usuario_id IS NULL
                    ORDER BY n.fecha_creacion DESC
                    LIMIT %s
                ''', (limit,))
            
            notificaciones = []
            for row in cursor.fetchall():
                notif = dict(row)
                notif['fecha_creacion'] = notif['fecha_creacion']
                notificaciones.append(notif)
            
            conn.close()
            return notificaciones
        except Exception as e:
            print(f"Error obteniendo notificaciones: {e}")
            return []

    @staticmethod
    def obtener_todas(usuario_id=None, limit=50):
        """Obtiene todas las notificaciones"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if usuario_id:
                cursor.execute('''
                    SELECT n.*, c.nombre_completo as cliente_nombre
                    FROM notificaciones n
                    LEFT JOIN clientes c ON n.cliente_id = c.id
                    WHERE n.usuario_id IS NULL OR n.usuario_id = %s
                    ORDER BY n.fecha_creacion DESC
                    LIMIT %s
                ''', (usuario_id, limit))
            else:
                cursor.execute('''
                    SELECT n.*, c.nombre_completo as cliente_nombre
                    FROM notificaciones n
                    LEFT JOIN clientes c ON n.cliente_id = c.id
                    WHERE n.usuario_id IS NULL
                    ORDER BY n.fecha_creacion DESC
                    LIMIT %s
                ''', (limit,))
            
            notificaciones = []
            for row in cursor.fetchall():
                notif = dict(row)
                notificaciones.append(notif)
            
            conn.close()
            return notificaciones
        except Exception as e:
            print(f"Error obteniendo todas las notificaciones: {e}")
            return []

    @staticmethod
    def marcar_como_leida(notificacion_id):
        """Marca una notificación como leída"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE notificaciones 
                SET leida = 1 
                WHERE id = %s
            ''', (notificacion_id,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error marcando notificación como leída: {e}")
            return False

    @staticmethod
    def marcar_todas_como_leidas(usuario_id=None):
        """Marca todas las notificaciones como leídas"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if usuario_id:
                cursor.execute('''
                    UPDATE notificaciones 
                    SET leida = 1 
                    WHERE leida = 0 
                    AND (usuario_id IS NULL OR usuario_id = %s)
                ''', (usuario_id,))
            else:
                cursor.execute('''
                    UPDATE notificaciones 
                    SET leida = 1 
                    WHERE leida = 0 
                    AND usuario_id IS NULL
                ''')
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error marcando todas las notificaciones como leídas: {e}")
            return False

    @staticmethod
    def eliminar_antiguas(dias=30):
        """Elimina notificaciones antiguas"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            fecha_limite = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                DELETE FROM notificaciones 
                WHERE fecha_creacion < %s
            ''', (fecha_limite,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error eliminando notificaciones antiguas: {e}")
            return False

    @staticmethod
    def limpiar_notificaciones_vencimiento(cliente_id):
        """
        Elimina todas las notificaciones de vencimiento (próximo y ya vencido)
        de un cliente específico. Se llama cuando el cliente paga, renueva
        o cambia de plan, para que dejen de aparecer en el navbar.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM notificaciones
                WHERE cliente_id = %s
                  AND tipo IN ('vencimiento', 'vencimiento_proximo')
            ''', (cliente_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error limpiando notificaciones de vencimiento para cliente {cliente_id}: {e}")
            return False

    @staticmethod
    def contar_no_leidas(usuario_id=None):
        """Cuenta las notificaciones no leídas"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            if usuario_id:
                cursor.execute('''
                    SELECT COUNT(*) 
                    FROM notificaciones 
                    WHERE leida = 0 
                    AND (usuario_id IS NULL OR usuario_id = %s)
                ''', (usuario_id,))
            else:
                cursor.execute('''
                    SELECT COUNT(*) 
                    FROM notificaciones 
                    WHERE leida = 0 
                    AND usuario_id IS NULL
                ''')
            
            count = (lambda r: list(r.values())[0] if isinstance(r, dict) else r[0])(cursor.fetchone())
            conn.close()
            return count
        except Exception as e:
            print(f"Error contando notificaciones no leídas: {e}")
            return 0

    # ================================================================
    # VERIFICACIÓN AUTOMÁTICA DE VENCIMIENTOS — versión optimizada
    # 2 consultas SQL totales (JOIN) en vez de N+1.
    # Detecta clientes con membresía a ≤3 días de vencer o ya vencida
    # y genera notificaciones evitando duplicados en el mismo día.
    # ================================================================

    @staticmethod
    def verificar_vencimientos_y_notificar():
        """
        Revisa todos los clientes activos y genera notificaciones cuando:
          - Faltan 3 días o menos para que venza su membresía → 'vencimiento_proximo'
          - La membresía ya venció                            → 'vencimiento'

        Usa LEFT JOIN para determinar en UNA sola consulta qué clientes
        ya tienen notificación hoy, evitando el problema N+1.
        Total: 2 SELECTs + N INSERTs solo cuando realmente hacen falta + 1 DELETE.

        Retorna dict con cuántas notificaciones se generaron de cada tipo.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Fecha/hora actual en Perú (UTC-5)
            peru_tz = timezone(timedelta(hours=-5))
            ahora_peru = datetime.now(timezone.utc).astimezone(peru_tz)
            ahora_str  = ahora_peru.strftime('%Y-%m-%d %H:%M:%S')

            # ── CONSULTA 1: próximos a vencer (≤ 3 días) ────────────────────
            # El LEFT JOIN detecta en una sola pasada si ya existe notificación
            # no leída de HOY para cada cliente → sin N+1.
            cursor.execute('''
                SELECT
                    c.id,
                    c.nombre_completo,
                    c.fecha_vencimiento,
                    COUNT(n.id) AS ya_notificado_hoy
                FROM clientes c
                LEFT JOIN notificaciones n
                    ON  n.cliente_id  = c.id
                    AND n.tipo        = 'vencimiento_proximo'
                    AND n.leida       = 0
                    AND DATE(n.fecha_creacion) = CURDATE()
                WHERE c.activo = 1
                  AND c.fecha_vencimiento IS NOT NULL
                  AND DATE(c.fecha_vencimiento) >= CURDATE()
                  AND DATE(c.fecha_vencimiento) <= DATE_ADD(CURDATE(), INTERVAL 3 DAY)
                GROUP BY c.id, c.nombre_completo, c.fecha_vencimiento
            ''')
            proximos = cursor.fetchall()

            creados_proximos = 0
            inserts_proximos = []  # acumular para ejecutar en lote

            for row in proximos:
                cliente        = dict(row)
                ya_notificado  = int(cliente['ya_notificado_hoy']) > 0
                if ya_notificado:
                    continue

                cliente_id = cliente['id']
                nombre     = cliente['nombre_completo']

                fecha_venc = cliente['fecha_vencimiento']
                if isinstance(fecha_venc, str):
                    fecha_venc = datetime.strptime(fecha_venc[:10], '%Y-%m-%d').date()
                elif hasattr(fecha_venc, 'date'):
                    fecha_venc = fecha_venc.date()

                dias_restantes = (fecha_venc - ahora_peru.date()).days

                if dias_restantes == 0:
                    titulo  = "⚠️ Membresía vence HOY"
                    mensaje = f"La membresía de {nombre} vence hoy. Recuérdale renovar su plan."
                elif dias_restantes == 1:
                    titulo  = "⚠️ Membresía vence mañana"
                    mensaje = f"La membresía de {nombre} vence mañana. Contáctalo para renovar."
                else:
                    titulo  = f"⚠️ Membresía por vencer en {dias_restantes} días"
                    mensaje = f"La membresía de {nombre} vence en {dias_restantes} días. Recuérdale renovar su plan."

                inserts_proximos.append(('vencimiento_proximo', titulo, mensaje, cliente_id, ahora_str))
                creados_proximos += 1

            # INSERT en lote para próximos a vencer
            if inserts_proximos:
                cursor.executemany('''
                    INSERT INTO notificaciones
                        (tipo, titulo, mensaje, cliente_id, usuario_id, leida, fecha_creacion)
                    VALUES (%s, %s, %s, %s, NULL, 0, %s)
                ''', inserts_proximos)

            # ── CONSULTA 2: ya vencidos ──────────────────────────────────────
            cursor.execute('''
                SELECT
                    c.id,
                    c.nombre_completo,
                    c.fecha_vencimiento,
                    COUNT(n.id) AS ya_notificado_hoy
                FROM clientes c
                LEFT JOIN notificaciones n
                    ON  n.cliente_id  = c.id
                    AND n.tipo        = 'vencimiento'
                    AND n.leida       = 0
                    AND DATE(n.fecha_creacion) = CURDATE()
                WHERE c.activo = 1
                  AND c.fecha_vencimiento IS NOT NULL
                  AND DATE(c.fecha_vencimiento) < CURDATE()
                GROUP BY c.id, c.nombre_completo, c.fecha_vencimiento
            ''')
            vencidos = cursor.fetchall()

            creados_vencidos = 0
            inserts_vencidos = []  # acumular para ejecutar en lote

            for row in vencidos:
                cliente       = dict(row)
                ya_notificado = int(cliente['ya_notificado_hoy']) > 0
                if ya_notificado:
                    continue

                cliente_id = cliente['id']
                nombre     = cliente['nombre_completo']

                fecha_venc = cliente['fecha_vencimiento']
                if isinstance(fecha_venc, str):
                    fecha_venc = datetime.strptime(fecha_venc[:10], '%Y-%m-%d').date()
                elif hasattr(fecha_venc, 'date'):
                    fecha_venc = fecha_venc.date()

                dias_vencido = (ahora_peru.date() - fecha_venc).days

                if dias_vencido == 1:
                    titulo  = "❌ Membresía vencida ayer"
                    mensaje = f"La membresía de {nombre} venció ayer. Contactarlo para renovar."
                else:
                    titulo  = f"❌ Membresía vencida hace {dias_vencido} días"
                    mensaje = f"La membresía de {nombre} está vencida desde hace {dias_vencido} días. Se recomienda contactarlo."

                inserts_vencidos.append(('vencimiento', titulo, mensaje, cliente_id, ahora_str))
                creados_vencidos += 1

            # INSERT en lote para vencidos
            if inserts_vencidos:
                cursor.executemany('''
                    INSERT INTO notificaciones
                        (tipo, titulo, mensaje, cliente_id, usuario_id, leida, fecha_creacion)
                    VALUES (%s, %s, %s, %s, NULL, 0, %s)
                ''', inserts_vencidos)

            # ── DELETE: limpiar notificaciones de vencidos con más de 3 días ─
            # Evita acumulación de notificaciones antiguas sin renovar.
            cursor.execute('''
                DELETE FROM notificaciones
                WHERE tipo = 'vencimiento'
                  AND leida = 0
                  AND fecha_creacion < DATE_SUB(NOW(), INTERVAL 3 DAY)
            ''')

            conn.commit()
            conn.close()

            return {
                'proximos_creados': creados_proximos,
                'vencidos_creados': creados_vencidos,
                'total': creados_proximos + creados_vencidos
            }

        except Exception as e:
            print(f"Error verificando vencimientos: {e}")
            return {'proximos_creados': 0, 'vencidos_creados': 0, 'total': 0, 'error': str(e)}