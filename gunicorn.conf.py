"""
Configuración de Gunicorn para el Sistema de Gimnasio
====================================================
Gunicorn es el servidor de producción que se encarga de manejar
las solicitudes HTTP de forma eficiente.

Esta configuración está optimizada para un VPS con 4GB de RAM.
"""

import multiprocessing
import os

# ============================================
# CONFIGURACIÓN BÁSICA
# ============================================

#绑定的地址 IP 和端口
# 0.0.0.0 = todas las interfaces de red
# 5000 = puerto de Flask
bind = "127.0.0.1:5000"

# Modo de trabajo
# 'worker' = modo producción
worker_class = "sync"  # sync es más estable para Flask

# Número de workers (procesos)
# Regla: 2-4 workers por CPU core
# Para CX23 (2 vCPU): 4-8 workers
workers = 4

# Número de threads por worker
# Útil para operaciones I/O (lectura de archivos, BD)
threads = 2

# Timeout en segundos
# Si una request tarda más, se cancela
timeout = 120

# Timeout para graceful restart
graceful_timeout = 30

# ============================================
# CONFIGURACIÓN DE PROCESOS
# ============================================

# Tiempo máximo para iniciar un worker (segundos)
worker_tmp_dir = "/dev/shm"

# Umbral de memoria para reiniciar workers (MB)
# Si un worker usa más de 1GB, se reinicia
max_requests = 1000
max_requests_jitter = 100

# ============================================
# CONFIGURACIÓN DE LOGS
# ============================================

# Archivo de logs de acceso (quién accede)
accesslog = "/var/log/gimnasio/access.log"

# Archivo de logs de errores
errorlog = "/var/log/gimnasio/error.log"

# Nivel de logs: debug, info, warning, error, critical
loglevel = "info"

# Formato de logs
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# ============================================
# CONFIGURACIÓN DE SEGURIDAD
# ============================================

# Usuario y grupo del proceso (se configura en systemd)
# user = "gimnasio"
# group = "www-data"

# Detached mode (ejecutar en segundo plano)
daemon = False

# PID file (archivo con el ID del proceso)
pidfile = "/var/run/gimnasio/gimnasio.pid"

# ============================================
# CONFIGURACIÓN DE PRE-FORK
# ============================================

# Pre-load de la aplicación (más rápido)
preload_app = True

# ============================================
# CONFIGURACIÓN DE SSL (si usas HTTPS directo)
# ============================================

# Nota: Para SSL, se recomienda usar Nginx como proxy
# Estas opciones son para uso directo de Gunicorn con SSL
#
# keyfile = "/path/to/ssl/key.pem"
# certfile = "/path/to/ssl/cert.pem"
# ssl_version = "TLSv1_2"
# ciphers = "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256"

# ============================================
# CONFIGURACIÓN DE PROXY
# ============================================

# Si usas Nginx como proxy reverso, activa esto
# Esto permite que Flask sepa la IP real del cliente
proxy_allow_ips = "127.0.0.1"

# ============================================
# VARIABLES DE ENTORNO
# ============================================

# Zona horaria de Perú
os.environ['TZ'] = 'America/Lima'
