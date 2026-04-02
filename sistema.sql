-- =====================================================
-- ESQUEMA DE BASE DE DATOS - (MySQL)
-- =====================================================

SET FOREIGN_KEY_CHECKS = 0;

CREATE TABLE IF NOT EXISTS roles (
    id INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    descripcion TEXT,
    estado VARCHAR(20) DEFAULT 'activo',
    permisos TEXT,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_modificacion TIMESTAMP NULL,
    usuario_creador_id INT
);

CREATE TABLE IF NOT EXISTS planes_membresia (
    id INT PRIMARY KEY AUTO_INCREMENT,
    codigo VARCHAR(50) NOT NULL UNIQUE,
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    precio DECIMAL(10,2) NOT NULL,
    duracion VARCHAR(50) NOT NULL,
    limite_semanal INT DEFAULT 7,
    habilitado TINYINT(1) DEFAULT 1,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    qr_habilitado TINYINT(1) DEFAULT 1,
    permite_aplazamiento TINYINT(1) DEFAULT 1,
    permite_invitados TINYINT(1) DEFAULT 1,
    cantidad_invitados INT DEFAULT 0,
    envia_whatsapp TINYINT(1) DEFAULT 1,
    usuario_id INT
);

CREATE TABLE IF NOT EXISTS configuraciones (
    id INT PRIMARY KEY AUTO_INCREMENT,
    empresa_nombre VARCHAR(200),
    empresa_logo VARCHAR(300),
    color_sidebar VARCHAR(20),
    color_navbar VARCHAR(20),
    color_fondo VARCHAR(20),
    color_iconos VARCHAR(20),
    color_letras VARCHAR(20),
    color_botones VARCHAR(20),
    color_botones_secundarios VARCHAR(20),
    funcionalidades_habilitadas TEXT,
    configuracion_completada TINYINT(1) DEFAULT 0,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_modificacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    login_background VARCHAR(300) DEFAULT NULL,
    login_background_opacity INT DEFAULT 50
);

CREATE TABLE IF NOT EXISTS sesions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    session_id VARCHAR(255) NOT NULL,
    data MEDIUMBLOB,
    expiry DATETIME(6)
);

CREATE TABLE IF NOT EXISTS usuarios (
    id INT PRIMARY KEY AUTO_INCREMENT,
    dni VARCHAR(20),
    nombre_completo VARCHAR(200),
    telefono VARCHAR(20),
    email VARCHAR(150),
    rol_id INT,
    username VARCHAR(100),
    password VARCHAR(300),
    ultimo_login DATETIME,
    estado VARCHAR(20) DEFAULT 'activo',
    fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
    foto VARCHAR(300),
    usuario_creador_id INT,
    FOREIGN KEY (rol_id) REFERENCES roles(id)
);

CREATE TABLE IF NOT EXISTS clientes (
    id INT PRIMARY KEY AUTO_INCREMENT,
    dni VARCHAR(8) UNIQUE NOT NULL,
    nombre_completo VARCHAR(200) NOT NULL,
    telefono VARCHAR(20) NOT NULL,
    plan_id INT NOT NULL,
    fecha_inicio DATETIME,
    fecha_vencimiento DATETIME,
    qr_code TEXT,
    activo TINYINT(1) DEFAULT 1,
    fecha_registro DATE,
    usuario_id INT,
    turno VARCHAR(50) DEFAULT NULL,
    sexo VARCHAR(20) DEFAULT 'no_especificado',
    FOREIGN KEY (plan_id) REFERENCES planes_membresia(id)
);

CREATE TABLE IF NOT EXISTS pagos (
    id INT PRIMARY KEY AUTO_INCREMENT,
    cliente_id INT NOT NULL,
    plan_id INT NOT NULL,
    monto DECIMAL(10,2) NOT NULL DEFAULT 0,
    metodo_pago VARCHAR(50) DEFAULT 'efectivo',
    fecha_pago DATETIME DEFAULT CURRENT_TIMESTAMP,
    estado VARCHAR(20) DEFAULT 'pendiente',
    usuario_registro INT,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id),
    FOREIGN KEY (plan_id) REFERENCES planes_membresia(id)
);

CREATE TABLE IF NOT EXISTS historial_membresia (
    id INT PRIMARY KEY AUTO_INCREMENT,
    cliente_id INT NOT NULL,
    plan_id INT NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    monto_pagado DECIMAL(10,2) NOT NULL,
    metodo_pago VARCHAR(50),
    estado VARCHAR(20) DEFAULT 'activa',
    observaciones TEXT,
    fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
    usuario_id INT,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
    FOREIGN KEY (plan_id) REFERENCES planes_membresia(id)
);

CREATE TABLE IF NOT EXISTS productos (
    id INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(200) NOT NULL,
    descripcion TEXT,
    categoria VARCHAR(100) NOT NULL,
    precio DECIMAL(10,2) NOT NULL,
    stock INT DEFAULT 0,
    stock_minimo INT DEFAULT 5,
    estado VARCHAR(20) DEFAULT 'activo',
    fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    usuario_id INT
);

CREATE TABLE IF NOT EXISTS ventas (
    id INT PRIMARY KEY AUTO_INCREMENT,
    codigo VARCHAR(50),
    total DECIMAL(10,2),
    metodo_pago VARCHAR(50),
    fecha_venta DATETIME,
    estado VARCHAR(20),
    cliente_id INT NULL,
    fecha_modificacion DATETIME,
    usuario_id INT,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS detalle_ventas (
    id INT PRIMARY KEY AUTO_INCREMENT,
    venta_id INT NOT NULL,
    producto_id INT NOT NULL,
    cantidad INT NOT NULL,
    precio_unitario DECIMAL(10,2) NOT NULL,
    subtotal DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (venta_id) REFERENCES ventas(id) ON DELETE CASCADE,
    FOREIGN KEY (producto_id) REFERENCES productos(id)
);

CREATE TABLE IF NOT EXISTS entradas_inventario (
    id INT PRIMARY KEY AUTO_INCREMENT,
    producto_id INT NOT NULL,
    cantidad INT NOT NULL,
    costo_unitario DECIMAL(10,2),
    fecha_entrada DATETIME DEFAULT CURRENT_TIMESTAMP,
    usuario_registro VARCHAR(100),
    observaciones TEXT,
    costo_total DECIMAL(10,2),
    FOREIGN KEY (producto_id) REFERENCES productos(id)
);

CREATE TABLE IF NOT EXISTS invitados (
    id INT PRIMARY KEY AUTO_INCREMENT,
    cliente_titular_id INT,
    nombre VARCHAR(200),
    dni VARCHAR(20),
    telefono VARCHAR(20),
    fecha_visita DATE,
    estado VARCHAR(20) DEFAULT 'activo',
    usuario_id INT,
    FOREIGN KEY (cliente_titular_id) REFERENCES clientes(id)
);

CREATE TABLE IF NOT EXISTS accesos (
    id INT PRIMARY KEY AUTO_INCREMENT,
    cliente_id INT,
    tipo VARCHAR(50),
    dni VARCHAR(20),
    metodo_acceso VARCHAR(50),
    fecha_hora_entrada VARCHAR(50),
    usuario_id INT,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id)
);

CREATE TABLE IF NOT EXISTS notificaciones (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tipo VARCHAR(50) NOT NULL,
    titulo VARCHAR(200) NOT NULL,
    mensaje TEXT NOT NULL,
    cliente_id INT,
    usuario_id INT,
    leida TINYINT(1) DEFAULT 0,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE SET NULL,
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS intentos_login (
    id INT PRIMARY KEY AUTO_INCREMENT,
    ip_address VARCHAR(50) NOT NULL,
    username VARCHAR(100) NOT NULL,
    intentos INT DEFAULT 0,
    bloqueado_hasta DATETIME,
    ultimo_intento DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id INT PRIMARY KEY AUTO_INCREMENT,
    usuario_id INT NOT NULL,
    token_hash VARCHAR(300) NOT NULL,
    expiracion DATETIME NOT NULL,
    usado TINYINT(1) DEFAULT 0,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS promociones (
    id INT PRIMARY KEY AUTO_INCREMENT,
    plan_id INT NOT NULL,
    nombre VARCHAR(200) NOT NULL,
    descripcion TEXT,
    porcentaje_descuento DECIMAL(5,2) NULL,
    monto_descuento DECIMAL(10,2) NULL,
    fecha_inicio DATETIME NOT NULL,
    fecha_fin DATETIME NOT NULL,
    sexo_aplicable VARCHAR(20) DEFAULT 'todos',
    turno_aplicable VARCHAR(20) DEFAULT 'todos',
    activo TINYINT(1) DEFAULT 1,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    usuario_id INT,
    FOREIGN KEY (plan_id) REFERENCES planes_membresia(id) ON DELETE CASCADE
);

-- =====================================================
-- ÍNDICES DE RENDIMIENTO
-- Se crean desde db_config.py con manejo de duplicados.
-- No se ejecutan aquí directamente para evitar errores de MySQL.
-- =====================================================

SET FOREIGN_KEY_CHECKS = 1;