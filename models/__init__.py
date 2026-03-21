"""
Modelos
Contiene las clases que representan los datos del sistema.
Adaptado a la estructura simplificada del frontend.
"""

class Cliente:
    """Modelo para Cliente del gimnasio"""
    
    def __init__(self, id=None, dni=None, nombre_completo=None, telefono=None, 
                 plan_id=None, fecha_inicio=None,
                 fecha_vencimiento=None, qr_code=None, fecha_registro=None, usuario_id=None, turno=None, sexo=None):
        
        self.id = id
        self.dni = dni
        self.nombre_completo = nombre_completo
        self.telefono = telefono
        self.plan_id = plan_id
        self.fecha_inicio = fecha_inicio
        self.fecha_vencimiento = fecha_vencimiento
        self.qr_code = qr_code
        self.fecha_registro = fecha_registro
        self.usuario_id = usuario_id
        self.turno = turno
        self.sexo = sexo
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'dni': self.dni,
            'nombre_completo': self.nombre_completo,
            'telefono': self.telefono,
            'plan_id': self.plan_id,
            'fecha_inicio': self.fecha_inicio,
            'fecha_vencimiento': self.fecha_vencimiento,
            'qr_code': self.qr_code,
            'fecha_registro': self.fecha_registro,
            'usuario_id': self.usuario_id,
            'turno': self.turno,
            'sexo': self.sexo
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            dni=data.get('dni'),
            nombre_completo=data.get('nombre_completo'),
            telefono=data.get('telefono'),
            plan_id=data.get('plan_id'),
            fecha_inicio=data.get('fecha_inicio'),
            fecha_vencimiento=data.get('fecha_vencimiento'),
            qr_code=data.get('qr_code'),
            fecha_registro=data.get('fecha_registro'),
            usuario_id=data.get('usuario_id'),
            turno=data.get('turno'),
            sexo=data.get('sexo')
        )


class PlanMembresia:
    """Modelo para Plan de Membresía"""
    
    def __init__(self, id=None, codigo=None, nombre=None, descripcion=None,
                 precio=None, duracion=None, caracteristicas=None, qr_habilitado=None,
                 permite_aplazamiento=None, permite_invitados=None, habilitado=None, fecha_creacion=None, usuario_id=None, limite_semanal=None):
        self.id = id
        self.codigo = codigo
        self.nombre = nombre
        self.descripcion = descripcion
        self.precio = precio
        self.duracion = duracion
        self.caracteristicas = caracteristicas
        self.qr_habilitado = qr_habilitado
        self.permite_aplazamiento = permite_aplazamiento
        self.permite_invitados = permite_invitados
        self.habilitado = habilitado
        self.fecha_creacion = fecha_creacion
        self.usuario_id = usuario_id
        self.limite_semanal = limite_semanal
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'codigo': self.codigo,
            'nombre': self.nombre,
            'descripcion': self.descripcion,
            'precio': self.precio,
            'duracion': self.duracion,
            'caracteristicas': self.caracteristicas,
            'qr_habilitado': self.qr_habilitado,
            'permite_aplazamiento': self.permite_aplazamiento,
            'permite_invitados': self.permite_invitados,
            'habilitado': self.habilitado,
            'fecha_creacion': self.fecha_creacion,
            'usuario_id': self.usuario_id,
            'limite_semanal': self.limite_semanal
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            codigo=data.get('codigo'),
            nombre=data.get('nombre'),
            descripcion=data.get('descripcion'),
            precio=data.get('precio'),
            duracion=data.get('duracion'),
            caracteristicas=data.get('caracteristicas'),
            qr_habilitado=data.get('qr_habilitado'),
            permite_aplazamiento=data.get('permite_aplazamiento'),
            permite_invitados=data.get('permite_invitados'),
            habilitado=data.get('habilitado'),
            fecha_creacion=data.get('fecha_creacion'),
            usuario_id=data.get('usuario_id'),
            limite_semanal=data.get('limite_semanal')
        )


class Pago:
    """Modelo para Pago"""
    
    def __init__(self, id=None, cliente_id=None, plan_id=None, monto=None,
                 metodo_pago=None, fecha_pago=None, estado=None, 
                 usuario_registro=None):
        self.id = id
        self.cliente_id = cliente_id
        self.plan_id = plan_id
        self.monto = monto
        self.metodo_pago = metodo_pago
        self.fecha_pago = fecha_pago
        self.estado = estado
        self.usuario_registro = usuario_registro
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'cliente_id': self.cliente_id,
            'plan_id': self.plan_id,
            'monto': self.monto,
            'metodo_pago': self.metodo_pago,
            'fecha_pago': self.fecha_pago,
            'estado': self.estado,
            'usuario_registro': self.usuario_registro
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            cliente_id=data.get('cliente_id'),
            plan_id=data.get('plan_id'),
            monto=data.get('monto'),
            metodo_pago=data.get('metodo_pago'),
            fecha_pago=data.get('fecha_pago'),
            estado=data.get('estado'),
            usuario_registro=data.get('usuario_registro')
        )


class Invitado:
    """Modelo para Invitado"""
    
    def __init__(self, id=None, cliente_titular_id=None, nombre=None, dni=None,
                 telefono=None, fecha_visita=None, hora_entrada=None, hora_salida=None,
                 estado=None, observaciones=None, usuario_id=None):
        self.id = id
        self.cliente_titular_id = cliente_titular_id
        self.nombre = nombre
        self.dni = dni
        self.telefono = telefono
        self.fecha_visita = fecha_visita
        self.hora_entrada = hora_entrada
        self.hora_salida = hora_salida
        self.estado = estado
        self.observaciones = observaciones
        self.usuario_id = usuario_id
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'cliente_titular_id': self.cliente_titular_id,
            'nombre': self.nombre,
            'dni': self.dni,
            'telefono': self.telefono,
            'fecha_visita': self.fecha_visita,
            'hora_entrada': self.hora_entrada,
            'hora_salida': self.hora_salida,
            'estado': self.estado,
            'observaciones': self.observaciones,
            'usuario_id': self.usuario_id
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            cliente_titular_id=data.get('cliente_titular_id'),
            nombre=data.get('nombre'),
            dni=data.get('dni'),
            telefono=data.get('telefono'),
            fecha_visita=data.get('fecha_visita'),
            hora_entrada=data.get('hora_entrada'),
            hora_salida=data.get('hora_salida'),
            estado=data.get('estado'),
            observaciones=data.get('observaciones'),
            usuario_id=data.get('usuario_id')
        )


class Acceso:
    """Modelo para Acceso"""
    
    def __init__(self, id=None, cliente_id=None, tipo=None,
                 dni=None, fecha_hora_entrada=None,
                 metodo_acceso=None, observaciones=None,usuario_id=None):
        self.id = id
        self.cliente_id = cliente_id
        self.tipo = tipo
        self.dni = dni
        self.fecha_hora_entrada = fecha_hora_entrada
        self.metodo_acceso = metodo_acceso
        self.observaciones = observaciones
        self.usuario_id = usuario_id
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'cliente_id': self.cliente_id,
            'tipo': self.tipo,
            'dni': self.dni,
            'fecha_hora_entrada': self.fecha_hora_entrada,
            'metodo_acceso': self.metodo_acceso,
            'observaciones': self.observaciones,
            'usuario_id': self.usuario_id
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            cliente_id=data.get('cliente_id'),
            tipo=data.get('tipo'),
            dni=data.get('dni'),
            fecha_hora_entrada=data.get('fecha_hora_entrada'),
            metodo_acceso=data.get('metodo_acceso'),
            observaciones=data.get('observaciones'),
            usuario_id=data.get('usuario_id')
        )


class Producto:
    """Modelo para Producto del inventario"""
    
    def __init__(self, id=None, nombre=None, descripcion=None, categoria=None,
                 precio=None, stock=None, stock_minimo=None,
                 estado=None, fecha_registro=None, fecha_actualizacion=None, usuario_id=None):
        self.id = id
        self.nombre = nombre
        self.descripcion = descripcion
        self.categoria = categoria
        self.precio = precio
        self.stock = stock
        self.stock_minimo = stock_minimo
        self.estado = estado
        self.fecha_registro = fecha_registro
        self.fecha_actualizacion = fecha_actualizacion
        self.usuario_id = usuario_id
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'nombre': self.nombre,
            'descripcion': self.descripcion,
            'categoria': self.categoria,
            'precio': self.precio,
            'stock': self.stock,
            'stock_minimo': self.stock_minimo,
            'estado': self.estado,
            'fecha_registro': self.fecha_registro,
            'fecha_actualizacion': self.fecha_actualizacion,
            'usuario_id': self.usuario_id
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            nombre=data.get('nombre'),
            descripcion=data.get('descripcion'),
            categoria=data.get('categoria'),
            precio=data.get('precio'),
            stock=data.get('stock'),
            stock_minimo=data.get('stock_minimo'),
            estado=data.get('estado'),
            fecha_registro=data.get('fecha_registro'),
            fecha_actualizacion=data.get('fecha_actualizacion'),
            usuario_id=data.get('usuario_id')
        )


class Usuario:
    """Modelo para Usuario del sistema (Personal del gimnasio)"""
    
    def __init__(self, id=None, dni=None, nombre_completo=None, telefono=None,
                 email=None, rol_id=None, username=None, password=None,
                 ultimo_login=None, estado=None, fecha_registro=None, foto=None, usuario_creador_id=None):
        self.id = id
        self.dni = dni
        self.nombre_completo = nombre_completo
        self.telefono = telefono
        self.email = email
        self.rol_id = rol_id
        self.username = username
        self.password = password
        self.ultimo_login = ultimo_login
        self.estado = estado
        self.fecha_registro = fecha_registro
        self.foto = foto
        self.usuario_creador_id = usuario_creador_id
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'dni': self.dni,
            'nombre_completo': self.nombre_completo,
            'telefono': self.telefono,
            'email': self.email,
            'rol_id': self.rol_id,
            'username': self.username,
            'password': self.password,
            'ultimo_login': self.ultimo_login,
            'estado': self.estado,
            'fecha_registro': self.fecha_registro,
            'foto': self.foto,
            'usuario_creador_id': self.usuario_creador_id
            
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            dni=data.get('dni'),
            nombre_completo=data.get('nombre_completo'),
            telefono=data.get('telefono'),
            email=data.get('email'),
            rol_id=data.get('rol_id'),
            username=data.get('username'),
            password=data.get('password'),
            ultimo_login=data.get('ultimo_login'),
            estado=data.get('estado'),
            fecha_registro=data.get('fecha_registro'),
            foto=data.get('foto'),
            usuario_creador_id=data.get('usuario_creador_id')
        )


class Rol:
    """Modelo para Rol de usuario"""
    
    def __init__(self, id=None, nombre=None, descripcion=None, permisos=None, estado=None, usuario_creador_id=None):
        self.id = id
        self.nombre = nombre
        self.descripcion = descripcion
        self.permisos = permisos
        self.estado = estado
        self.usuario_creador_id = usuario_creador_id
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'nombre': self.nombre,
            'descripcion': self.descripcion,
            'permisos': self.permisos,
            'estado': self.estado,
            'usuario_creador_id': self.usuario_creador_id
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            nombre=data.get('nombre'),
            descripcion=data.get('descripcion'),
            permisos=data.get('permisos'),
            estado=data.get('estado'),
            usuario_creador_id=data.get('usuario_creador_id')
        )


class HistorialMembresia:
    """Modelo para Historial de Membresía del cliente"""
    
    def __init__(self, id=None, cliente_id=None, plan_id=None, fecha_inicio=None,
                 fecha_fin=None, monto_pagado=None, metodo_pago=None, estado=None, observaciones=None, usuario_id=None):
        self.id = id
        self.cliente_id = cliente_id
        self.plan_id = plan_id
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin
        self.monto_pagado = monto_pagado
        self.metodo_pago = metodo_pago
        self.estado = estado
        self.observaciones = observaciones
        self.usuario_id = usuario_id
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'cliente_id': self.cliente_id,
            'plan_id': self.plan_id,
            'fecha_inicio': self.fecha_inicio,
            'fecha_fin': self.fecha_fin,
            'monto_pagado': self.monto_pagado,
            'metodo_pago': self.metodo_pago,
            'estado': self.estado,
            'observaciones': self.observaciones,
            'usuario_id': self.usuario_id
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            cliente_id=data.get('cliente_id'),
            plan_id=data.get('plan_id'),
            fecha_inicio=data.get('fecha_inicio'),
            fecha_fin=data.get('fecha_fin'),
            monto_pagado=data.get('monto_pagado'),
            metodo_pago=data.get('metodo_pago'),
            estado=data.get('estado'),
            observaciones=data.get('observaciones'),
            usuario_id=data.get('usuario_id')
        )


class Venta:
    """Modelo para Venta de productos"""
    
    def __init__(self, id=None, codigo=None, total=None,
                 metodo_pago=None, fecha_venta=None, estado=None,
                 cliente_dni=None, cliente_nombre=None,usuario_id=None):
        self.id = id
        self.codigo = codigo
        self.total = total
        self.metodo_pago = metodo_pago
        self.fecha_venta = fecha_venta
        self.estado = estado
        self.cliente_dni = cliente_dni
        self.cliente_nombre = cliente_nombre
        self.usuario_id = usuario_id
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'codigo': self.codigo,
            'total': self.total,
            'metodo_pago': self.metodo_pago,
            'fecha_venta': self.fecha_venta,
            'estado': self.estado,
            'cliente_dni': self.cliente_dni,
            'cliente_nombre': self.cliente_nombre,
            'usuario_id': self.usuario_id
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            codigo=data.get('codigo'),
            total=data.get('total'),
            metodo_pago=data.get('metodo_pago'),
            fecha_venta=data.get('fecha_venta'),
            estado=data.get('estado'),
            cliente_dni=data.get('cliente_dni'),
            cliente_nombre=data.get('cliente_nombre'),
            usuario_id=data.get('usuario_id')
        )


class Configuracion:
    """Modelo para Configuración del sistema"""
    
    def __init__(self, id=None, empresa_nombre=None, empresa_logo=None,
                 color_primario=None, color_secundario=None, color_acento=None,
                 whatsapp_numero=None, whatsapp_token=None, 
                 planes_habilitados=None, funcionalidades_habilitadas=None,
                 configuracion_completada=None, fecha_creacion=None,
                 fecha_modificacion=None):
        self.id = id
        self.empresa_nombre = empresa_nombre
        self.empresa_logo = empresa_logo
        self.color_primario = color_primario
        self.color_secundario = color_secundario
        self.color_acento = color_acento
        self.whatsapp_numero = whatsapp_numero
        self.whatsapp_token = whatsapp_token
        self.planes_habilitados = planes_habilitados
        self.funcionalidades_habilitadas = funcionalidades_habilitadas
        self.configuracion_completada = configuracion_completada
        self.fecha_creacion = fecha_creacion
        self.fecha_modificacion = fecha_modificacion
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'empresa_nombre': self.empresa_nombre,
            'empresa_logo': self.empresa_logo,
            'color_primario': self.color_primario,
            'color_secundario': self.color_secundario,
            'color_acento': self.color_acento,
            'whatsapp_numero': self.whatsapp_numero,
            'whatsapp_token': self.whatsapp_token,
            'planes_habilitados': self.planes_habilitados,
            'funcionalidades_habilitadas': self.funcionalidades_habilitadas,
            'configuracion_completada': self.configuracion_completada,
            'fecha_creacion': self.fecha_creacion,
            'fecha_modificacion': self.fecha_modificacion
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            empresa_nombre=data.get('empresa_nombre'),
            empresa_logo=data.get('empresa_logo'),
            color_primario=data.get('color_primario'),
            color_secundario=data.get('color_secundario'),
            color_acento=data.get('color_acento'),
            whatsapp_numero=data.get('whatsapp_numero'),
            whatsapp_token=data.get('whatsapp_token'),
            planes_habilitados=data.get('planes_habilitados'),
            funcionalidades_habilitadas=data.get('funcionalidades_habilitadas'),
            configuracion_completada=data.get('configuracion_completada'),
            fecha_creacion=data.get('fecha_creacion'),
            fecha_modificacion=data.get('fecha_modificacion')
        )


class Promocion:
    """Modelo para Promoción de Planes"""
    
    def __init__(self, id=None, plan_id=None, nombre=None, descripcion=None,
                 porcentaje_descuento=None, monto_descuento=None,
                 fecha_inicio=None, fecha_fin=None, sexo_aplicable=None,
                 activo=None, fecha_creacion=None, usuario_id=None):
        self.id = id
        self.plan_id = plan_id
        self.nombre = nombre
        self.descripcion = descripcion
        self.porcentaje_descuento = porcentaje_descuento
        self.monto_descuento = monto_descuento
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin
        self.sexo_aplicable = sexo_aplicable
        self.activo = activo
        self.fecha_creacion = fecha_creacion
        self.usuario_id = usuario_id
    
    def to_dict(self):
        """Convierte el objeto a diccionario"""
        return {
            'id': self.id,
            'plan_id': self.plan_id,
            'nombre': self.nombre,
            'descripcion': self.descripcion,
            'porcentaje_descuento': self.porcentaje_descuento,
            'monto_descuento': self.monto_descuento,
            'fecha_inicio': self.fecha_inicio,
            'fecha_fin': self.fecha_fin,
            'sexo_aplicable': self.sexo_aplicable,
            'activo': self.activo,
            'fecha_creacion': self.fecha_creacion,
            'usuario_id': self.usuario_id
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea un objeto desde un diccionario"""
        return cls(
            id=data.get('id'),
            plan_id=data.get('plan_id'),
            nombre=data.get('nombre'),
            descripcion=data.get('descripcion'),
            porcentaje_descuento=data.get('porcentaje_descuento'),
            monto_descuento=data.get('monto_descuento'),
            fecha_inicio=data.get('fecha_inicio'),
            fecha_fin=data.get('fecha_fin'),
            sexo_aplicable=data.get('sexo_aplicable'),
            activo=data.get('activo'),
            fecha_creacion=data.get('fecha_creacion'),
            usuario_id=data.get('usuario_id')
        )