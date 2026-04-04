"""
DAO Package
Contiene todos los Data Access Objects del sistema.
"""

from dao.cliente_dao import ClienteDAO
from dao.producto_dao import ProductoDAO
from dao.usuario_dao import UsuarioDAO
from dao.rol_dao import RolDAO
from dao.pago_dao import PagoDAO
from dao.venta_dao import VentaDAO
from dao.acceso_dao import AccesoDAO
from dao.plan_dao import PlanDAO
from dao.configuracion_dao import ConfiguracionDAO
from dao.invitado_dao import InvitadoDAO
from dao.historial_membresia_dao import HistorialMembresiaDAO
from dao.notificacion_dao import NotificacionDAO
from dao.inventario_dao import InventarioDAO
from dao.promocion_dao import PromocionDAO
from dao.pareja_promocion_dao import ParejaPromocionDAO

# Instancias DAO disponibles
cliente_dao = ClienteDAO()
producto_dao = ProductoDAO()
usuario_dao = UsuarioDAO()
rol_dao = RolDAO()
pago_dao = PagoDAO()
venta_dao = VentaDAO()
acceso_dao = AccesoDAO()
plan_dao = PlanDAO()
configuracion_dao = ConfiguracionDAO()
invitado_dao = InvitadoDAO()
historial_membresia_dao = HistorialMembresiaDAO()
notificacion_dao = NotificacionDAO()
inventarioDAO = InventarioDAO()
promocion_dao = PromocionDAO()
pareja_promocion_dao = ParejaPromocionDAO()