/**
 * Sistema de Gimnasio - Sistema de Reportes Profesionales CORREGIDO
 * VERSIÓN FINAL - Todos los reportes funcionan perfectamente
 * INCLUYE: Soporte para productos con entradas de inventario
 * NUEVO: Soporte para DETALLES DE VENTAS en exportación Excel
 */

// Configuración global
const ReportConfig = {
    colores: {
        primario: '#1E3A8A',
        secundario: '#3B82F6',
        acento: '#10B981',
        warning: '#F59E0B',
        danger: '#EF4444'
    },
    nombresArchivo: {
        clientes: 'Reporte_Clientes',
        ventas: 'Reporte_Ventas',
        membresias: 'Reporte_Membresias',
        pagos: 'Reporte_Pagos',
        productos: 'Reporte_Inventario',
        asistencia: 'Reporte_Accesos',
        invitados: 'Reporte_Invitados',
        empleados: 'Reporte_Personal',
        promociones: 'Reporte_Promociones'
    }
};

// ============================================
// FUNCIÓN PRINCIPAL: Obtener datos de la tabla
// ============================================
function obtenerDatosTablaActual() {
    const resultadosDiv = document.getElementById('resultadosReporte');
    if (!resultadosDiv) {
        console.error('No se encontró el div de resultadosReporte');
        return null;
    }

    const tablas = resultadosDiv.querySelectorAll('table');
    if (tablas.length === 0) {
        console.error('No se encontraron tablas');
        return null;
    }


    // ============================================
    // IDENTIFICAR TABLA PRINCIPAL
    // ============================================
    let tablaPrincipal = null;
    for (let i = 0; i < tablas.length; i++) {
        const tabla = tablas[i];
        if (tabla.id === 'tabla-principal-clientes' || 
            tabla.querySelector('thead th input[type="checkbox"]')) {
            tablaPrincipal = tabla;
            break;
        }
    }

    if (!tablaPrincipal) {
        tablaPrincipal = tablas[0];
    }

    const datos = {
        headers: [],
        rows: [],
        ids: [],
        tiene_checkboxes: false,
        rows_con_detalles: []
    };

    // ============================================
    // EXTRAER HEADERS
    // ============================================
    const thead = tablaPrincipal.querySelector('thead');
    if (thead) {
        const headerRow = thead.querySelector('tr:last-child');
        if (headerRow) {
            const headerCells = headerRow.querySelectorAll('th, td');
            headerCells.forEach((th, index) => {
                if (th.querySelector('input[type="checkbox"]')) {
                    datos.tiene_checkboxes = true;
                    return;
                }

                let texto = th.textContent.trim();
                texto = texto.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();

                if (texto && !texto.includes('Seleccionar')) {
                    datos.headers.push(texto);
                }
            });
        }
    }


    // ============================================
    // DETECTAR TIPO DE REPORTE Y MODO
    // ============================================
    const esClientes = window.selectedReportType === 'clientes';
    const esVentas = window.selectedReportType === 'ventas';
    const esMembresias = window.selectedReportType === 'membresias';
    const esPagos = window.selectedReportType === 'pagos';
    const esAsistencia = window.selectedReportType === 'asistencia';
    const esEmpleados = window.selectedReportType === 'empleados';
    const esInvitados = window.selectedReportType === 'invitados';
    const esProductos = window.selectedReportType === 'productos';
    const esPromociones = window.selectedReportType === 'promociones';

    const esModoTodosClientes = esClientes && 
                               document.querySelector('.btn-subtype.active')?.innerText?.includes('Todos los Clientes');
    const esPlanEspecifico = esClientes && 
                            !esModoTodosClientes && 
                            !datos.tiene_checkboxes;

    // ============================================
    // EXTRAER FILAS
    // ============================================
    const filas = tablaPrincipal.querySelectorAll('tbody tr');
    filas.forEach((fila, rowIndex) => {
        if (fila.classList.contains('historial-row') || 
            fila.id?.includes('historial-container') ||
            fila.querySelector('td[colspan]')?.getAttribute('colspan') > 5) {
            return;
        }

        const checkbox = fila.querySelector('input[type="checkbox"]');
        let procesarFila = false;
        let rowId = null;

        // CASO 1: Reportes SIN checkboxes
        if (esMembresias || esPagos || esAsistencia || esEmpleados || esInvitados || esPromociones) {
            procesarFila = true;
        }
        // CASO 2: Ventas
        else if (esVentas && checkbox) {
            if (checkbox.checked) {  // ← AHORA verifica si está marcado
                procesarFila = true;
                rowId = checkbox.dataset.id ? parseInt(checkbox.dataset.id) : null;
                if (rowId) datos.ids.push(rowId);
            }
        }
        // CASO 3: Todos los Clientes
        else if (esModoTodosClientes) {
            procesarFila = true;
            if (checkbox?.dataset.id) rowId = parseInt(checkbox.dataset.id);
        }
        // CASO 4: Plan Específico
        else if (esPlanEspecifico) {
            procesarFila = true;
        }
        // CASO 5: Productos CON checkboxes - solo seleccionadas
        else if (esProductos && checkbox) {
            if (checkbox.checked) {
                procesarFila = true;
                rowId = checkbox.dataset.id ? parseInt(checkbox.dataset.id) : null;
                if (rowId) datos.ids.push(rowId);
            }
        }
        // CASO 6: Otros reportes con checkboxes
        else if (checkbox) {
            if (checkbox.checked) {
                procesarFila = true;
                rowId = checkbox.dataset.id ? parseInt(checkbox.dataset.id) : null;
                if (rowId) datos.ids.push(rowId);
            }
        }

        if (!procesarFila) return;

        // EXTRAER DATOS DE LA FILA
        const row = [];
        const celdas = fila.querySelectorAll(':scope > td, :scope > th');
        let celdasProcesadas = 0;

        celdas.forEach((celda, colIndex) => {
            if (colIndex === 0 && celda.querySelector('input[type="checkbox"]')) {
                return;
            }

            if (datos.headers.length > 0 && celdasProcesadas >= datos.headers.length) {
                return;
            }

            let texto = celda.textContent.trim();
            texto = texto.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
            row.push(texto);
            celdasProcesadas++;
        });

        if (row.length > 0) {
            datos.rows.push(row);
            datos.rows_con_detalles.push({
                datos: row,
                id: rowId,
                detalles: []
            });
        }
    });


    return datos;
}

// ============================================
// EXPORTAR PDF
// ============================================
async function exportarPDFDatosActuales(tipoReporte) {
    const datosTabla = obtenerDatosTablaActual();
    if (!datosTabla || datosTabla.rows.length === 0) {
        showToast('No hay datos para exportar', 'warning');
        return;
    }
    try {
        showReportLoading(true);
        

        if (tipoReporte === 'clientes') {
            datosTabla.headers = ['N°', 'Nombre Completo', 'DNI', 'Teléfono', 'Plan', 'Estado', 'Fecha Inicio', 'Fecha Vencimiento', 'Registrado por'];
        }

        const data = {
            tipo_reporte: tipoReporte,
            datos_tabla: {
                headers: datosTabla.headers,
                rows: datosTabla.rows,
                rows_con_detalles: datosTabla.rows_con_detalles,
                total_registros: datosTabla.rows.length,
                tiene_checkboxes: datosTabla.tiene_checkboxes
            }
        };

        const response = await fetch('/api/reportes/exportar/pdf', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        if (!response.ok) throw new Error('Error al generar PDF');

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${ReportConfig.nombresArchivo[tipoReporte]}_${new Date().toISOString().slice(0,10)}.pdf`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        showToast('PDF descargado correctamente', 'success');
    } catch (error) {
        console.error('Error:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        showReportLoading(false);
    }
}

// ============================================
// EXPORTAR PDF CON HISTORIAL (CLIENTES)
// ============================================
async function exportarPDFConHistorial(tipoReporte, clienteIds) {
    const datosTabla = obtenerDatosTablaActual();
    if (!datosTabla || datosTabla.rows.length === 0) {
        showToast('No hay datos para exportar', 'warning');
        return;
    }
    try {
        showReportLoading(true);
        

        datosTabla.headers = ['N°', 'Nombre Completo', 'DNI', 'Teléfono', 'Plan', 'Estado', 'Fecha Inicio', 'Fecha Vencimiento', 'Registrado por'];

        const historialData = await obtenerHistorialMembresias(clienteIds);

        const data = {
            tipo_reporte: tipoReporte,
            datos_tabla: {
                headers: datosTabla.headers,
                rows: datosTabla.rows,
                rows_con_detalles: datosTabla.rows_con_detalles,
                total_registros: datosTabla.rows.length,
                tiene_checkboxes: datosTabla.tiene_checkboxes
            },
            incluir_historial: true,
            historial_membresias: historialData.historial || {}
        };

        const response = await fetch('/api/reportes/exportar/pdf', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        if (!response.ok) throw new Error('Error al generar PDF');

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${ReportConfig.nombresArchivo[tipoReporte]}_${new Date().toISOString().slice(0,10)}.pdf`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        showToast('PDF descargado correctamente', 'success');
    } catch (error) {
        console.error('Error:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        showReportLoading(false);
    }
}

// ============================================
// OBTENER ENTRADAS DE INVENTARIO (PRODUCTOS)
// ============================================
async function obtenerEntradasInventario(productoIds) {
    try {
        const entradasData = {};
        
        for (const productoId of productoIds) {
            const response = await fetch(`/api/productos/${productoId}/historial_entradas`);
            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    entradasData[productoId] = data.data || [];
                }
            }
        }
        
        return entradasData;
    } catch (error) {
        console.error('Error:', error);
        return {};
    }
}

// ============================================
// NUEVA FUNCIÓN: OBTENER DETALLES DE VENTAS
// ============================================
async function obtenerDetallesVentas(ventaIds) {
    try {
        const detallesData = {};
        
        for (const ventaId of ventaIds) {
            const response = await fetch(`/api/ventas/${ventaId}/detalles`);
            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    detallesData[ventaId] = data.data || [];
                }
            }
        }
        
        return detallesData;
    } catch (error) {
        console.error('Error al obtener detalles de ventas:', error);
        return {};
    }
}

// ============================================
// EXPORTAR PDF CON ENTRADAS (PRODUCTOS)
// ============================================
async function exportarPDFConEntradas(tipoReporte, productoIds, datosTabla) {
    if (!datosTabla || datosTabla.rows.length === 0) {
        showToast('No hay datos para exportar', 'warning');
        return;
    }
    try {
        showReportLoading(true);
        

        const entradasData = await obtenerEntradasInventario(productoIds);

        const data = {
            tipo_reporte: tipoReporte,
            datos_tabla: {
                headers: datosTabla.headers,
                rows: datosTabla.rows,
                rows_con_detalles: datosTabla.rows_con_detalles,
                total_registros: datosTabla.rows.length,
                tiene_checkboxes: datosTabla.tiene_checkboxes
            },
            incluir_entradas: true,
            entradas_inventario: entradasData
        };

        const response = await fetch('/api/reportes/exportar/pdf', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        if (!response.ok) throw new Error('Error al generar PDF');

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${ReportConfig.nombresArchivo[tipoReporte]}_${new Date().toISOString().slice(0,10)}.pdf`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        showToast('PDF descargado correctamente', 'success');
    } catch (error) {
        console.error('Error:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        showReportLoading(false);
    }
}

// ============================================
// OBTENER HISTORIAL DE MEMBRESÍAS (CLIENTES)
// ============================================
async function obtenerHistorialMembresias(clienteIds) {
    try {
        const response = await fetch('/api/clientes/historial-membresias', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ cliente_ids: clienteIds })
        });

        if (!response.ok) throw new Error('Error al obtener historial');
        
        return await response.json();
    } catch (error) {
        console.error('Error:', error);
        return { historial: {} };
    }
}

// ============================================
// EXPORTAR EXCEL CON HISTORIAL (CLIENTES)
// ============================================
async function exportarExcelConHistorial(tipoReporte, clienteIds) {
    const datosTabla = obtenerDatosTablaActual();
    if (!datosTabla || datosTabla.rows.length === 0) {
        showToast('No hay datos para exportar', 'warning');
        return;
    }
    try {
        showReportLoading(true);
        datosTabla.headers = ['N°', 'Nombre Completo', 'DNI', 'Teléfono', 'Plan', 'Estado', 'Fecha Inicio', 'Fecha Vencimiento', 'Registrado por'];
        
        // Obtener historial de membresías
        const historialData = await obtenerHistorialMembresias(clienteIds);
        
        const data = {
            tipo_reporte: tipoReporte,
            datos_tabla: {
                headers: datosTabla.headers,
                rows: datosTabla.rows,
                rows_con_detalles: datosTabla.rows_con_detalles,
                total_registros: datosTabla.rows.length,
                tiene_checkboxes: datosTabla.tiene_checkboxes
            },
            incluir_historial: true,
            historial_membresias: historialData.historial || {}
        };
        
        const response = await fetch('/api/reportes/exportar/excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        
        if (!response.ok) throw new Error('Error al generar Excel');
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${ReportConfig.nombresArchivo[tipoReporte]}_${new Date().toISOString().slice(0,10)}.xlsx`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        showToast('Excel descargado correctamente', 'success');
    } catch (error) {
        console.error('Error:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        showReportLoading(false);
    }
}

// ============================================
// EXPORTAR EXCEL CON ENTRADAS (PRODUCTOS)
// ============================================
async function exportarExcelConEntradas(tipoReporte, productoIds) {
    const datosTabla = obtenerDatosTablaActual();
    if (!datosTabla || datosTabla.rows.length === 0) {
        showToast('No hay datos para exportar', 'warning');
        return;
    }
    try {
        showReportLoading(true);
        
        // Obtener entradas de inventario
        const entradasData = await obtenerEntradasInventario(productoIds);
        
        
        const data = {
            tipo_reporte: tipoReporte,
            datos_tabla: {
                headers: datosTabla.headers,
                rows: datosTabla.rows,
                rows_con_detalles: datosTabla.rows_con_detalles,
                total_registros: datosTabla.rows.length,
                tiene_checkboxes: datosTabla.tiene_checkboxes
            },
            incluir_entradas: true,
            entradas_inventario: entradasData
        };
        
        const response = await fetch('/api/reportes/exportar/excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        
        if (!response.ok) throw new Error('Error al generar Excel');
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${ReportConfig.nombresArchivo[tipoReporte]}_${new Date().toISOString().slice(0,10)}.xlsx`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        showToast('Excel descargado correctamente', 'success');
    } catch (error) {
        console.error('Error:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        showReportLoading(false);
    }
}

// ============================================
// NUEVA FUNCIÓN: EXPORTAR EXCEL CON DETALLES DE VENTAS
// ============================================
async function exportarExcelConDetallesVentas(tipoReporte, ventaIds) {
    const datosTabla = obtenerDatosTablaActual();
    if (!datosTabla || datosTabla.rows.length === 0) {
        showToast('No hay datos para exportar', 'warning');
        return;
    }
    try {
        showReportLoading(true);
        
        // Obtener detalles de ventas
        const detallesData = await obtenerDetallesVentas(ventaIds);
        
        
        const data = {
            tipo_reporte: tipoReporte,
            datos_tabla: {
                headers: datosTabla.headers,
                rows: datosTabla.rows,
                rows_con_detalles: datosTabla.rows_con_detalles,
                total_registros: datosTabla.rows.length,
                tiene_checkboxes: datosTabla.tiene_checkboxes
            },
            incluir_detalles: true,
            detalles_ventas: detallesData
        };
        
        const response = await fetch('/api/reportes/exportar/excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        
        if (!response.ok) throw new Error('Error al generar Excel');
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${ReportConfig.nombresArchivo[tipoReporte]}_${new Date().toISOString().slice(0,10)}.xlsx`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        showToast('Excel descargado correctamente', 'success');
    } catch (error) {
        console.error('Error:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        showReportLoading(false);
    }
}

// ============================================
// EXPORTAR EXCEL
// ============================================
async function exportarExcelDatosActuales(tipoReporte) {
    const datosTabla = obtenerDatosTablaActual();
    if (!datosTabla || datosTabla.rows.length === 0) {
        showToast('No hay datos para exportar', 'warning');
        return;
    }
    try {
        showReportLoading(true);
        

        if (tipoReporte === 'clientes') {
            datosTabla.headers = ['N°', 'Nombre Completo', 'DNI', 'Teléfono', 'Plan', 'Estado', 'Fecha Inicio', 'Fecha Vencimiento', 'Registrado por'];
        }

        const data = {
            tipo_reporte: tipoReporte,
            datos_tabla: {
                headers: datosTabla.headers,
                rows: datosTabla.rows,
                rows_con_detalles: datosTabla.rows_con_detalles,
                total_registros: datosTabla.rows.length,
                tiene_checkboxes: datosTabla.tiene_checkboxes
            }
        };

        const response = await fetch('/api/reportes/exportar/excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        if (!response.ok) throw new Error('Error al generar Excel');

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${ReportConfig.nombresArchivo[tipoReporte]}_${new Date().toISOString().slice(0,10)}.xlsx`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        showToast('Excel descargado correctamente', 'success');
    } catch (error) {
        console.error('Error:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        showReportLoading(false);
    }
}

// ============================================
// MANEJADOR PRINCIPAL DE EXPORTACIÓN
// ============================================
async function handleReportExport(tipoReporte) {
    if (!tipoReporte) {
        showToast('Selecciona un tipo de reporte', 'warning');
        return;
    }

    const datosTabla = obtenerDatosTablaActual();
    if (!datosTabla || datosTabla.rows.length === 0) {
        showToast('No hay datos para exportar', 'warning');
        return;
    }

    const formatoRadio = document.querySelector('input[name="exportFormat"]:checked');
    const tipoExportacion = formatoRadio ? formatoRadio.value : 'pdf';

    // DETECTAR TIPOS DE REPORTE
    const esClientes = tipoReporte === 'clientes';
    const esVentas = tipoReporte === 'ventas';
    const esMembresias = tipoReporte === 'membresias';
    const esPagos = tipoReporte === 'pagos';
    const esAsistencia = tipoReporte === 'asistencia';
    const esEmpleados = tipoReporte === 'empleados';
    const esInvitados = tipoReporte === 'invitados';
    const esProductos = tipoReporte === 'productos';
    const esPromociones = tipoReporte === 'promociones';

    const esModoTodosClientes = esClientes && 
                               document.querySelector('.btn-subtype.active')?.innerText?.includes('Todos los Clientes');
    const esPlanEspecifico = esClientes && 
                            !esModoTodosClientes && 
                            !datosTabla.tiene_checkboxes;

    let idsSeleccionados = [];

    // Para clientes con checkboxes
    if (esClientes && !esModoTodosClientes && !esPlanEspecifico && datosTabla.tiene_checkboxes) {
        idsSeleccionados = datosTabla.ids || [];
        if (idsSeleccionados.length === 0) {
            showToast('Selecciona al menos una fila para exportar', 'warning');
            return;
        }
    }

    // Para productos con checkboxes
    if (esProductos && datosTabla.tiene_checkboxes) {
        idsSeleccionados = datosTabla.ids || [];
        if (idsSeleccionados.length === 0) {
            showToast('Selecciona al menos un producto para exportar con sus entradas', 'warning');
            return;
        }
    }

    // ✨ NUEVO: Para ventas con checkboxes
    if (esVentas && datosTabla.tiene_checkboxes) {
        idsSeleccionados = datosTabla.ids || [];
        if (idsSeleccionados.length === 0) {
            showToast('Selecciona al menos una venta para exportar con sus detalles', 'warning');
            return;
        }
    }

    // Exportar según el formato
    if (tipoExportacion === 'pdf') {
        if (esClientes && idsSeleccionados.length > 0) {
            await exportarPDFConHistorial(tipoReporte, idsSeleccionados);
        } else if (esProductos && idsSeleccionados.length > 0) {
            await exportarPDFConEntradas(tipoReporte, idsSeleccionados, datosTabla);
        } else {
            await exportarPDFDatosActuales(tipoReporte);
        }
    } else {
        // Exportar a Excel - AHORA incluye historial de membresías para clientes, entradas para productos Y detalles para ventas
        if (esClientes && idsSeleccionados.length > 0) {
            await exportarExcelConHistorial(tipoReporte, idsSeleccionados);
        } else if (esProductos && idsSeleccionados.length > 0) {
            await exportarExcelConEntradas(tipoReporte, idsSeleccionados);
        } else if (esVentas && idsSeleccionados.length > 0) {
            // ✨ NUEVO: Exportar Excel con detalles de ventas para ventas seleccionadas
            await exportarExcelConDetallesVentas(tipoReporte, idsSeleccionados);
        } else {
            await exportarExcelDatosActuales(tipoReporte);
        }
    }
}

// ============================================
// FUNCIONES AUXILIARES
// ============================================
function showReportLoading(show) {
    let loadingContainer = document.getElementById('reportLoading');
    
    if (show) {
        if (!loadingContainer) {
            loadingContainer = document.createElement('div');
            loadingContainer.id = 'reportLoading';
            // El position:fixed va en el contenedor raíz para que display:none lo oculte completamente
            loadingContainer.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); display: flex; align-items: center; justify-content: center; z-index: 9999;';
            loadingContainer.innerHTML = `
                <div style="background: white; padding: 30px 50px; border-radius: 12px; text-align: center; box-shadow: 0 10px 40px rgba(0,0,0,0.2);">
                    <div class="spinner" style="width: 50px; height: 50px; border: 4px solid #e0e0e0; border-top-color: ${ReportConfig.colores.primario}; border-radius: 50%; animation: spin 1s linear infinite; margin: 0 auto 15px;"></div>
                    <p style="margin: 0; color: #333; font-weight: 500;">Generando reporte...</p>
                </div>
            `;
            document.body.appendChild(loadingContainer);
        } else {
            loadingContainer.style.display = 'flex';
        }
    } else if (loadingContainer) {
        loadingContainer.style.display = 'none';
        // Al terminar el reporte, siempre limpiar page-leaving para desbloquear la UI
        const main = document.querySelector('.main-content');
        if (main) main.classList.remove('page-leaving');
    }
}

function showToast(message, type = 'info') {
    const colors = { success: '#10B981', error: '#EF4444', warning: '#F59E0B', info: '#3B82F6' };
    
    const toast = document.createElement('div');
    toast.style.cssText = `position: fixed; top: 20px; right: 20px; background: ${colors[type] || colors.info}; color: white; padding: 15px 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); z-index: 10000; max-width: 300px; animation: slideIn 0.3s ease;`;
    toast.textContent = message;
    
    document.body.appendChild(toast);
    setTimeout(() => { toast.remove(); }, 3000);
}

// ============================================
// EXPORTACIÓN GLOBAL
// ============================================
window.ReportConfig = ReportConfig;
window.handleReportExport = handleReportExport;
window.obtenerDatosTablaActual = obtenerDatosTablaActual;
window.exportarPDFDatosActuales = exportarPDFDatosActuales;
window.exportarPDFConHistorial = exportarPDFConHistorial;
window.exportarPDFConEntradas = exportarPDFConEntradas;
window.exportarExcelDatosActuales = exportarExcelDatosActuales;
window.exportarExcelConHistorial = exportarExcelConHistorial;
window.exportarExcelConEntradas = exportarExcelConEntradas;
window.exportarExcelConDetallesVentas = exportarExcelConDetallesVentas;  // ✨ NUEVA FUNCIÓN
window.showReportLoading = showReportLoading;
window.obtenerHistorialMembresias = obtenerHistorialMembresias;
window.obtenerEntradasInventario = obtenerEntradasInventario;
window.obtenerDetallesVentas = obtenerDetallesVentas;  // ✨ NUEVA FUNCIÓN