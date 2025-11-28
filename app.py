import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime, date
import hashlib
from io import BytesIO
import json
import base64
from datetime import datetime, date, timedelta

# ===============================INICIALIZACIÓN DE BASES DE DATOS================================

def init_avisos_db():
    """Base de datos para avisos de mantenimiento general"""
    if not os.path.exists('data'):
        os.makedirs('data')
    
    conn = sqlite3.connect('data/avisos.db', check_same_thread=False, timeout=30)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS avisos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_padre TEXT UNIQUE,
            codigo_mantto TEXT UNIQUE,
            codigo_ot_base TEXT,
            codigo_ot_sufijo TEXT,
            estado TEXT DEFAULT 'INGRESADO',
            antiguedad INTEGER,
            area TEXT,
            equipo TEXT,
            codigo_equipo TEXT,
            componentes TEXT,
            descripcion_problema TEXT,
            ingresado_por TEXT,
            ingresado_el DATE,
            hay_riesgo TEXT,
            imagen_aviso_nombre TEXT,
            imagen_aviso_datos BLOB,
            tipo_mantenimiento TEXT,
            tipo_preventivo TEXT,
            tipo_ot TEXT DEFAULT "CORRECTIVO",
            cantidad_mecanicos INTEGER DEFAULT 0,
            cantidad_electricos INTEGER DEFAULT 0,
            cantidad_soldadores INTEGER DEFAULT 0,
            cantidad_op_vahos INTEGER DEFAULT 0,
            cantidad_calderistas INTEGER DEFAULT 0,
            descripcion_trabajo TEXT,
            responsable TEXT,
            clasificacion TEXT,
            sistema TEXT,
            materiales TEXT,
            alimentador_proveedor TEXT,
            fecha_estimada_inicio DATE,
            duracion_estimada TEXT,
            responsables_comienzo TEXT,
            fecha_inicio_mantenimiento DATE,
            hora_inicio_mantenimiento TIME,
            hora_finalizacion_mantenimiento TIME,
            responsables_finalizacion TEXT,
            fecha_finalizacion DATE,
            hora_final TIME,
            descripcion_trabajo_realizado TEXT,
            paro_linea TEXT,
            imagen_final_nombre TEXT,
            imagen_final_datos BLOB,
            observaciones_cierre TEXT,
            comentario TEXT,
            tecnico_asignado TEXT,
            horas_estimadas INTEGER,
            prioridad TEXT DEFAULT "MEDIA",
            fecha_programada DATE,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    return conn

def init_ot_unicas_db():
    """Base de datos para códigos OT únicos (base)"""
    if not os.path.exists('data'):
        os.makedirs('data')
    
    conn = sqlite3.connect('data/ot_unicas.db', check_same_thread=False, timeout=30)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS ot_unicas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_padre TEXT,
            codigo_mantto TEXT,
            codigo_ot_base TEXT UNIQUE,
            codigo_ot_sufijo TEXT,
            ot_base_creado_en TIMESTAMP,
            ot_sufijo_creado_en TIMESTAMP,
            estado TEXT DEFAULT 'PROGRAMADO',
            antiguedad INTEGER,
            prioridad_nueva TEXT,
            area TEXT,
            equipo TEXT,
            codigo_equipo TEXT,
            componentes TEXT,
            descripcion_problema TEXT,
            ingresado_por TEXT,
            ingresado_el DATE,
            hay_riesgo TEXT,
            imagen_aviso_nombre TEXT,
            imagen_aviso_datos BLOB,
            tipo_mantenimiento TEXT,
            tipo_preventivo TEXT,
            cantidad_mecanicos INTEGER DEFAULT 0,
            cantidad_electricos INTEGER DEFAULT 0,
            cantidad_soldadores INTEGER DEFAULT 0,
            cantidad_op_vahos INTEGER DEFAULT 0,
            cantidad_calderistas INTEGER DEFAULT 0,
            descripcion_trabajo TEXT,
            responsable TEXT,
            clasificacion TEXT,
            sistema TEXT,
            materiales TEXT,
            alimentador_proveedor TEXT,
            fecha_estimada_inicio DATE,
            duracion_estimada TEXT,
            responsables_comienzo TEXT,
            fecha_inicio_mantenimiento DATE,
            hora_inicio_mantenimiento TIME,
            hora_finalizacion_mantenimiento TIME,
            responsables_finalizacion TEXT,
            fecha_finalizacion DATE,
            hora_final TIME,
            descripcion_trabajo_realizado TEXT,
            paro_linea TEXT,
            imagen_final_nombre TEXT,
            imagen_final_datos BLOB,
            observaciones_cierre TEXT,
            comentario TEXT,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    return conn

def init_ot_sufijos_db():
    """Base de datos para OT con sufijo"""
    if not os.path.exists('data'):
        os.makedirs('data')
    
    conn = sqlite3.connect('data/ot_sufijos.db', check_same_thread=False, timeout=30)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS ot_sufijos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_padre TEXT,
            codigo_mantto TEXT,
            codigo_ot_base TEXT,
            codigo_ot_sufijo TEXT UNIQUE,
            ot_base_creado_en TIMESTAMP,
            ot_sufijo_creado_en TIMESTAMP,
            estado TEXT DEFAULT 'PENDIENTE',
            antiguedad INTEGER,
            prioridad_nueva TEXT,
            area TEXT,
            equipo TEXT,
            codigo_equipo TEXT,
            componentes TEXT,
            descripcion_problema TEXT,
            ingresado_por TEXT,
            ingresado_el DATE,
            hay_riesgo TEXT,
            imagen_aviso_nombre TEXT,
            imagen_aviso_datos BLOB,
            tipo_mantenimiento TEXT,
            tipo_preventivo TEXT,
            cantidad_mecanicos INTEGER DEFAULT 0,
            cantidad_electricos INTEGER DEFAULT 0,
            cantidad_soldadores INTEGER DEFAULT 0,
            cantidad_op_vahos INTEGER DEFAULT 0,
            cantidad_calderistas INTEGER DEFAULT 0,
            descripcion_trabajo TEXT,
            responsable TEXT,
            clasificacion TEXT,
            sistema TEXT,
            materiales TEXT,
            alimentador_proveedor TEXT,
            fecha_estimada_inicio DATE,
            duracion_estimada TEXT,
            responsables_comienzo TEXT,
            fecha_inicio_mantenimiento DATE,
            hora_inicio_mantenimiento TIME,
            hora_finalizacion_mantenimiento TIME,
            responsables_finalizacion TEXT,
            fecha_finalizacion DATE,
            hora_final TIME,
            descripcion_trabajo_realizado TEXT,
            paro_linea TEXT,
            imagen_final_nombre TEXT,
            imagen_final_datos BLOB,
            observaciones_cierre TEXT,
            comentario TEXT,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    return conn

def init_equipos_db():
    """Base de datos para información técnica de equipos - VERSIÓN MIGRADA"""
    if not os.path.exists('data'):
        os.makedirs('data')
    
    conn = sqlite3.connect('data/equipos.db', check_same_thread=False, timeout=30)
    c = conn.cursor()
    
    # Primero creamos la tabla si no existe
    c.execute('''
        CREATE TABLE IF NOT EXISTS equipos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_equipo TEXT UNIQUE,
            equipo TEXT,
            area TEXT,
            descripcion_funcionalidad TEXT,
            especificaciones_tecnica_nombre TEXT,
            especificaciones_tecnica_datos BLOB,
            informe_nombre TEXT,
            informe_datos BLOB,
            informes_json TEXT,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # MIGRACIÓN: Verificar si necesitamos agregar la columna informes_json
    try:
        c.execute("SELECT informes_json FROM equipos LIMIT 1")
    except sqlite3.OperationalError:
        # La columna no existe, necesitamos agregarla
        st.info("🔄 Actualizando estructura de la base de datos...")
        c.execute('ALTER TABLE equipos ADD COLUMN informes_json TEXT')
        conn.commit()
        st.success("✅ Base de datos actualizada correctamente")
    
    # MIGRACIÓN: Migrar datos existentes de informe_nombre/informe_datos a informes_json
    c.execute("SELECT id, informe_nombre, informe_datos FROM equipos WHERE informe_nombre IS NOT NULL AND (informes_json IS NULL OR informes_json = '')")
    equipos_con_informes = c.fetchall()
    
    for equipo_id, nombre, datos in equipos_con_informes:
        if nombre:  # Solo si hay un informe existente
            # Codificar datos binarios a base64 para JSON
            datos_base64 = base64.b64encode(datos).decode('utf-8') if datos else None
            informe_migrado = [{
                'nombre': nombre,
                'datos_base64': datos_base64,
                'fecha_agregado': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'tipo': 'application/octet-stream'
            }]
            c.execute('UPDATE equipos SET informes_json = ? WHERE id = ?', 
                     (json.dumps(informe_migrado), equipo_id))
    
    conn.commit()
    return conn

def init_colaboradores_db():
    """Base de datos para dotación de personal de mantenimiento"""
    if not os.path.exists('data'):
        os.makedirs('data')
    
    conn = sqlite3.connect('data/colaboradores.db', check_same_thread=False, timeout=30)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS colaboradores (
            codigo_id TEXT PRIMARY KEY,
            nombre_colaborador TEXT NOT NULL,
            personal TEXT NOT NULL,
            cargo TEXT NOT NULL,
            contraseña TEXT NOT NULL,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Insertar usuario administrador por defecto si no existe
    try:
        c.execute('SELECT COUNT(*) FROM colaboradores WHERE codigo_id = ?', ('70697318',))
        if c.fetchone()[0] == 0:
            contraseña_hash = hashlib.sha256('lavidaesconstantecambio'.encode()).hexdigest()
            c.execute('''
                INSERT INTO colaboradores 
                (codigo_id, nombre_colaborador, personal, cargo, contraseña)
                VALUES (?, ?, ?, ?, ?)
            ''', ('70697318', 'DIABLO12XD', 'INTERNO', 'COORDINADOR', contraseña_hash))
    except:
        pass
    
    # Índices para mejorar rendimiento
    c.execute('CREATE INDEX IF NOT EXISTS idx_codigo_id ON colaboradores(codigo_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_cargo ON colaboradores(cargo)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_personal ON colaboradores(personal)')
    
    conn.commit()
    return conn

# ===============================INICIALIZAR CONEXIONES================================
conn_avisos = init_avisos_db()
conn_ot_unicas = init_ot_unicas_db()
conn_ot_sufijos = init_ot_sufijos_db()
conn_equipos = init_equipos_db()
conn_colaboradores = init_colaboradores_db()

# ===============================SISTEMA DE LOGIN================================

def verificar_login(codigo_id, contraseña):
    """Verifica las credenciales del usuario"""
    try:
        c = conn_colaboradores.cursor()
        c.execute('''
            SELECT codigo_id, nombre_colaborador, cargo, contraseña 
            FROM colaboradores 
            WHERE codigo_id = ?
        ''', (codigo_id,))
        
        usuario = c.fetchone()
        
        if usuario:
            # Verificar contraseña hasheada
            contraseña_hash = hashlib.sha256(contraseña.encode()).hexdigest()
            if usuario[3] == contraseña_hash:
                return {
                    'codigo_id': usuario[0],
                    'nombre': usuario[1],
                    'cargo': usuario[2],
                    'autenticado': True
                }
        
        return {'autenticado': False, 'error': 'Credenciales inválidas'}
    
    except Exception as e:
        return {'autenticado': False, 'error': f'Error del sistema: {str(e)}'}

def inicializar_sesion():
    """Inicializa la sesión del usuario si no existe"""
    if 'usuario' not in st.session_state:
        st.session_state.usuario = None
    if 'autenticado' not in st.session_state:
        st.session_state.autenticado = False

def obtener_permisos_por_cargo(cargo):
    """Define los permisos basados en el cargo del usuario"""
    # Permisos base (mínimos)
    permisos = {
        'acceso_avisos': False,
        'acceso_ot': False,
        'acceso_equipos': False,
        'acceso_colaboradores': False,
        'acceso_reportes': False,
        'acceso_bases_datos': False,
        'puede_crear': False,
        'puede_editar': False,
        'puede_eliminar': False,
        'puede_descargar_excel': False,
        'puede_ver_colaboradores': False,
        'puede_editar_equipos': False,
        'puede_eliminar_equipos': False
    }
    
    cargo_upper = cargo.upper()
    
    # ================================
    # GERENTE, JEFE DE MANTENIMIENTO, COORDINADOR
    # ================================
    if any(palabra in cargo_upper for palabra in ['GERENTE', 'JEFE DE MANTENIMIENTO', 'COORDINADOR']):
        permisos.update({
            'acceso_avisos': True,
            'acceso_ot': True,
            'acceso_equipos': True,
            'acceso_colaboradores': True,
            'acceso_reportes': True,
            'acceso_bases_datos': True,
            'puede_crear': True,
            'puede_editar': True,
            'puede_eliminar': True,
            'puede_descargar_excel': True,
            'puede_ver_colaboradores': True,
            'puede_editar_equipos': True,
            'puede_eliminar_equipos': True
        })
    
    # ================================
    # PLANNER DE MANTTO
    # ================================
    elif 'PLANNER DE MANTTO' in cargo_upper:
        permisos.update({
            'acceso_avisos': True,
            'acceso_ot': True,
            'acceso_equipos': True,
            'acceso_colaboradores': True,  # Puede ver pero no editar/eliminar
            'acceso_reportes': True,
            'acceso_bases_datos': True,
            'puede_crear': True,
            'puede_editar': True,
            'puede_eliminar': False,  # No puede eliminar colaboradores
            'puede_descargar_excel': True,
            'puede_ver_colaboradores': True,
            'puede_editar_equipos': True,
            'puede_eliminar_equipos': True
        })
    
    # ================================
    # TÉCNICOS (MECÁNICO, ELÉCTRICO, SOLDADOR, OPERADOR VAHOS, CALDERISTA, AUXILIAR)
    # ================================
    elif any(palabra in cargo_upper for palabra in ['TECNICO MECANICO', 'TECNICO ELECTRICO', 'SOLDADOR', 
                                                   'OPERADOR DE VAHOS', 'CALDERISTA', 'AUXILIAR']):
        permisos.update({
            'acceso_avisos': False,
            'acceso_ot': False,
            'acceso_equipos': True,  # Solo visualización
            'acceso_colaboradores': False,
            'acceso_reportes': True,
            'acceso_bases_datos': False,
            'puede_crear': False,
            'puede_editar': False,
            'puede_eliminar': False,
            'puede_descargar_excel': False,
            'puede_ver_colaboradores': False,
            'puede_editar_equipos': False,
            'puede_eliminar_equipos': False
        })
    
    # ================================
    # SUPERVISORES (MECÁNICO, ELÉCTRICO)
    # ================================
    elif any(palabra in cargo_upper for palabra in ['SUPERVISOR MECANICO', 'SUPERVISOR ELECTRICO']):
        permisos.update({
            'acceso_avisos': True,
            'acceso_ot': False,  # No acceso a Órdenes de Trabajo
            'acceso_equipos': True,
            'acceso_colaboradores': True,  # Solo visualización
            'acceso_reportes': True,
            'acceso_bases_datos': True,  # Solo visualización
            'puede_crear': True,
            'puede_editar': True,
            'puede_eliminar': False,  # No puede eliminar colaboradores
            'puede_descargar_excel': False,  # No puede descargar Excel
            'puede_ver_colaboradores': True,
            'puede_editar_equipos': True,
            'puede_eliminar_equipos': False  # No puede eliminar equipos
        })
    
    # ================================
    # ASISTENTE MANTENIMIENTO, PRACTICANTE MANTENIMIENTO
    # ================================
    elif any(palabra in cargo_upper for palabra in ['ASISTENTE MANTENIMIENTO', 'PRACTICANTE MANTENIMIENTO']):
        permisos.update({
            'acceso_avisos': True,
            'acceso_ot': False,  # No acceso a Órdenes de Trabajo
            'acceso_equipos': True,
            'acceso_colaboradores': True,  # Solo visualización
            'acceso_reportes': True,
            'acceso_bases_datos': True,
            'puede_crear': True,
            'puede_editar': True,
            'puede_eliminar': False,  # No puede eliminar colaboradores
            'puede_descargar_excel': True,
            'puede_ver_colaboradores': True,
            'puede_editar_equipos': True,
            'puede_eliminar_equipos': False
        })
    
    # ================================
    # INGENIERO CIVIL (permisos por defecto)
    # ================================
    elif 'INGENIERO CIVIL' in cargo_upper:
        permisos.update({
            'acceso_avisos': True,
            'acceso_ot': True,
            'acceso_equipos': True,
            'acceso_colaboradores': False,
            'acceso_reportes': True,
            'acceso_bases_datos': True,
            'puede_crear': True,
            'puede_editar': True,
            'puede_eliminar': False,
            'puede_descargar_excel': True,
            'puede_ver_colaboradores': False,
            'puede_editar_equipos': True,
            'puede_eliminar_equipos': False
        })
    
    return permisos

def mostrar_login():
    """Muestra el formulario de login"""
    st.title("🔐 Sistema de Mantenimiento - Login")
    
    st.markdown("---")
    
    # Información del sistema
    col_info1, col_info2 = st.columns(2)
    with col_info1:
        st.info("""
        **📋 Acceso al Sistema**
        
        Solo personal autorizado puede acceder al sistema 
        de gestión de mantenimiento.
        """)
    
    with col_info2:
        st.warning("""
        **🔒 Seguridad**
        
        • Use sus credenciales personales
        • No comparta su contraseña
        • Cierre sesión al terminar
        """)
    
    st.markdown("---")
    
    # Formulario de login
    with st.form("formulario_login"):
        st.subheader("Ingreso al Sistema")
        
        codigo_id = st.text_input(
            "Código de Usuario *",
            placeholder="Ej: MEC-001, SUP-001",
            help="Ingrese su código de usuario asignado"
        )
        
        contraseña = st.text_input(
            "Contraseña *", 
            type="password",
            placeholder="Ingrese su contraseña",
            help="Contraseña personal del sistema"
        )
        
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        with col_btn2:
            submitted = st.form_submit_button("🚀 Iniciar Sesión", use_container_width=True)
        
        if submitted:
            if not codigo_id or not contraseña:
                st.error("❌ Por favor, complete todos los campos")
                return
            
            # Verificar credenciales
            resultado = verificar_login(codigo_id, contraseña)
            
            if resultado['autenticado']:
                st.session_state.usuario = resultado
                st.session_state.autenticado = True
                st.session_state.permisos = obtener_permisos_por_cargo(resultado['cargo'])
                
                st.success(f"✅ ¡Bienvenido(a), {resultado['nombre']}!")
                st.balloons()
                
                # Pequeña pausa para mostrar el mensaje de bienvenida
                import time
                time.sleep(1)
                st.rerun()
            else:
                st.error(f"❌ {resultado['error']}")
    
    # Información de ayuda
    with st.expander("💡 ¿Problemas para acceder?", expanded=False):
        st.write("""
        **Si no puede acceder al sistema:**
        
        1. **Verifique su código de usuario** - Debe coincidir exactamente con el asignado
        2. **Confirme su contraseña** - Asegúrese de escribirla correctamente
        3. **Contacte al administrador** - Si olvidó sus credenciales o no aparece en el sistema
        
        **Usuario administrador por defecto:**
        - Código: `COL-000001`
        - Contraseña: `admin123`
        """)

def mostrar_logout():
    """Muestra el botón de logout en el sidebar"""
    if st.session_state.autenticado:
        st.sidebar.markdown("---")
        st.sidebar.write(f"**👤 Usuario:** {st.session_state.usuario['nombre']}")
        st.sidebar.write(f"**💼 Cargo:** {st.session_state.usuario['cargo']}")
        
        if st.sidebar.button("🚪 Cerrar Sesión", use_container_width=True):
            # Limpiar sesión
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

def verificar_acceso_seccion(seccion_requerida):
    """Verifica si el usuario tiene acceso a una sección específica"""
    if not st.session_state.autenticado:
        st.error("🔒 Debe iniciar sesión para acceder a esta sección")
        mostrar_login()
        st.stop()
    
    permisos = st.session_state.get('permisos', {})
    
    # Mapeo de secciones a permisos
    mapa_permisos = {
        'avisos': 'acceso_avisos',
        'ot': 'acceso_ot',
        'equipos': 'acceso_equipos',
        'colaboradores': 'acceso_colaboradores',
        'reportes': 'acceso_reportes',
        'bases_datos': 'acceso_bases_datos'
    }
    
    permiso_requerido = mapa_permisos.get(seccion_requerida)
    
    if permiso_requerido and not permisos.get(permiso_requerido, False):
        st.error("🚫 No tiene permisos para acceder a esta sección")
        st.info(f"Su cargo ({st.session_state.usuario['cargo']}) no tiene acceso a esta funcionalidad.")
        st.stop()
    
    return True

# ===============================FUNCIONES PARA MANEJO DE INFORMES ACUMULATIVOS================================

def obtener_informes_equipo(codigo_equipo):
    """Obtener la lista de informes de un equipo"""
    c = conn_equipos.cursor()
    c.execute('SELECT informes_json FROM equipos WHERE codigo_equipo = ?', (codigo_equipo,))
    resultado = c.fetchone()
    
    if resultado and resultado[0]:
        try:
            return json.loads(resultado[0])
        except json.JSONDecodeError:
            return []
    else:
        return []

def eliminar_informe_especifico(codigo_equipo, nombre_informe):
    """Eliminar un informe específico de un equipo"""
    try:
        informes = obtener_informes_equipo(codigo_equipo)
        informes_actualizados = [inf for inf in informes if inf['nombre'] != nombre_informe]
        
        c = conn_equipos.cursor()
        c.execute('UPDATE equipos SET informes_json = ? WHERE codigo_equipo = ?', 
                 (json.dumps(informes_actualizados), codigo_equipo))
        conn_equipos.commit()
        
        return True
    except Exception as e:
        st.error(f"❌ Error al eliminar el informe: {str(e)}")
        return False

def descargar_informe(informe_data):
    """Crear un botón de descarga para un informe"""
    if 'datos_base64' in informe_data:
        # Decodificar de base64
        datos_bytes = base64.b64decode(informe_data['datos_base64'])
    else:
        # Para compatibilidad con datos antiguos
        datos_bytes = informe_data.get('datos', b'')
    
    st.download_button(
        label=f"📥 Descargar {informe_data['nombre']}",
        data=datos_bytes,
        file_name=informe_data['nombre'],
        mime=informe_data.get('tipo', 'application/octet-stream'),
        key=f"download_{informe_data['nombre']}_{hash(informe_data['nombre'])}"
    )

# ===============================FUNCIONES PARA GESTIÓN DE COLABORADORES================================

def verificar_codigo_unico(codigo_id):
    """Verifica si el código ID ya existe en la base de datos"""
    try:
        c = conn_colaboradores.cursor()
        c.execute('SELECT COUNT(*) FROM colaboradores WHERE codigo_id = ?', (codigo_id,))
        return c.fetchone()[0] == 0
    except Exception as e:
        st.error(f"Error al verificar código: {e}")
        return False

def hash_contraseña(contraseña):
    """Hashea la contraseña para almacenamiento seguro"""
    return hashlib.sha256(contraseña.encode()).hexdigest()

def verificar_contraseña(contraseña_plana, contraseña_hash):
    """Verifica si la contraseña coincide con el hash"""
    return hashlib.sha256(contraseña_plana.encode()).hexdigest() == contraseña_hash

def obtener_colaboradores():
    """Obtener lista de todos los colaboradores"""
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_id,
                nombre_colaborador,
                personal,
                cargo,
                creado_en,
                actualizado_en
            FROM colaboradores 
            ORDER BY cargo, nombre_colaborador
        ''', conn_colaboradores)
        return df
    except Exception as e:
        st.error(f"Error al cargar colaboradores: {e}")
        return pd.DataFrame()

def obtener_colaborador_por_id(codigo_id):
    """Obtener datos de un colaborador específico"""
    try:
        c = conn_colaboradores.cursor()
        c.execute('''
            SELECT codigo_id, nombre_colaborador, personal, cargo, contraseña
            FROM colaboradores WHERE codigo_id = ?
        ''', (codigo_id,))
        return c.fetchone()
    except Exception as e:
        st.error(f"Error al obtener colaborador: {e}")
        return None

def obtener_cargos_unicos():
    """Obtener lista de cargos únicos"""
    try:
        df = pd.read_sql('SELECT DISTINCT cargo FROM colaboradores ORDER BY cargo', conn_colaboradores)
        return df['cargo'].tolist()
    except:
        return []

def obtener_personal_unico():
    """Obtener lista de tipos de personal únicos"""
    try:
        df = pd.read_sql('SELECT DISTINCT personal FROM colaboradores ORDER BY personal', conn_colaboradores)
        return df['personal'].tolist()
    except:
        return []

def agregar_colaborador(codigo_id, nombre_colaborador, personal, cargo, contraseña):
    """Agregar nuevo colaborador a la base de datos"""
    try:
        # Verificar que el código ID sea único
        if not verificar_codigo_unico(codigo_id):
            st.error("❌ Error: El código ID ya existe en la base de datos")
            return False
            
        contraseña_hash = hash_contraseña(contraseña)
        c = conn_colaboradores.cursor()
        c.execute('''
            INSERT INTO colaboradores 
            (codigo_id, nombre_colaborador, personal, cargo, contraseña, actualizado_en)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (codigo_id, nombre_colaborador, personal, cargo, contraseña_hash))
        conn_colaboradores.commit()
        return True
    except sqlite3.IntegrityError:
        st.error("❌ Error: El código ID ya existe en la base de datos")
        return False
    except Exception as e:
        st.error(f"❌ Error al agregar colaborador: {str(e)}")
        return False

def actualizar_colaborador(codigo_id_actual, nuevo_codigo_id, nombre_colaborador, personal, cargo, nueva_contraseña=None):
    """Actualizar datos de un colaborador existente"""
    try:
        # Si se cambia el código ID, verificar que el nuevo sea único
        if codigo_id_actual != nuevo_codigo_id and not verificar_codigo_unico(nuevo_codigo_id):
            st.error("❌ Error: El nuevo código ID ya existe en la base de datos")
            return False
            
        c = conn_colaboradores.cursor()
        
        if nueva_contraseña:
            contraseña_hash = hash_contraseña(nueva_contraseña)
            c.execute('''
                UPDATE colaboradores 
                SET codigo_id = ?, nombre_colaborador = ?, personal = ?, cargo = ?, 
                    contraseña = ?, actualizado_en = CURRENT_TIMESTAMP
                WHERE codigo_id = ?
            ''', (nuevo_codigo_id, nombre_colaborador, personal, cargo, contraseña_hash, codigo_id_actual))
        else:
            c.execute('''
                UPDATE colaboradores 
                SET codigo_id = ?, nombre_colaborador = ?, personal = ?, cargo = ?,
                    actualizado_en = CURRENT_TIMESTAMP
                WHERE codigo_id = ?
            ''', (nuevo_codigo_id, nombre_colaborador, personal, cargo, codigo_id_actual))
        
        conn_colaboradores.commit()
        return True
    except sqlite3.IntegrityError:
        st.error("❌ Error: El nuevo código ID ya existe en la base de datos")
        return False
    except Exception as e:
        st.error(f"❌ Error al actualizar colaborador: {str(e)}")
        return False

def eliminar_colaborador(codigo_id):
    """Eliminar colaborador de la base de datos"""
    try:
        c = conn_colaboradores.cursor()
        c.execute('DELETE FROM colaboradores WHERE codigo_id = ?', (codigo_id,))
        conn_colaboradores.commit()
        return c.rowcount > 0
    except Exception as e:
        st.error(f"❌ Error al eliminar colaborador: {str(e)}")
        return False

def mostrar_colaboradores_registrados():
    """Muestra la lista de colaboradores registrados"""
    st.subheader("👥 Colaboradores Registrados")
    
    # Filtros de búsqueda
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        busqueda = st.text_input("🔍 Buscar colaboradores", placeholder="Buscar por nombre o código...")
    with col2:
        cargos = obtener_cargos_unicos()
        cargo_filtro = st.selectbox("Filtrar por cargo", ["Todos"] + cargos)
    with col3:
        tipos_personal = obtener_personal_unico()
        personal_filtro = st.selectbox("Filtrar por tipo", ["Todos"] + tipos_personal)
    
    # Obtener todos los colaboradores
    df = obtener_colaboradores()
    
    if df.empty:
        st.info("No hay colaboradores registrados aún.")
        return
    
    # Aplicar filtros
    if busqueda:
        mask = (df['codigo_id'].str.contains(busqueda, case=False, na=False)) | \
               (df['nombre_colaborador'].str.contains(busqueda, case=False, na=False))
        df = df[mask]
    
    if cargo_filtro != "Todos":
        df = df[df['cargo'] == cargo_filtro]
    
    if personal_filtro != "Todos":
        df = df[df['personal'] == personal_filtro]
    
    # Mostrar estadísticas
    col_met1, col_met2, col_met3, col_met4 = st.columns(4)
    with col_met1:
        st.metric("Total Colaboradores", len(df))
    with col_met2:
        st.metric("Cargos Diferentes", df['cargo'].nunique())
    with col_met3:
        st.metric("Tipos de Personal", df['personal'].nunique())
    with col_met4:
        st.metric("Actualizado", df['actualizado_en'].max().split()[0] if not df.empty else "N/A")
    
    # Mostrar tabla de colaboradores
    st.dataframe(
        df[['codigo_id', 'nombre_colaborador', 'personal', 'cargo', 'creado_en']],
        use_container_width=True,
        column_config={
            "codigo_id": "Código ID",
            "nombre_colaborador": "Nombre",
            "personal": "Tipo Personal",
            "cargo": "Cargo",
            "creado_en": "Fecha Registro"
        }
    )
    
    # Gráfico de distribución por cargo
    if not df.empty:
        st.subheader("📊 Distribución por Cargo")
        cargo_counts = df['cargo'].value_counts()
        st.bar_chart(cargo_counts)

def mostrar_formulario_colaboradores():
    """Muestra el formulario para gestionar colaboradores (agregar/editar/eliminar)"""
    st.subheader("➕ Agregar Nuevo Colaborador")
    
    with st.form("formulario_colaborador", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            codigo_id = st.text_input(
                "Código ID *", 
                placeholder="Ej: MEC-001, ELEC-015, SUP-001",
                help="Código único que identificará al colaborador. No se puede repetir."
            )
            nombre_colaborador = st.text_input("Nombre Completo *", placeholder="Ej: Juan Pérez García")
            personal = st.selectbox(
                "Tipo de Personal *",
                options=["MECANICO", "ELECTRICO", "SOLDADOR", "OPERADOR_VAHOS", "CALDERISTA", "SUPERVISOR", "ADMINISTRATIVO", "INTERNO", "EXTERNO", "CONTRATISTA"]
            )
        
        with col2:
            cargo = st.selectbox(
                "Cargo *",
                options=[
                    "TECNICO MECANICO", "TECNICO ELECTRICO", "SOLDADOR", 
                    "OPERADOR DE VAHOS", "CALDERISTA", "SUPERVISOR MECANICO", 
                    "SUPERVISOR ELECTRICO", "JEFE DE MANTENIMIENTO", "PLANNER DE MANTTO", 
                    "AUXILIAR", "INGENIERO CIVIL", "COORDINADOR", "GERENTE",
                    "ASISTENTE MANTENIMIENTO","PRACTICANTE MANTENIMIENTO"
                ]
            )
            contraseña = st.text_input(
                "Contraseña *", 
                type="password",
                placeholder="Mínimo 6 caracteres",
                help="Esta contraseña se usará para el login del sistema"
            )
            confirmar_contraseña = st.text_input(
                "Confirmar Contraseña *", 
                type="password",
                placeholder="Repita la contraseña"
            )
        
        # Mostrar sugerencias de formatos de código ID
        with st.expander("💡 Sugerencias de formato para Código ID", expanded=False):
            st.write("""
            **Formatos sugeridos:**
            - **MEC-001** → Mecánico número 1
            - **ELEC-015** → Eléctrico número 15  
            - **SUP-001** → Supervisor número 1
            - **ADM-001** → Administrador número 1
            - **SOLD-008** → Soldador número 8
            - **CALD-003** → Calderista número 3
            - **OPV-002** → Operador de vahos número 2
            
            **Importante:** El código debe ser ÚNICO y no repetirse.
            """)
        
        st.markdown("**Campos obligatorios ***")
        
        submitted = st.form_submit_button("💾 Guardar Colaborador")
        
        if submitted:
            # Validaciones
            if not all([codigo_id, nombre_colaborador, personal, cargo, contraseña, confirmar_contraseña]):
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            if not codigo_id.strip():
                st.error("❌ El Código ID no puede estar vacío")
                return
            
            if contraseña != confirmar_contraseña:
                st.error("❌ Las contraseñas no coinciden")
                return
            
            if len(contraseña) < 6:
                st.error("❌ La contraseña debe tener al menos 6 caracteres")
                return
            
            # Verificar unicidad del código ID
            if not verificar_codigo_unico(codigo_id):
                st.error("❌ El Código ID ya existe. Por favor, use un código diferente.")
                return
            
            if agregar_colaborador(codigo_id, nombre_colaborador, personal, cargo, contraseña):
                st.success(f"✅ Colaborador '{nombre_colaborador}' registrado exitosamente!")
                st.success(f"🔑 Código para login: **{codigo_id}**")
                st.balloons()

def mostrar_edicion_colaboradores():
    """Muestra la interfaz para editar colaboradores"""
    st.subheader("✏️ Editar Colaborador")
    
    # Obtener lista de colaboradores para seleccionar
    colaboradores_df = obtener_colaboradores()
    
    if colaboradores_df.empty:
        st.info("No hay colaboradores registrados para editar.")
        return
    
    # Seleccionar colaborador a editar
    colaboradores_lista = [f"{row['codigo_id']} - {row['nombre_colaborador']} ({row['cargo']})" 
                          for _, row in colaboradores_df.iterrows()]
    colaborador_seleccionado = st.selectbox("Seleccionar colaborador:", colaboradores_lista, key="editar_colaborador")
    
    if not colaborador_seleccionado:
        return
    
    # Obtener código del colaborador seleccionado
    codigo_seleccionado = colaborador_seleccionado.split(' - ')[0]
    
    # Obtener datos actuales del colaborador
    colaborador_actual = obtener_colaborador_por_id(codigo_seleccionado)
    
    if not colaborador_actual:
        st.error("❌ No se pudo encontrar el colaborador seleccionado.")
        return
    
    # Mostrar formulario de edición
    with st.form("formulario_edicion_colaborador"):
        st.info(f"Editando: **{colaborador_actual[1]}** - Código actual: **{colaborador_actual[0]}**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            nuevo_codigo_id = st.text_input(
                "Código ID *", 
                value=colaborador_actual[0],
                help="Modifique solo si necesita cambiar el código ID"
            )
            nuevo_nombre = st.text_input("Nombre Completo *", value=colaborador_actual[1])
            nuevo_personal = st.selectbox(
                "Tipo de Personal *",
                options=["MECANICO", "ELECTRICO", "SOLDADOR", "OPERADOR_VAHOS", "CALDERISTA", "SUPERVISOR", "ADMINISTRATIVO", "INTERNO", "EXTERNO", "CONTRATISTA"],
                index=["MECANICO", "ELECTRICO", "SOLDADOR", "OPERADOR_VAHOS", "CALDERISTA", "SUPERVISOR", "ADMINISTRATIVO", "INTERNO", "EXTERNO", "CONTRATISTA"].index(colaborador_actual[2]) if colaborador_actual[2] in ["MECANICO", "ELECTRICO", "SOLDADOR", "OPERADOR_VAHOS", "CALDERISTA", "SUPERVISOR", "ADMINISTRATIVO", "INTERNO", "EXTERNO", "CONTRATISTA"] else 0
            )
        
        with col2:
            nuevo_cargo = st.selectbox(
                "Cargo *",
                options=[
                    "TECNICO MECANICO", "TECNICO ELECTRICO", "SOLDADOR", 
                    "OPERADOR DE VAHOS", "CALDERISTA", "SUPERVISOR MECANICO", 
                    "SUPERVISOR ELECTRICO", "JEFE DE MANTENIMIENTO", "PLANNER DE MANTTO", 
                    "AUXILIAR", "INGENIERO CIVIL", "COORDINADOR", "GERENTE",
                    "ASISTENTE MANTENIMIENTO","PRACTICANTE MANTENIMIENTO"
                ],
                index=[
                    "TECNICO MECANICO", "TECNICO ELECTRICO", "SOLDADOR", 
                    "OPERADOR DE VAHOS", "CALDERISTA", "SUPERVISOR MECANICO", 
                    "SUPERVISOR ELECTRICO", "JEFE DE MANTENIMIENTO", "PLANNER DE MANTTO", 
                    "AUXILIAR", "INGENIERO CIVIL", "COORDINADOR", "GERENTE",
                    "ASISTENTE MANTENIMIENTO","PRACTICANTE MANTENIMIENTO"
                ].index(colaborador_actual[3]) if colaborador_actual[3] in [
                    "TECNICO MECANICO", "TECNICO ELECTRICO", "SOLDADOR", 
                    "OPERADOR DE VAHOS", "CALDERISTA", "SUPERVISOR MECANICO", 
                    "SUPERVISOR ELECTRICO", "JEFE DE MANTENIMIENTO", "PLANNER DE MANTTO", 
                    "AUXILIAR", "INGENIERO CIVIL", "COORDINADOR", "GERENTE",
                    "ASISTENTE MANTENIMIENTO","PRACTICANTE MANTENIMIENTO"
                ] else 0
            )
            
            st.write("**Cambiar Contraseña** (opcional)")
            nueva_contraseña = st.text_input(
                "Nueva Contraseña", 
                type="password",
                placeholder="Dejar vacío para mantener la actual",
                help="Mínimo 6 caracteres"
            )
            confirmar_nueva_contraseña = st.text_input(
                "Confirmar Nueva Contraseña", 
                type="password",
                placeholder="Repita la nueva contraseña"
            )
        
        col_btn1, col_btn2 = st.columns([1, 1])
        
        with col_btn1:
            submitted_editar = st.form_submit_button("💾 Actualizar", use_container_width=True)
        
        with col_btn2:
            submitted_eliminar = st.form_submit_button("🗑️ Eliminar", use_container_width=True, type="secondary")
        
        if submitted_editar:
            if not all([nuevo_codigo_id, nuevo_nombre, nuevo_personal, nuevo_cargo]):
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            if not nuevo_codigo_id.strip():
                st.error("❌ El Código ID no puede estar vacío")
                return
            
            if nueva_contraseña:
                if nueva_contraseña != confirmar_nueva_contraseña:
                    st.error("❌ Las nuevas contraseñas no coinciden")
                    return
                if len(nueva_contraseña) < 6:
                    st.error("❌ La nueva contraseña debe tener al menos 6 caracteres")
                    return
            
            if actualizar_colaborador(
                codigo_seleccionado, nuevo_codigo_id, nuevo_nombre, 
                nuevo_personal, nuevo_cargo, nueva_contraseña if nueva_contraseña else None
            ):
                st.success(f"✅ Colaborador '{nuevo_nombre}' actualizado exitosamente!")
                if codigo_seleccionado != nuevo_codigo_id:
                    st.info(f"📝 Código ID actualizado: {codigo_seleccionado} → {nuevo_codigo_id}")
        
        if submitted_eliminar:
            # Mostrar advertencia de eliminación
            st.warning(f"⚠️ ¿Está seguro de eliminar al colaborador '{colaborador_actual[1]}'?")
            st.error("Esta acción no se puede deshacer.")
            
            # Crear un estado de sesión para controlar la confirmación
            if 'confirmar_eliminacion' not in st.session_state:
                st.session_state.confirmar_eliminacion = False
    
    # SECCIÓN SEPARADA FUERA DEL FORMULARIO para la confirmación de eliminación
    if submitted_eliminar:
        col_conf1, col_conf2, col_conf3 = st.columns([1, 2, 1])
        with col_conf2:
            # Botón de confirmación FUERA del formulario
            if st.button("🗑️ CONFIRMAR ELIMINACIÓN", type="primary", use_container_width=True, key="confirmar_eliminar_definitivo"):
                if eliminar_colaborador(codigo_seleccionado):
                    st.success(f"✅ Colaborador '{colaborador_actual[1]}' eliminado exitosamente!")
                    # Limpiar el estado
                    if 'confirmar_eliminacion' in st.session_state:
                        del st.session_state.confirmar_eliminacion
                    # Recargar la página
                    st.rerun()

def gestion_colaboradores():
    """Función principal para la gestión de colaboradores"""
    
    # Verificar permisos
    permisos = st.session_state.get('permisos', {})
    puede_ver_colaboradores = permisos.get('puede_ver_colaboradores', False)
    puede_editar = permisos.get('puede_editar', False)
    puede_eliminar = permisos.get('puede_eliminar', False)
    
    if not puede_ver_colaboradores:
        st.error("🚫 No tiene permisos para acceder a esta sección")
        st.info("Su cargo no tiene acceso a la gestión de colaboradores.")
        return
    
    st.title("👥 Colaboradores de Mantenimiento")
    
    # Pestañas basadas en permisos
    tab_names = ["📋 Colaboradores Registrados"]
    
    if puede_editar:
        tab_names.append("➕ Agregar/Editar Colaboradores")
    
    tabs = st.tabs(tab_names)
    
    with tabs[0]:
        mostrar_colaboradores_registrados()
    
    if puede_editar and len(tabs) > 1:
        with tabs[1]:
            col1, col2 = st.columns([1, 1])
            with col1:
                mostrar_formulario_colaboradores()
            with col2:
                mostrar_edicion_colaboradores()

def mostrar_inicio_autenticado():
    """Muestra la página de inicio para usuarios autenticados"""
    usuario = st.session_state.usuario
    
    st.title(f"🏠 Bienvenido(a), {usuario['nombre']}")
    
    # Tarjeta de información del usuario
    col_user1, col_user2, col_user3 = st.columns(3)
    
    with col_user1:
        st.info(f"**👤 Usuario:** {usuario['codigo_id']}")
    
    with col_user2:
        st.info(f"**💼 Cargo:** {usuario['cargo']}")
    
    with col_user3:
        st.info(f"**📅 Fecha:** {datetime.now().strftime('%d/%m/%Y')}")
    
    st.markdown("---")
    
    # Resumen del sistema (solo para usuarios con permisos)
    permisos = st.session_state.get('permisos', {})
    
    st.subheader("📊 Resumen del Sistema")
    
    col_res1, col_res2, col_res3, col_res4 = st.columns(4)
    
    try:
        # Avisos activos
        if permisos.get('acceso_avisos', False):
            avisos_activos = pd.read_sql(
                "SELECT COUNT(*) as total FROM avisos WHERE estado IN ('INGRESADO', 'PROGRAMADO')", 
                conn_avisos
            ).iloc[0]['total']
            with col_res1:
                st.metric("Avisos Activos", avisos_activos)
        
        # OT pendientes
        if permisos.get('acceso_ot', False):
            ot_pendientes = pd.read_sql(
                "SELECT COUNT(*) as total FROM ot_unicas WHERE estado IN ('PROGRAMADO', 'PENDIENTE')", 
                conn_ot_unicas
            ).iloc[0]['total']
            with col_res2:
                st.metric("OT Pendientes", ot_pendientes)
        
        # Total equipos
        if permisos.get('acceso_equipos', False):
            total_equipos = pd.read_sql(
                "SELECT COUNT(*) as total FROM equipos", 
                conn_equipos
            ).iloc[0]['total']
            with col_res3:
                st.metric("Equipos Registrados", total_equipos)
        
        # Total colaboradores
        if permisos.get('acceso_colaboradores', False):
            total_colaboradores = pd.read_sql(
                "SELECT COUNT(*) as total FROM colaboradores", 
                conn_colaboradores
            ).iloc[0]['total']
            with col_res4:
                st.metric("Colaboradores", total_colaboradores)
                
    except Exception as e:
        st.warning("No se pudieron cargar todas las estadísticas del sistema")
    
    st.markdown("---")
    
    # Accesos rápidos basados en permisos
    st.subheader("🚀 Accesos Rápidos")
    
    col_acc1, col_acc2, col_acc3 = st.columns(3)
    
    with col_acc1:
        if permisos.get('acceso_avisos', False) and permisos.get('puede_crear', False):
            if st.button("📝 Crear Aviso", use_container_width=True):
                st.session_state.ir_a_avisos = True
                st.rerun()
    
    with col_acc2:
        if permisos.get('acceso_ot', False) and permisos.get('puede_crear', False):
            if st.button("📋 Crear OT", use_container_width=True):
                st.session_state.ir_a_ot = True
                st.rerun()
    
    with col_acc3:
        if permisos.get('acceso_reportes', False):
            if st.button("📊 Ver Reportes", use_container_width=True):
                st.session_state.ir_a_reportes = True
                st.rerun()
    
    # Información de permisos
    with st.expander("🔐 Información de Permisos", expanded=False):
        st.write(f"**Cargo actual:** {usuario['cargo']}")
        st.write("**Permisos activos:**")
        
        permisos_info = {
            "📝 Avisos de Mantenimiento": permisos.get('acceso_avisos', False),
            "📋 Órdenes de Trabajo": permisos.get('acceso_ot', False),
            "🏭 Gestión de Equipos": permisos.get('acceso_equipos', False),
            "👥 Colaboradores": permisos.get('acceso_colaboradores', False),
            "📊 Reportes": permisos.get('acceso_reportes', False),
            "💾 Bases de Datos": permisos.get('acceso_bases_datos', False),
            "✏️ Editar datos": permisos.get('puede_editar', False),
            "🗑️ Eliminar datos": permisos.get('puede_eliminar', False)
        }
        
        for permiso, activo in permisos_info.items():
            icono = "✅" if activo else "❌"
            st.write(f"{icono} {permiso}")

# ===============================FUNCIONES PARA GESTIÓN DE EQUIPOS================================

def mostrar_formulario_equipos():
    """Muestra el formulario para agregar equipos a la base de datos"""
    st.header("📋 Agregar Nuevo Equipo")
    
    # Formulario para agregar nuevo equipo
    with st.form("formulario_equipo", clear_on_submit=True):
        st.subheader("Información Básica del Equipo")
        
        col1, col2 = st.columns(2)
        
        with col1:
            codigo_equipo = st.text_input("Código del Equipo *", placeholder="Ej: EQ-001")
            equipo = st.text_input("Nombre del Equipo *", placeholder="Ej: Bomba Centrífuga")
            area = st.text_input("Área *", placeholder="Ej: Planta de Procesos")
        
        with col2:
            descripcion_funcionalidad = st.text_area(
                "Descripción de Funcionalidad *", 
                placeholder="Describa la función principal del equipo...",
                height=100
            )
        
        st.subheader("Documentación Técnica")
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.write("**Especificaciones Técnicas**")
            especificaciones_file = st.file_uploader(
                "Subir PDF o imagen de especificaciones técnicas",
                type=['pdf', 'png', 'jpg', 'jpeg'],
                key="especificaciones"
            )
        
        with col4:
            st.write("**Informes Técnicos**")
            informe_file = st.file_uploader(
                "Subir PDF o imagen de informes técnicos",
                type=['pdf', 'png', 'jpg', 'jpeg'],
                key="informes"
            )
            st.info("💡 Puedes agregar múltiples informes posteriormente")
        
        st.markdown("**Campos obligatorios *")
        submitted = st.form_submit_button("💾 Guardar Equipo")
        
        if submitted:
            # Validar campos obligatorios
            if not codigo_equipo or not equipo or not area or not descripcion_funcionalidad:
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            try:
                # Procesar archivos subidos
                especificaciones_nombre = None
                especificaciones_datos = None
                informes_json = "[]"  # Inicialmente vacío
                
                if especificaciones_file is not None:
                    especificaciones_nombre = especificaciones_file.name
                    especificaciones_datos = especificaciones_file.getvalue()
                
                # Procesar informe inicial si se subió (usando base64)
                if informe_file is not None:
                    datos_bytes = informe_file.getvalue()
                    datos_base64 = base64.b64encode(datos_bytes).decode('utf-8')
                    
                    informe_inicial = {
                        'nombre': informe_file.name,
                        'datos_base64': datos_base64,  # Usar base64 en lugar de bytes
                        'fecha_agregado': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'tipo': informe_file.type if hasattr(informe_file, 'type') else 'application/octet-stream'
                    }
                    informes_json = json.dumps([informe_inicial])
                
                # Insertar en la base de datos
                c = conn_equipos.cursor()
                c.execute('''
                    INSERT INTO equipos 
                    (codigo_equipo, equipo, area, descripcion_funcionalidad, 
                     especificaciones_tecnica_nombre, especificaciones_tecnica_datos,
                     informes_json, actualizado_en)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (codigo_equipo, equipo, area, descripcion_funcionalidad,
                      especificaciones_nombre, especificaciones_datos,
                      informes_json))
                
                conn_equipos.commit()
                st.success(f"✅ Equipo '{equipo}' guardado exitosamente!")
                
            except sqlite3.IntegrityError:
                st.error("❌ Error: El código del equipo ya existe en la base de datos")
            except Exception as e:
                st.error(f"❌ Error al guardar el equipo: {str(e)}")

def obtener_lista_equipos():
    """Obtener lista de todos los equipos"""
    try:
        df = pd.read_sql('''
            SELECT 
                id,
                codigo_equipo, 
                equipo, 
                area, 
                descripcion_funcionalidad,
                especificaciones_tecnica_nombre,
                informes_json,
                creado_en
            FROM equipos 
            ORDER BY creado_en DESC
        ''', conn_equipos)
        return df
    except Exception as e:
        st.error(f"Error al cargar equipos: {e}")
        return pd.DataFrame()

def obtener_areas_unicas():
    """Obtener lista de áreas únicas"""
    try:
        df = pd.read_sql('SELECT DISTINCT area FROM equipos ORDER BY area', conn_equipos)
        return df['area'].tolist()
    except:
        return []

def mostrar_lista_equipos():
    """Muestra la lista de equipos existentes con opciones para editar y eliminar"""
    st.subheader("📊 Equipos Registrados")
    
    # Filtros de búsqueda
    col1, col2 = st.columns([2, 1])
    with col1:
        busqueda = st.text_input("🔍 Buscar equipos", placeholder="Buscar por código, nombre o descripción...")
    with col2:
        areas = obtener_areas_unicas()
        area_filtro = st.selectbox("Filtrar por área", ["Todas"] + areas)
    
    # Obtener todos los equipos
    df = obtener_lista_equipos()
    
    if df.empty:
        st.info("No hay equipos registrados aún.")
        return
    
    # Aplicar filtros
    if busqueda:
        mask = (df['codigo_equipo'].str.contains(busqueda, case=False, na=False)) | \
               (df['equipo'].str.contains(busqueda, case=False, na=False)) | \
               (df['descripcion_funcionalidad'].str.contains(busqueda, case=False, na=False))
        df = df[mask]
    
    if area_filtro != "Todas":
        df = df[df['area'] == area_filtro]
    
    # Contar número de informes para cada equipo
    def contar_informes(informes_json):
        if informes_json and informes_json != "[]":
            try:
                return len(json.loads(informes_json))
            except:
                return 0
        return 0
    
    df['num_informes'] = df['informes_json'].apply(contar_informes)
    
    # Mostrar tabla de equipos
    st.dataframe(
        df[['codigo_equipo', 'equipo', 'area', 'descripcion_funcionalidad', 'num_informes', 'creado_en']],
        use_container_width=True,
        column_config={
            "codigo_equipo": "Código",
            "equipo": "Equipo",
            "area": "Área",
            "descripcion_funcionalidad": "Descripción",
            "num_informes": "N° Informes",
            "creado_en": "Fecha Registro"
        }
    )
    
    # Estadísticas rápidas
    st.subheader("📈 Estadísticas")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Equipos", len(df))
    with col2:
        st.metric("Áreas Únicas", df['area'].nunique())
    with col3:
        equipos_con_especificaciones = df['especificaciones_tecnica_nombre'].notna().sum()
        st.metric("Con Especificaciones", equipos_con_especificaciones)
    with col4:
        equipos_con_informes = (df['num_informes'] > 0).sum()
        st.metric("Con Informes", equipos_con_informes)
    
    return df

def eliminar_archivo_equipo(codigo_equipo, tipo_archivo):
    """Eliminar archivo de un equipo (especificaciones)"""
    try:
        c = conn_equipos.cursor()
        if tipo_archivo == "especificaciones":
            c.execute('UPDATE equipos SET especificaciones_tecnica_nombre = NULL, especificaciones_tecnica_datos = NULL WHERE codigo_equipo = ?', (codigo_equipo,))
            mensaje = "Especificaciones eliminadas correctamente"
            conn_equipos.commit()
            st.success(f"✅ {mensaje}!")
            return True
    except Exception as e:
        st.error(f"❌ Error al eliminar archivo: {str(e)}")
        return False

def editar_equipo():
    """Permite editar un equipo existente - CON INFORMES ACUMULATIVOS"""
    st.subheader("✏️ Editar Equipo")
    
    # Obtener lista de equipos para seleccionar
    equipos_df = obtener_lista_equipos()
    
    if equipos_df.empty:
        st.info("No hay equipos registrados para editar.")
        return
    
    # Seleccionar equipo a editar
    equipos_lista = [f"{row['codigo_equipo']} - {row['equipo']} ({row['area']})" for _, row in equipos_df.iterrows()]
    equipo_seleccionado = st.selectbox("Seleccionar equipo a editar:", equipos_lista, key="editar_select")
    
    if not equipo_seleccionado:
        return
    
    # Obtener ID del equipo seleccionado
    codigo_seleccionado = equipo_seleccionado.split(' - ')[0]
    
    # Obtener datos actuales del equipo
    c = conn_equipos.cursor()
    c.execute('SELECT * FROM equipos WHERE codigo_equipo = ?', (codigo_seleccionado,))
    equipo_actual = c.fetchone()
    
    if not equipo_actual:
        st.error("❌ No se pudo encontrar el equipo seleccionado.")
        return
    
    # Mostrar información actual del equipo
    st.info(f"Editando: **{equipo_actual[2]}** ({equipo_actual[1]})")
    
    # Obtener informes actuales
    informes_actuales = obtener_informes_equipo(codigo_seleccionado)
    
    # SECCIÓN COMPLETAMENTE SEPARADA: Eliminación de archivos
    st.subheader("🗑️ Gestión de Documentación")
    
    col_elim1, col_elim2 = st.columns(2)
    
    with col_elim1:
        st.write("**Especificaciones Técnicas**")
        if equipo_actual[5]:  # Si hay especificaciones
            st.info(f"📄 Archivo actual: {equipo_actual[5]}")
            if st.button("❌ Eliminar especificaciones", key="elim_esp", use_container_width=True):
                if eliminar_archivo_equipo(codigo_seleccionado, "especificaciones"):
                    st.rerun()
        else:
            st.info("ℹ️ No hay especificaciones cargadas")
            st.button("❌ Eliminar especificaciones", disabled=True, key="elim_esp_disabled", use_container_width=True)
    
    with col_elim2:
        st.write("**Informes Técnicos**")
        if informes_actuales:
            st.info(f"📚 Total de informes: {len(informes_actuales)}")
            # Mostrar lista de informes con opción de eliminar individualmente
            with st.expander("📋 Ver informes existentes"):
                for i, informe in enumerate(informes_actuales):
                    col1, col2, col3 = st.columns([2, 1, 1])
                    with col1:
                        st.write(f"• {informe['nombre']} ({informe['fecha_agregado']})")
                    with col2:
                        descargar_informe(informe)
                    with col3:
                        if st.button("🗑️", key=f"elim_inf_{i}"):
                            if eliminar_informe_especifico(codigo_seleccionado, informe['nombre']):
                                st.rerun()
        else:
            st.info("ℹ️ No hay informes cargados")
    
    st.markdown("---")
    
    # SECCIÓN COMPLETAMENTE SEPARADA: Formulario de edición
    st.subheader("📝 Editar Información del Equipo")
    
    # Obtener datos actualizados después de posibles eliminaciones
    c.execute('SELECT * FROM equipos WHERE codigo_equipo = ?', (codigo_seleccionado,))
    equipo_actualizado = c.fetchone()
    informes_actualizados = obtener_informes_equipo(codigo_seleccionado)
    
    with st.form("formulario_edicion_equipo"):
        col1, col2 = st.columns(2)
        
        with col1:
            # Mostrar código pero no permitir editar (es la clave única)
            st.text_input("Código del Equipo *", value=equipo_actualizado[1], disabled=True)
            nuevo_equipo = st.text_input("Nombre del Equipo *", value=equipo_actualizado[2])
            nueva_area = st.text_input("Área *", value=equipo_actualizado[3])
        
        with col2:
            nueva_descripcion = st.text_area(
                "Descripción de Funcionalidad *", 
                value=equipo_actualizado[4],
                height=100
            )
        
        st.subheader("📎 Agregar Nueva Documentación")
        col3, col4 = st.columns(2)
        
        with col3:
            st.write("**Especificaciones Técnicas**")
            nuevo_especificaciones = st.file_uploader(
                "Subir nuevas especificaciones",
                type=['pdf', 'png', 'jpg', 'jpeg'],
                key="editar_especificaciones"
            )
            if equipo_actualizado[5]:
                st.success(f"✅ Hay especificaciones cargadas: {equipo_actualizado[5]}")
            else:
                st.warning("⚠️ No hay especificaciones cargadas")
        
        with col4:
            st.write("**Informes Técnicos**")
            st.info("💡 Los nuevos informes se agregarán a los existentes")
            nuevo_informe = st.file_uploader(
                "Agregar nuevo informe",
                type=['pdf', 'png', 'jpg', 'jpeg'],
                key="editar_informes"
            )
            if informes_actualizados:
                st.success(f"✅ {len(informes_actualizados)} informe(s) cargado(s)")
            else:
                st.warning("⚠️ No hay informes cargados")
        
        submitted = st.form_submit_button("💾 Actualizar Equipo", use_container_width=True)
        
        if submitted:
            if not nuevo_equipo or not nueva_area or not nueva_descripcion:
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            try:
                # Procesar nuevos archivos si se subieron
                especificaciones_nombre = equipo_actualizado[5]
                especificaciones_datos = equipo_actualizado[6]
                
                if nuevo_especificaciones is not None:
                    especificaciones_nombre = nuevo_especificaciones.name
                    especificaciones_datos = nuevo_especificaciones.getvalue()
                
                # Procesar nuevos informes (ACUMULATIVOS)
                informes_final = informes_actualizados.copy() if informes_actualizados else []
                
                if nuevo_informe is not None:
                    # Verificar si ya existe un informe con el mismo nombre
                    nombre_existe = any(inf['nombre'] == nuevo_informe.name for inf in informes_final)
                    
                    if nombre_existe:
                        st.warning(f"⚠️ Ya existe un informe con el nombre '{nuevo_informe.name}'. No se agregará.")
                    else:
                        # Codificar a base64 para JSON
                        datos_bytes = nuevo_informe.getvalue()
                        datos_base64 = base64.b64encode(datos_bytes).decode('utf-8')
                        
                        nuevo_informe_data = {
                            'nombre': nuevo_informe.name,
                            'datos_base64': datos_base64,  # Usar base64 en lugar de bytes
                            'fecha_agregado': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'tipo': nuevo_informe.type if hasattr(nuevo_informe, 'type') else 'application/octet-stream'
                        }
                        informes_final.append(nuevo_informe_data)
                        st.success(f"✅ Se agregó el informe: {nuevo_informe.name}")
                
                # Actualizar en la base de datos
                c.execute('''
                    UPDATE equipos SET
                        equipo = ?, area = ?, descripcion_funcionalidad = ?,
                        especificaciones_tecnica_nombre = ?, especificaciones_tecnica_datos = ?,
                        informes_json = ?, actualizado_en = CURRENT_TIMESTAMP
                    WHERE codigo_equipo = ?
                ''', (nuevo_equipo, nueva_area, nueva_descripcion,
                      especificaciones_nombre, especificaciones_datos,
                      json.dumps(informes_final), codigo_seleccionado))
                
                conn_equipos.commit()
                st.success(f"✅ Equipo '{nuevo_equipo}' actualizado exitosamente!")
                
                # En lugar de rerun, mostramos confirmación
                st.balloons()
                
            except Exception as e:
                st.error(f"❌ Error al actualizar el equipo: {str(e)}")

def eliminar_equipo():
    """Permite eliminar un equipo existente"""
    st.subheader("🗑️ Eliminar Equipo")
    
    # Obtener lista de equipos para seleccionar
    equipos_df = obtener_lista_equipos()
    
    if equipos_df.empty:
        st.info("No hay equipos registrados para eliminar.")
        return
    
    # Seleccionar equipo a eliminar
    equipos_lista = [f"{row['codigo_equipo']} - {row['equipo']} ({row['area']})" for _, row in equipos_df.iterrows()]
    equipo_seleccionado = st.selectbox("Seleccionar equipo a eliminar:", equipos_lista, key="eliminar_select")
    
    if equipo_seleccionado:
        # Obtener ID del equipo seleccionado
        codigo_seleccionado = equipo_seleccionado.split(' - ')[0]
        nombre_equipo = equipo_seleccionado.split(' - ')[1].split(' (')[0]
        
        # Mostrar información del equipo a eliminar
        st.warning(f"**Equipo seleccionado para eliminar:** {equipo_seleccionado}")
        
        # Confirmación de eliminación
        st.error("⚠️ **ADVERTENCIA:** Esta acción no se puede deshacer. Se eliminará permanentemente el equipo y toda su información.")
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.button("🗑️ Confirmar Eliminación", type="primary", use_container_width=True):
                try:
                    c = conn_equipos.cursor()
                    c.execute('DELETE FROM equipos WHERE codigo_equipo = ?', (codigo_seleccionado,))
                    conn_equipos.commit()
                    
                    if c.rowcount > 0:
                        st.success(f"✅ Equipo '{nombre_equipo}' eliminado exitosamente!")
                        st.balloons()
                    else:
                        st.error("❌ No se pudo eliminar el equipo.")
                        
                except Exception as e:
                    st.error(f"❌ Error al eliminar el equipo: {str(e)}")

def mostrar_dashboard_equipos():
    """Muestra dashboard con estadísticas de equipos"""
    st.subheader("📊 Dashboard de Equipos")
    
    df = obtener_lista_equipos()
    
    if df.empty:
        st.info("No hay equipos registrados para mostrar estadísticas.")
        return
    
    # Contar número de informes para cada equipo
    def contar_informes(informes_json):
        if informes_json and informes_json != "[]":
            try:
                return len(json.loads(informes_json))
            except:
                return 0
        return 0
    
    df['num_informes'] = df['informes_json'].apply(contar_informes)
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de Equipos", len(df))
    with col2:
        st.metric("Áreas Diferentes", df['area'].nunique())
    with col3:
        equipos_con_especificaciones = df['especificaciones_tecnica_nombre'].notna().sum()
        st.metric("Con Especificaciones", equipos_con_especificaciones)
    with col4:
        equipos_con_informes = (df['num_informes'] > 0).sum()
        st.metric("Con Informes", equipos_con_informes)
    
    # Distribución por áreas
    st.subheader("📈 Distribución por Áreas")
    if not df.empty:
        area_counts = df['area'].value_counts()
        st.bar_chart(area_counts)
    
    # Equipos recientes
    st.subheader("🆕 Equipos Agregados Recientemente")
    if not df.empty:
        recent_equipos = df.head(5)[['codigo_equipo', 'equipo', 'area', 'num_informes', 'creado_en']]
        st.dataframe(recent_equipos, use_container_width=True)

def gestion_equipos():
    """Función principal para la gestión de equipos"""
    
    # Verificar permisos
    permisos = st.session_state.get('permisos', {})
    puede_editar_equipos = permisos.get('puede_editar_equipos', False)
    puede_eliminar_equipos = permisos.get('puede_eliminar_equipos', False)
    
    st.title("🏭 Gestión de Equipos")
    
    # Pestañas basadas en permisos
    tab_names = ["📋 Lista de Equipos"]
    
    # Solo mostrar pestañas de edición si tiene permisos
    if puede_editar_equipos:
        tab_names.extend(["➕ Agregar Nuevo Equipo", "✏️ Editar Equipo"])
    
    if puede_eliminar_equipos:
        tab_names.append("🗑️ Eliminar Equipo")
    
    if permisos.get('acceso_reportes', False):
        tab_names.append("📊 Dashboard")
    
    tabs = st.tabs(tab_names)
    
    # Lógica para mostrar contenido según pestaña activa
    tab_index = 0
    
    # Lista de Equipos (siempre visible)
    with tabs[tab_index]:
        mostrar_lista_equipos()
    tab_index += 1
    
    # Agregar Nuevo Equipo
    if puede_editar_equipos and tab_index < len(tabs):
        with tabs[tab_index]:
            mostrar_formulario_equipos()
        tab_index += 1
    
    # Editar Equipo
    if puede_editar_equipos and tab_index < len(tabs):
        with tabs[tab_index]:
            editar_equipo()
        tab_index += 1
    
    # Eliminar Equipo
    if puede_eliminar_equipos and tab_index < len(tabs):
        with tabs[tab_index]:
            eliminar_equipo()
        tab_index += 1
    
    # Dashboard
    if permisos.get('acceso_reportes', False) and tab_index < len(tabs):
        with tabs[tab_index]:
            mostrar_dashboard_equipos()

# ===============================FUNCIONES PARA AVISOS DE MANTENIMIENTO================================

def generar_codigo_padre():
    """Genera código padre automático con formato CODP-00000001"""
    conn = conn_avisos
    c = conn.cursor()
    
    # Obtener el último código padre
    c.execute('SELECT codigo_padre FROM avisos ORDER BY id DESC LIMIT 1')
    ultimo_codigo = c.fetchone()
    
    if ultimo_codigo and ultimo_codigo[0]:
        # Extraer el número y incrementar
        try:
            numero = int(ultimo_codigo[0].split('-')[1])
            nuevo_numero = numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    
    return f"CODP-{nuevo_numero:08d}"

def generar_codigo_mantto():
    """Genera código de aviso a mantenimiento con formato AM-00000001"""
    conn = conn_avisos
    c = conn.cursor()
    
    # Obtener el último código mantto
    c.execute('SELECT codigo_mantto FROM avisos ORDER BY id DESC LIMIT 1')
    ultimo_codigo = c.fetchone()
    
    if ultimo_codigo and ultimo_codigo[0]:
        # Extraer el número y incrementar
        try:
            numero = int(ultimo_codigo[0].split('-')[1])
            nuevo_numero = numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    
    return f"AM-{nuevo_numero:08d}"

def obtener_areas_equipos():
    """Obtener lista de áreas únicas de la base de equipos"""
    try:
        df = pd.read_sql('SELECT DISTINCT area FROM equipos ORDER BY area', conn_equipos)
        return df['area'].tolist()
    except Exception as e:
        st.error(f"Error al cargar áreas: {e}")
        return []

def obtener_equipos_por_area(area):
    """Obtener lista de equipos filtrados por área"""
    try:
        df = pd.read_sql('SELECT codigo_equipo, equipo FROM equipos WHERE area = ? ORDER BY equipo', conn_equipos, params=(area,))
        return df
    except Exception as e:
        st.error(f"Error al cargar equipos: {e}")
        return pd.DataFrame()

def obtener_codigo_equipo_por_nombre(equipo_nombre, area):
    """Obtener el código del equipo basado en nombre y área"""
    try:
        c = conn_equipos.cursor()
        c.execute('SELECT codigo_equipo FROM equipos WHERE equipo = ? AND area = ?', (equipo_nombre, area))
        resultado = c.fetchone()
        return resultado[0] if resultado else None
    except Exception as e:
        st.error(f"Error al obtener código del equipo: {e}")
        return None

def calcular_antiguedad(fecha_ingreso):
    """Calcula la antigüedad en días desde la fecha de ingreso"""
    if not fecha_ingreso:
        return 0
    hoy = date.today()
    dias = (hoy - fecha_ingreso).days
    return max(0, dias)

def mostrar_formulario_avisos():
    """Muestra el formulario para crear avisos de mantenimiento"""
    st.header("📝 Crear Aviso de Mantenimiento")
    
    # Generar códigos automáticos (ocultos)
    codigo_padre = generar_codigo_padre()
    codigo_mantto = generar_codigo_mantto()
    estado = "INGRESADO"
    fecha_actual = date.today()
    tipo_mantenimiento = "MANTENIMIENTO CORRECTIVO"
    
    # SECCIÓN FUERA DEL FORMULARIO: Selección de equipo
    st.subheader("🏭 Selección del Equipo")
    
    # Área (selectbox con áreas de equipos)
    areas = obtener_areas_equipos()
    if not areas:
        st.error("No hay áreas disponibles. Primero debe registrar equipos en la sección de Gestión de Equipos.")
        return
        
    area_seleccionada = st.selectbox("Área *", options=areas, key="area_aviso")
    
    # Equipo (dependiente del área seleccionada)
    equipos_df = obtener_equipos_por_area(area_seleccionada)
    
    if equipos_df.empty:
        st.error(f"No hay equipos registrados en el área '{area_seleccionada}'.")
        return
        
    equipos_lista = equipos_df['equipo'].tolist()
    equipo_seleccionado = st.selectbox("Equipo *", options=equipos_lista, key="equipo_aviso")
    
    # Código del equipo (automático)
    codigo_equipo_auto = obtener_codigo_equipo_por_nombre(equipo_seleccionado, area_seleccionada)
    codigo_equipo = st.text_input("Código del Equipo *", value=codigo_equipo_auto, disabled=True)
    
    st.markdown("---")
    
    # SECCIÓN DEL FORMULARIO: Información específica del aviso
    with st.form("formulario_aviso", clear_on_submit=True):
        st.subheader("📋 Información del Aviso")
        
        # Mostrar información de resumen (solo lectura)
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.info(f"**Código Padre:** {codigo_padre}")
            st.info(f"**Código Mantenimiento:** {codigo_mantto}")
        with col_info2:
            st.info(f"**Estado:** {estado}")
            st.info(f"**Tipo Mantenimiento:** {tipo_mantenimiento}")
        
        st.subheader("🔧 Descripción del Problema")
        
        # Descripción del problema
        descripcion_problema = st.text_area(
            "Descripción del Problema *", 
            placeholder="Describa detalladamente el problema o falla encontrada...",
            height=120,
            help="Incluya síntomas, condiciones de operación, y cualquier información relevante"
        )
        
        st.subheader("⚠️ Información Adicional")
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Ingresado por (por ahora texto, luego será automático con login)
            ingresado_por = st.text_input("Ingresado por *", placeholder="Su nombre o usuario")
        
        with col4:
            # ¿Hay riesgo?
            hay_riesgo = st.selectbox("¿Hay riesgo? *", options=["NO", "SI"], 
                                    help="¿Existe algún riesgo para la seguridad o el medio ambiente?")
        
        st.subheader("📷 Evidencia Fotográfica (Opcional)")
        
        # Imagen del aviso
        imagen_aviso = st.file_uploader(
            "Subir imagen del problema",
            type=['png', 'jpg', 'jpeg'],
            help="Foto que muestre el problema o falla (opcional)"
        )
        
        # Validación y envío
        st.markdown("**Campos obligatorios ***")
        
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        with col_btn2:
            submitted = st.form_submit_button("📤 Enviar Aviso de Mantenimiento", use_container_width=True)
        
        if submitted:
            # Validar campos obligatorios
            if not descripcion_problema or not ingresado_por:
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            try:
                # Procesar imagen si se subió
                imagen_nombre = None
                imagen_datos = None
                
                if imagen_aviso is not None:
                    imagen_nombre = imagen_aviso.name
                    imagen_datos = imagen_aviso.getvalue()
                
                # Calcular antigüedad
                antiguedad_dias = calcular_antiguedad(fecha_actual)
                
                # Insertar en la base de datos
                c = conn_avisos.cursor()
                c.execute('''
                    INSERT INTO avisos 
                    (codigo_padre, codigo_mantto, estado, antiguedad, area, equipo, 
                     codigo_equipo, descripcion_problema, ingresado_por, ingresado_el,
                     hay_riesgo, imagen_aviso_nombre, imagen_aviso_datos, tipo_mantenimiento)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    codigo_padre, codigo_mantto, estado, antiguedad_dias, area_seleccionada, 
                    equipo_seleccionado, codigo_equipo, descripcion_problema, ingresado_por, 
                    fecha_actual, hay_riesgo, imagen_nombre, imagen_datos, tipo_mantenimiento
                ))
                
                conn_avisos.commit()
                
                st.success(f"✅ Aviso de mantenimiento '{codigo_mantto}' creado exitosamente!")
                st.balloons()
                
                # Mostrar resumen del aviso creado
                with st.expander("📋 Ver resumen del aviso creado", expanded=True):
                    col_res1, col_res2 = st.columns(2)
                    with col_res1:
                        st.write(f"**Código Padre:** {codigo_padre}")
                        st.write(f"**Código Mantenimiento:** {codigo_mantto}")
                        st.write(f"**Estado:** {estado}")
                        st.write(f"**Área:** {area_seleccionada}")
                        st.write(f"**Equipo:** {equipo_seleccionado}")
                    
                    with col_res2:
                        st.write(f"**Código Equipo:** {codigo_equipo}")
                        st.write(f"**Ingresado por:** {ingresado_por}")
                        st.write(f"**Fecha de ingreso:** {fecha_actual}")
                        st.write(f"**¿Hay riesgo?:** {hay_riesgo}")
                        st.write(f"**Tipo mantenimiento:** {tipo_mantenimiento}")
                    
                    st.write("**Descripción del problema:**")
                    st.info(descripcion_problema)
                    
                    if imagen_nombre:
                        st.write(f"**Imagen adjunta:** {imagen_nombre}")
                
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed" in str(e):
                    st.error("❌ Error: Uno de los códigos ya existe en la base de datos. Intente crear el aviso nuevamente.")
                else:
                    st.error(f"❌ Error de integridad en la base de datos: {str(e)}")
            except Exception as e:
                st.error(f"❌ Error al crear el aviso: {str(e)}")

def obtener_lista_avisos():
    """Obtener lista de todos los avisos"""
    try:
        df = pd.read_sql('''
            SELECT 
                id,
                codigo_padre,
                codigo_mantto,
                estado,
                antiguedad,
                area,
                equipo,
                codigo_equipo,
                descripcion_problema,
                ingresado_por,
                ingresado_el,
                hay_riesgo,
                imagen_aviso_nombre,
                tipo_mantenimiento,
                creado_en
            FROM avisos 
            ORDER BY creado_en DESC
        ''', conn_avisos)
        return df
    except Exception as e:
        st.error(f"Error al cargar avisos: {e}")
        return pd.DataFrame()

def mostrar_lista_avisos():
    """Muestra la lista de avisos existentes"""
    st.subheader("📊 Avisos Registrados")
    
    # Filtros de búsqueda
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        busqueda = st.text_input("🔍 Buscar avisos", placeholder="Buscar por código, equipo o descripción...", key="busqueda_avisos")
    with col2:
        estados = ["Todos", "INGRESADO", "PROGRAMADO", "PENDIENTE", "CULMINADO", "CERRADO", "ANULADO"]
        estado_filtro = st.selectbox("Filtrar por estado", estados, key="estado_avisos")
    with col3:
        areas = ["Todas"] + obtener_areas_equipos()
        area_filtro = st.selectbox("Filtrar por área", areas, key="area_filtro_avisos")
    
    # Obtener todos los avisos
    df = obtener_lista_avisos()
    
    if df.empty:
        st.info("No hay avisos de mantenimiento registrados aún.")
        return
    
    # Aplicar filtros
    if busqueda:
        mask = (df['codigo_padre'].str.contains(busqueda, case=False, na=False)) | \
               (df['codigo_mantto'].str.contains(busqueda, case=False, na=False)) | \
               (df['equipo'].str.contains(busqueda, case=False, na=False)) | \
               (df['descripcion_problema'].str.contains(busqueda, case=False, na=False))
        df = df[mask]
    
    if estado_filtro != "Todos":
        df = df[df['estado'] == estado_filtro]
    
    if area_filtro != "Todas":
        df = df[df['area'] == area_filtro]
    
    # Mostrar tabla de avisos
    st.dataframe(
        df[['codigo_mantto', 'codigo_padre', 'estado', 'area', 'equipo', 'ingresado_por', 'ingresado_el', 'antiguedad', 'creado_en']],
        use_container_width=True,
        column_config={
            "codigo_mantto": "Código Aviso",
            "codigo_padre": "Código Padre",
            "estado": "Estado",
            "area": "Área",
            "equipo": "Equipo",
            "ingresado_por": "Ingresado por",
            "ingresado_el": "Fecha Ingreso",
            "antiguedad": "Antigüedad (días)",
            "creado_en": "Creado en"
        }
    )
    
    # Estadísticas rápidas
    st.subheader("📈 Estadísticas de Avisos")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Avisos", len(df))
    with col2:
        st.metric("Avisos Ingresados", len(df[df['estado'] == 'INGRESADO']))
    with col3:
        st.metric("Con Riesgo", len(df[df['hay_riesgo'] == 'SI']))
    with col4:
        avg_antiguedad = df['antiguedad'].mean() if not df.empty else 0
        st.metric("Antigüedad Promedio", f"{avg_antiguedad:.1f} días")
    
    return df

def gestion_avisos():
    """Función principal para la gestión de avisos de mantenimiento"""

    # Verificar permisos de escritura
    permisos = st.session_state.get('permisos', {})
    puede_crear = permisos.get('puede_crear', False)
    puede_editar = permisos.get('puede_editar', False)
    puede_eliminar = permisos.get('puede_eliminar', False)
    
    st.title("📝 Avisos de Mantenimiento")
    
    # Pestañas para diferentes funcionalidades
    tab1, tab2 = st.tabs([
        "➕ Crear Nuevo Aviso", 
        "📋 Lista de Avisos"
    ])
    
    with tab1:
        mostrar_formulario_avisos()
    
    with tab2:
        mostrar_lista_avisos()

# ===============================FUNCIONES PARA ÓRDENES DE TRABAJO================================

def generar_codigo_padre_ot_directa():
    """Genera código padre automático para OT directa con formato CODP-OT-00000001"""
    conn = conn_ot_unicas
    c = conn.cursor()
    
    # Obtener el último código padre de OT directas (que empiecen con CODP-OT-)
    c.execute("SELECT codigo_padre FROM ot_unicas WHERE codigo_padre LIKE 'CODP-OT-%' ORDER BY id DESC LIMIT 1")
    ultimo_codigo = c.fetchone()
    
    if ultimo_codigo and ultimo_codigo[0]:
        # Extraer el número y incrementar
        try:
            numero = int(ultimo_codigo[0].split('-')[2])
            nuevo_numero = numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    
    return f"CODP-OT-{nuevo_numero:08d}"

def generar_codigo_ot_base():
    """Genera código OT base automático con formato OT-0000001"""
    conn = conn_ot_unicas
    c = conn.cursor()
    
    # Obtener el último código OT base
    c.execute('SELECT codigo_ot_base FROM ot_unicas ORDER BY id DESC LIMIT 1')
    ultimo_codigo = c.fetchone()
    
    if ultimo_codigo and ultimo_codigo[0]:
        # Extraer el número y incrementar
        try:
            numero = int(ultimo_codigo[0].split('-')[1])
            nuevo_numero = numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    
    return f"OT-{nuevo_numero:07d}"

def obtener_avisos_ingresados():
    """Obtener lista de avisos con estado INGRESADO"""
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_padre,
                codigo_mantto,
                area,
                equipo,
                codigo_equipo,
                descripcion_problema,
                tipo_mantenimiento,
                ingresado_por,
                ingresado_el,
                hay_riesgo
            FROM avisos 
            WHERE estado = 'INGRESADO'
            ORDER BY creado_en DESC
        ''', conn_avisos)
        return df
    except Exception as e:
        st.error(f"Error al cargar avisos ingresados: {e}")
        return pd.DataFrame()

def calcular_antiguedad_ot(fecha_creacion):
    """Calcula la antigüedad en días desde la fecha de creación de la OT"""
    if not fecha_creacion:
        return 0
    hoy = date.today()
    if isinstance(fecha_creacion, str):
        fecha_creacion = datetime.strptime(fecha_creacion, '%Y-%m-%d').date()
    dias = (hoy - fecha_creacion).days
    return max(0, dias)

def mostrar_formulario_ot_directa():
    """Muestra el formulario para crear órdenes de trabajo directas"""
    st.header("⚡ Crear Orden de Trabajo Directa")
    
    # Generar códigos automáticos
    codigo_padre = generar_codigo_padre()  # Usar el mismo formato que avisos
    codigo_ot_base = generar_codigo_ot_base()
    estado_ot = "PROGRAMADO"
    fecha_actual = datetime.now()
    fecha_ingreso_auto = date.today()  # Fecha automática para ingresado_el
    
    st.subheader("🏭 Información del Equipo")
    
    # Área (selectbox con áreas de equipos) - FUERA DEL FORMULARIO
    areas = obtener_areas_equipos()
    if not areas:
        st.error("No hay áreas disponibles. Primero debe registrar equipos en la sección de Gestión de Equipos.")
        return
        
    area_seleccionada = st.selectbox("Área *", options=areas, key="area_ot_directa")
    
    # Equipo (dependiente del área seleccionada) - FUERA DEL FORMULARIO
    equipos_df = obtener_equipos_por_area(area_seleccionada)
    
    if equipos_df.empty:
        st.error(f"No hay equipos registrados en el área '{area_seleccionada}'.")
        return
        
    equipos_lista = equipos_df['equipo'].tolist()
    equipo_seleccionado = st.selectbox("Equipo *", options=equipos_lista, key="equipo_ot_directa")
    
    # Código del equipo (automático) - FUERA DEL FORMULARIO
    codigo_equipo_auto = obtener_codigo_equipo_por_nombre(equipo_seleccionado, area_seleccionada)
    codigo_equipo = st.text_input("Código del Equipo *", value=codigo_equipo_auto, disabled=True, key="codigo_equipo_ot_directa")
    
    st.subheader("🔩 Tipo de Mantenimiento")
    
    col_mant1, col_mant2 = st.columns(2)
    
    with col_mant1:
        # Tipo de mantenimiento - FUERA DEL FORMULARIO
        tipo_mantenimiento = st.selectbox(
            "Tipo de Mantenimiento *",
            options=[
                "MANTENIMIENTO CORRECTIVO", 
                "MANTENIMIENTO PREVENTIVO", 
                "MANTENIMIENTO PREDICTIVO", 
                "MANTENIMIENTO CORRECTIVO DE EMERGENCIA"
            ],
            help="Seleccione el tipo de mantenimiento a realizar",
            key="tipo_mant_ot_directa"
        )
    
    with col_mant2:
        # Tipo preventivo (solo se habilita si es MANTENIMIENTO PREVENTIVO) - FUERA DEL FORMULARIO
        if tipo_mantenimiento == "MANTENIMIENTO PREVENTIVO":
            tipo_preventivo = st.selectbox(
                "Tipo de Preventivo *",
                options=[
                    "CAMBIO DE COMPONENTE/REPUESTO",
                    "INSPECCION",
                    "LUBRICACION", 
                    "LIMPIEZA",
                    "AJUSTE"
                ],
                help="Seleccione el tipo de mantenimiento preventivo",
                key="tipo_prev_ot_directa"
            )
        else:
            tipo_preventivo = "NO APLICA"
            st.text_input("Tipo de Preventivo", value="NO APLICA", disabled=True, key="tipo_prev_na_ot_directa")
    
    st.markdown("---")
    
    # Mostrar códigos automáticos - FUERA DEL FORMULARIO
    st.subheader("📄 Información de la OT")
    col_codigos1, col_codigos2, col_codigos3 = st.columns(3)
    with col_codigos1:
        st.info(f"**Código Padre:** {codigo_padre}")
    with col_codigos2:
        st.info(f"**Código OT:** {codigo_ot_base}")
    with col_codigos3:
        st.info(f"**Fecha de Ingreso:** {fecha_ingreso_auto}")
    
    # FORMULARIO PRINCIPAL (solo campos que necesitan ser enviados juntos)
    with st.form("formulario_ot_directa", clear_on_submit=True):
        st.subheader("🔧 Descripción del Trabajo")
        
        # Componentes
        componentes = st.text_area(
            "Componentes *",
            placeholder="Describa los componentes involucrados en el trabajo...",
            height=80,
            help="Lista o descripción de componentes a intervenir"
        )
        
        # Descripción del problema (opcional)
        descripcion_problema = st.text_area(
            "Descripción del Problema (Opcional)",
            placeholder="Describa el problema o falla encontrada (si aplica)...",
            height=80,
            help="Opcional: describa el problema específico"
        )
        
        st.subheader("👤 Información de Ingreso")
        
        col_ingreso1, col_ingreso2 = st.columns(2)
        
        with col_ingreso1:
            # Ingresado por
            ingresado_por = st.text_input("Ingresado por *", placeholder="Su nombre o usuario")
            
            # Mostrar fecha de ingreso automática (solo lectura)
            st.text_input("Fecha de Ingreso *", value=fecha_ingreso_auto.strftime("%Y-%m-%d"), disabled=True)
        
        with col_ingreso2:
            # ¿Hay riesgo?
            hay_riesgo = st.selectbox("¿Hay riesgo? *", options=["NO", "SI"], 
                                    help="¿Existe algún riesgo para la seguridad o el medio ambiente?")
        
        st.subheader("🎯 Prioridad y Planificación")
        
        col_plan1, col_plan2 = st.columns(2)
        
        with col_plan1:
            # Prioridad
            prioridad_nueva = st.selectbox(
                "Prioridad *", 
                options=["1. ALTO", "2. MEDIO", "3. BAJO"],
                help="Seleccione el nivel de prioridad para esta OT"
            )
            
            # Fecha estimada de inicio
            fecha_estimada_inicio = st.date_input(
                "Fecha Estimada de Inicio *",
                min_value=date.today(),
                help="Fecha planificada para comenzar el trabajo"
            )
        
        with col_plan2:
            # Duración estimada
            duracion_estimada = st.text_input(
                "Duración Estimada (hh:mm:ss) *",
                placeholder="Ej: 02:30:00",
                help="Duración estimada del trabajo en formato horas:minutos:segundos"
            )
            
            # Responsable
            responsable = st.selectbox(
                "Responsable *",
                options=["SUPERVISOR MECANICO", "SUPERVISOR ELECTRICO"],
                help="Seleccione el responsable de la OT"
            )
        
        st.subheader("🔩 Recursos y Materiales")
        
        col_recursos1, col_recursos2 = st.columns(2)
        
        with col_recursos1:
            # Personal requerido
            st.write("**Personal Requerido:**")
            cantidad_mecanicos = st.number_input("Mecánicos", min_value=0, value=0, step=1, key="mec_directa")
            cantidad_electricos = st.number_input("Eléctricos", min_value=0, value=0, step=1, key="elec_directa")
            cantidad_soldadores = st.number_input("Soldadores", min_value=0, value=0, step=1, key="sold_directa")
        
        with col_recursos2:
            # Personal requerido (continuación)
            st.write("**Personal Requerido (cont.):**")
            cantidad_op_vahos = st.number_input("Operadores Vahos", min_value=0, value=0, step=1, key="vahos_directa")
            cantidad_calderistas = st.number_input("Calderistas", min_value=0, value=0, step=1, key="calder_directa")
            
            # Clasificación
            clasificacion = st.selectbox(
                "Clasificación *",
                options=["EQUIPO", "INFRAESTRUCTURA"],
                help="Seleccione la clasificación del trabajo",
                key="clasif_directa"
            )
        
        st.subheader("📋 Descripción Técnica")
        
        # Descripción del trabajo
        descripcion_trabajo = st.text_area(
            "DESCRIPCIÓN DE TRABAJO A REALIZAR *",
            placeholder="Describa detalladamente el trabajo a realizar...",
            height=100,
            help="Descripción completa de las actividades planificadas"
        )
        
        st.subheader("📦 Materiales y Proveedores")
        
        col_mat1, col_mat2 = st.columns(2)
        
        with col_mat1:
            # Sistema
            sistema = st.selectbox(
                "Sistema *",
                options=["SISTEMA MECANICO", "SISTEMA ELECTRICO", "SISTEMA HIDRAULICO", 
                        "SISTEMA NEUMATICO", "SISTEMA ELECTRONICO", "SISTEMA CONTROL"],
                help="Seleccione el sistema principal involucrado",
                key="sistema_directa"
            )
            
            # Materiales
            materiales = st.text_area(
                "Materiales Requeridos",
                placeholder="Lista de materiales necesarios...",
                height=60,
                help="Materiales requeridos para el trabajo",
                key="materiales_directa"
            )
        
        with col_mat2:
            # Alimentador/Proveedor
            alimentador_proveedor = st.selectbox(
                "Alimentador/Proveedor *",
                options=["TECNICO", "TERCERO"],
                help="Seleccione la fuente de los materiales",
                key="alimentador_directa"
            )
        
        # Validación y envío
        st.markdown("**Campos obligatorios ***")
        
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        with col_btn2:
            submitted = st.form_submit_button("🚀 Crear Orden de Trabajo Directa", use_container_width=True)
        
        if submitted:
            # Validar campos obligatorios
            campos_obligatorios = [
                componentes, ingresado_por, prioridad_nueva,
                fecha_estimada_inicio, duracion_estimada, responsable, clasificacion,
                sistema, alimentador_proveedor, descripcion_trabajo
            ]
            
            if not all(campos_obligatorios):
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            # Validar formato de duración
            if not validar_formato_duracion(duracion_estimada):
                st.error("❌ Formato de duración inválido. Use el formato hh:mm:ss (ej: 02:30:00)")
                return
            
            try:
                # Calcular antigüedad
                antiguedad_dias = calcular_antiguedad_ot(fecha_actual.date())
                
                # INSERTAR EN OT_UNICAS (sin codigo_mantto ya que es OT directa)
                c_ot = conn_ot_unicas.cursor()
                c_ot.execute('''
                    INSERT INTO ot_unicas 
                    (codigo_padre, codigo_ot_base, ot_base_creado_en,
                     estado, antiguedad, prioridad_nueva, area, equipo, codigo_equipo,
                     componentes, descripcion_problema, tipo_mantenimiento, tipo_preventivo,
                     cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                     cantidad_op_vahos, cantidad_calderistas, descripcion_trabajo,
                     responsable, clasificacion, sistema, materiales, alimentador_proveedor,
                     fecha_estimada_inicio, duracion_estimada, ingresado_por, ingresado_el, hay_riesgo)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    codigo_padre, codigo_ot_base, fecha_actual,
                    estado_ot, antiguedad_dias, prioridad_nueva, area_seleccionada, equipo_seleccionado,
                    codigo_equipo, componentes, descripcion_problema, tipo_mantenimiento, tipo_preventivo,
                    cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                    cantidad_op_vahos, cantidad_calderistas, descripcion_trabajo,
                    responsable, clasificacion, sistema, materiales, alimentador_proveedor,
                    fecha_estimada_inicio, duracion_estimada, ingresado_por, fecha_ingreso_auto, hay_riesgo
                ))
                
                conn_ot_unicas.commit()
                
                st.success(f"✅ Orden de Trabajo Directa '{codigo_ot_base}' creada exitosamente!")
                st.balloons()
                
                # Mostrar resumen
                with st.expander("📋 Ver resumen de la OT Directa creada", expanded=True):
                    col_res1, col_res2 = st.columns(2)
                    with col_res1:
                        st.write(f"**Código OT:** {codigo_ot_base}")
                        st.write(f"**Código Padre:** {codigo_padre}")
                        st.write(f"**Estado:** {estado_ot}")
                        st.write(f"**Prioridad:** {prioridad_nueva}")
                        st.write(f"**Área:** {area_seleccionada}")
                        st.write(f"**Equipo:** {equipo_seleccionado}")
                        st.write(f"**Código Equipo:** {codigo_equipo}")
                        st.write(f"**Tipo Mantenimiento:** {tipo_mantenimiento}")
                        if tipo_mantenimiento == "MANTENIMIENTO PREVENTIVO":
                            st.write(f"**Tipo Preventivo:** {tipo_preventivo}")
                    
                    with col_res2:
                        st.write(f"**Ingresado por:** {ingresado_por}")
                        st.write(f"**Fecha Ingreso:** {fecha_ingreso_auto}")
                        st.write(f"**¿Hay riesgo?:** {hay_riesgo}")
                        st.write(f"**Responsable:** {responsable}")
                        st.write(f"**Clasificación:** {clasificacion}")
                        st.write(f"**Sistema:** {sistema}")
                        st.write(f"**Alimentador/Proveedor:** {alimentador_proveedor}")
                        st.write(f"**Fecha Estimada Inicio:** {fecha_estimada_inicio}")
                        st.write(f"**Duración Estimada:** {duracion_estimada}")
                    
                    st.write("**Componentes:**")
                    st.info(componentes)
                    
                    if descripcion_problema:
                        st.write("**Descripción del Problema:**")
                        st.info(descripcion_problema)
                    
                    st.write("**Descripción de Trabajo:**")
                    st.info(descripcion_trabajo)
                    
                    if materiales:
                        st.write("**Materiales:**")
                        st.info(materiales)
                    
                    # Resumen de personal
                    st.write("**Personal Requerido:**")
                    personal_data = {
                        "Mecánicos": cantidad_mecanicos,
                        "Eléctricos": cantidad_electricos,
                        "Soldadores": cantidad_soldadores,
                        "Operadores Vahos": cantidad_op_vahos,
                        "Calderistas": cantidad_calderistas
                    }
                    personal_df = pd.DataFrame(list(personal_data.items()), columns=["Cargo", "Cantidad"])
                    st.dataframe(personal_df, use_container_width=True, hide_index=True)
                
            except Exception as e:
                st.error(f"❌ Error al crear la orden de trabajo directa: {str(e)}")

def mostrar_formulario_ot():
    """Muestra el formulario para crear órdenes de trabajo a partir de avisos"""
    st.header("📋 Crear Orden de Trabajo desde Aviso")
    
    # Pestañas para las dos funcionalidades
    tab1, tab2 = st.tabs(["➕ Crear Nueva OT desde Aviso", "🔗 Asociar Avisos a OT Existente"])
    
    with tab1:
        mostrar_crear_nueva_ot_desde_aviso()
    
    with tab2:
        mostrar_asociar_avisos_ot_existente()

def mostrar_crear_nueva_ot_desde_aviso():
    """Muestra el formulario para crear una nueva OT desde un aviso"""
    st.subheader("🆕 Crear Nueva OT desde Aviso")
    
    # Obtener avisos en estado INGRESADO
    avisos_df = obtener_avisos_ingresados()
    
    if avisos_df.empty:
        st.info("No hay avisos en estado 'INGRESADO' disponibles para crear órdenes de trabajo.")
        return
    
    # Seleccionar aviso para transformar en OT
    st.write("📝 Seleccionar Aviso para OT")
    
    avisos_lista = [f"{row['codigo_mantto']} - {row['equipo']} ({row['area']}) - {row['descripcion_problema'][:50]}..." 
                   for _, row in avisos_df.iterrows()]
    
    aviso_seleccionado = st.selectbox("Seleccionar Aviso *", options=avisos_lista, key="aviso_ot_nuevo")
    
    if not aviso_seleccionado:
        return
    
    # Obtener datos del aviso seleccionado
    codigo_mantto_seleccionado = aviso_seleccionado.split(' - ')[0]
    aviso_data = avisos_df[avisos_df['codigo_mantto'] == codigo_mantto_seleccionado].iloc[0]
    
    # Mostrar información del aviso seleccionado
    st.subheader("📄 Información del Aviso Seleccionado")
    
    col_info1, col_info2 = st.columns(2)
    with col_info1:
        st.info(f"**Código Mantenimiento:** {aviso_data['codigo_mantto']}")
        st.info(f"**Código Padre:** {aviso_data['codigo_padre']}")
        st.info(f"**Área:** {aviso_data['area']}")
        st.info(f"**Equipo:** {aviso_data['equipo']}")
    
    with col_info2:
        st.info(f"**Código Equipo:** {aviso_data['codigo_equipo']}")
        st.info(f"**Tipo Mantenimiento:** {aviso_data['tipo_mantenimiento']}")
        st.info(f"**Ingresado por:** {aviso_data['ingresado_por']}")
        st.info(f"**¿Hay riesgo?:** {aviso_data['hay_riesgo']}")
    
    st.info(f"**Descripción del Problema:** {aviso_data['descripcion_problema']}")
    
    st.markdown("---")
    
    # FORMULARIO PARA DATOS DE LA OT
    with st.form("formulario_ot_nuevo", clear_on_submit=True):
        st.subheader("🔧 Información de la Orden de Trabajo")
        
        # Generar código OT automático
        codigo_ot_base = generar_codigo_ot_base()
        estado_ot = "PROGRAMADO"
        fecha_actual = datetime.now()
        
        # Mostrar información automática
        col_auto1, col_auto2 = st.columns(2)
        with col_auto1:
            st.info(f"**Código OT:** {codigo_ot_base}")
            st.info(f"**Estado OT:** {estado_ot}")
        with col_auto2:
            st.info(f"**Fecha Creación:** {fecha_actual.strftime('%Y-%m-%d')}")
            st.info(f"**Código Padre:** {aviso_data['codigo_padre']}")
        
        st.subheader("🎯 Prioridad y Planificación")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Prioridad
            prioridad_nueva = st.selectbox(
                "Prioridad *", 
                options=["1. ALTO", "2. MEDIO", "3. BAJO"],
                help="Seleccione el nivel de prioridad para esta OT",
                key="prioridad_nuevo"
            )
            
            # Fecha estimada de inicio
            fecha_estimada_inicio = st.date_input(
                "Fecha Estimada de Inicio *",
                min_value=date.today(),
                help="Fecha planificada para comenzar el trabajo",
                key="fecha_estimada_nuevo"
            )
        
        with col2:
            # Duración estimada
            duracion_estimada = st.text_input(
                "Duración Estimada (hh:mm:ss) *",
                placeholder="Ej: 02:30:00",
                help="Duración estimada del trabajo en formato horas:minutos:segundos",
                key="duracion_nuevo"
            )
            
            # Responsable
            responsable = st.selectbox(
                "Responsable *",
                options=["SUPERVISOR MECANICO", "SUPERVISOR ELECTRICO"],
                help="Seleccione el responsable de la OT",
                key="responsable_nuevo"
            )
        
        st.subheader("🔩 Recursos y Materiales")
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Personal requerido
            st.write("**Personal Requerido:**")
            cantidad_mecanicos = st.number_input("Mecánicos", min_value=0, value=0, step=1, key="mecanicos_nuevo")
            cantidad_electricos = st.number_input("Eléctricos", min_value=0, value=0, step=1, key="electricos_nuevo")
            cantidad_soldadores = st.number_input("Soldadores", min_value=0, value=0, step=1, key="soldadores_nuevo")
        
        with col4:
            # Personal requerido (continuación)
            st.write("**Personal Requerido (cont.):**")
            cantidad_op_vahos = st.number_input("Operadores Vahos", min_value=0, value=0, step=1, key="vahos_nuevo")
            cantidad_calderistas = st.number_input("Calderistas", min_value=0, value=0, step=1, key="calderistas_nuevo")
            
            # Clasificación
            clasificacion = st.selectbox(
                "Clasificación *",
                options=["EQUIPO", "INFRAESTRUCTURA"],
                help="Seleccione la clasificación del trabajo",
                key="clasificacion_nuevo"
            )
        
        st.subheader("📋 Descripción Técnica")
        
        # Componentes
        componentes = st.text_area(
            "Componentes *",
            placeholder="Describa los componentes involucrados en el trabajo...",
            height=80,
            help="Lista o descripción de componentes a intervenir",
            key="componentes_nuevo"
        )
        
        # Descripción del trabajo
        descripcion_trabajo = st.text_area(
            "DESCRIPCIÓN DE TRABAJO A REALIZAR *",
            placeholder="Describa detalladamente el trabajo a realizar...",
            height=100,
            help="Descripción completa de las actividades planificadas",
            key="desc_trabajo_nuevo"
        )
        
        st.subheader("📦 Materiales y Proveedores")
        
        col5, col6 = st.columns(2)
        
        with col5:
            # Sistema
            sistema = st.selectbox(
                "Sistema *",
                options=["SISTEMA MECANICO", "SISTEMA ELECTRICO", "SISTEMA HIDRAULICO", 
                        "SISTEMA NEUMATICO", "SISTEMA ELECTRONICO", "SISTEMA CONTROL"],
                help="Seleccione el sistema principal involucrado",
                key="sistema_nuevo"
            )
            
            # Materiales
            materiales = st.text_area(
                "Materiales Requeridos",
                placeholder="Lista de materiales necesarios...",
                height=60,
                help="Materiales requeridos para el trabajo",
                key="materiales_nuevo"
            )
        
        with col6:
            # Alimentador/Proveedor
            alimentador_proveedor = st.selectbox(
                "Alimentador/Proveedor *",
                options=["TECNICO", "TERCERO"],
                help="Seleccione la fuente de los materiales",
                key="alimentador_nuevo"
            )
        
        # Validación y envío
        st.markdown("**Campos obligatorios ***")
        
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        with col_btn2:
            submitted = st.form_submit_button("🚀 Crear Nueva Orden de Trabajo", use_container_width=True)
        
        if submitted:
            # Validar campos obligatorios
            campos_obligatorios = [
                prioridad_nueva, componentes, descripcion_trabajo, responsable,
                clasificacion, sistema, alimentador_proveedor, fecha_estimada_inicio, duracion_estimada
            ]
            
            if not all(campos_obligatorios):
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            # Validar formato de duración
            if not validar_formato_duracion(duracion_estimada):
                st.error("❌ Formato de duración inválido. Use el formato hh:mm:ss (ej: 02:30:00)")
                return
            
            try:
                # Calcular antigüedad
                antiguedad_dias = calcular_antiguedad_ot(fecha_actual.date())
                
                # ACTUALIZAR AVISO (cambiar estado de INGRESADO a PROGRAMADO)
                c_avisos = conn_avisos.cursor()
                c_avisos.execute('''
                    UPDATE avisos 
                    SET estado = 'PROGRAMADO', 
                        codigo_ot_base = ?,
                        componentes = ?,
                        cantidad_mecanicos = ?,
                        cantidad_electricos = ?,
                        cantidad_soldadores = ?,
                        cantidad_op_vahos = ?,
                        cantidad_calderistas = ?,
                        descripcion_trabajo = ?,
                        responsable = ?,
                        clasificacion = ?,
                        sistema = ?,
                        materiales = ?,
                        alimentador_proveedor = ?,
                        fecha_estimada_inicio = ?,
                        duracion_estimada = ?,
                        prioridad = ?
                    WHERE codigo_mantto = ?
                ''', (
                    codigo_ot_base, componentes, cantidad_mecanicos, cantidad_electricos,
                    cantidad_soldadores, cantidad_op_vahos, cantidad_calderistas,
                    descripcion_trabajo, responsable, clasificacion, sistema,
                    materiales, alimentador_proveedor, fecha_estimada_inicio,
                    duracion_estimada, prioridad_nueva, codigo_mantto_seleccionado
                ))
                
                # INSERTAR EN OT_UNICAS
                c_ot = conn_ot_unicas.cursor()
                c_ot.execute('''
                    INSERT INTO ot_unicas 
                    (codigo_padre, codigo_mantto, codigo_ot_base, ot_base_creado_en,
                     estado, antiguedad, prioridad_nueva, area, equipo, codigo_equipo,
                     componentes, descripcion_problema, tipo_mantenimiento,
                     cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                     cantidad_op_vahos, cantidad_calderistas, descripcion_trabajo,
                     responsable, clasificacion, sistema, materiales, alimentador_proveedor,
                     fecha_estimada_inicio, duracion_estimada)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    aviso_data['codigo_padre'], aviso_data['codigo_mantto'], codigo_ot_base, fecha_actual,
                    estado_ot, antiguedad_dias, prioridad_nueva, aviso_data['area'], aviso_data['equipo'],
                    aviso_data['codigo_equipo'], componentes, aviso_data['descripcion_problema'],
                    aviso_data['tipo_mantenimiento'], cantidad_mecanicos, cantidad_electricos,
                    cantidad_soldadores, cantidad_op_vahos, cantidad_calderistas, descripcion_trabajo,
                    responsable, clasificacion, sistema, materiales, alimentador_proveedor,
                    fecha_estimada_inicio, duracion_estimada
                ))
                
                conn_avisos.commit()
                conn_ot_unicas.commit()
                
                st.success(f"✅ Orden de Trabajo '{codigo_ot_base}' creada exitosamente!")
                st.success(f"✅ Aviso '{codigo_mantto_seleccionado}' actualizado a estado PROGRAMADO")
                st.balloons()
                
                # Mostrar resumen
                with st.expander("📋 Ver resumen de la OT creada", expanded=True):
                    col_res1, col_res2 = st.columns(2)
                    with col_res1:
                        st.write(f"**Código OT:** {codigo_ot_base}")
                        st.write(f"**Código Mantenimiento:** {aviso_data['codigo_mantto']}")
                        st.write(f"**Código Padre:** {aviso_data['codigo_padre']}")
                        st.write(f"**Estado:** {estado_ot}")
                        st.write(f"**Prioridad:** {prioridad_nueva}")
                        st.write(f"**Área:** {aviso_data['area']}")
                        st.write(f"**Equipo:** {aviso_data['equipo']}")
                    
                    with col_res2:
                        st.write(f"**Responsable:** {responsable}")
                        st.write(f"**Clasificación:** {clasificacion}")
                        st.write(f"**Sistema:** {sistema}")
                        st.write(f"**Alimentador/Proveedor:** {alimentador_proveedor}")
                        st.write(f"**Fecha Estimada Inicio:** {fecha_estimada_inicio}")
                        st.write(f"**Duración Estimada:** {duracion_estimada}")
                    
                    st.write("**Componentes:**")
                    st.info(componentes)
                    
                    st.write("**Descripción de Trabajo:**")
                    st.info(descripcion_trabajo)
                    
                    if materiales:
                        st.write("**Materiales:**")
                        st.info(materiales)
                
            except Exception as e:
                st.error(f"❌ Error al crear la orden de trabajo: {str(e)}")

def mostrar_asociar_avisos_ot_existente():
    """Muestra la funcionalidad para asociar múltiples avisos a una OT existente"""
    st.subheader("🔗 Asociar Avisos a OT Existente")
    
    # Obtener OTs en estado PROGRAMADO
    ot_programadas_df = obtener_ot_programadas()
    
    if ot_programadas_df.empty:
        st.info("No hay Órdenes de Trabajo en estado 'PROGRAMADO' disponibles.")
        return
    
    # Seleccionar OT existente
    st.write("📋 Seleccionar OT Existente")
    
    ot_lista = [f"{row['codigo_ot_base']} - {row['equipo']} ({row['area']}) - {row['prioridad_nueva']}" 
               for _, row in ot_programadas_df.iterrows()]
    
    ot_seleccionada = st.selectbox("Seleccionar OT *", options=ot_lista, key="ot_existente")
    
    if not ot_seleccionada:
        return
    
    # Obtener datos de la OT seleccionada
    codigo_ot_seleccionado = ot_seleccionada.split(' - ')[0]
    ot_data = ot_programadas_df[ot_programadas_df['codigo_ot_base'] == codigo_ot_seleccionado].iloc[0]
    
    # Mostrar información de la OT seleccionada
    st.subheader("📄 Información de la OT Seleccionada")
    
    col_ot1, col_ot2 = st.columns(2)
    with col_ot1:
        st.info(f"**Código OT:** {ot_data['codigo_ot_base']}")
        st.info(f"**Área:** {ot_data['area']}")
        st.info(f"**Equipo:** {ot_data['equipo']}")
        st.info(f"**Prioridad:** {ot_data['prioridad_nueva']}")
    
    with col_ot2:
        st.info(f"**Responsable:** {ot_data['responsable']}")
        st.info(f"**Clasificación:** {ot_data['clasificacion']}")
        st.info(f"**Sistema:** {ot_data['sistema']}")
        st.info(f"**Fecha Estimada Inicio:** {ot_data['fecha_estimada_inicio']}")
    
    # Obtener avisos en estado INGRESADO que coincidan en área y equipo
    avisos_compatibles_df = obtener_avisos_compatibles(ot_data['area'], ot_data['equipo'])
    
    if avisos_compatibles_df.empty:
        st.info(f"No hay avisos en estado 'INGRESADO' que coincidan con el área '{ot_data['area']}' y equipo '{ot_data['equipo']}'.")
        return
    
    st.subheader("📝 Avisos Compatibles para Asociar")
    st.write(f"**Se encontraron {len(avisos_compatibles_df)} avisos compatibles:**")
    
    # Mostrar tabla de avisos compatibles
    st.dataframe(
        avisos_compatibles_df[['codigo_mantto', 'codigo_padre', 'descripcion_problema', 'ingresado_por', 'ingresado_el']],
        use_container_width=True,
        column_config={
            "codigo_mantto": "Código Aviso",
            "codigo_padre": "Código Padre",
            "descripcion_problema": "Descripción Problema",
            "ingresado_por": "Ingresado por",
            "ingresado_el": "Fecha Ingreso"
        }
    )
    
    # Seleccionar avisos para asociar
    st.subheader("🎯 Seleccionar Avisos para Asociar")
    
    avisos_opciones = [f"{row['codigo_mantto']} - {row['descripcion_problema'][:50]}..." 
                      for _, row in avisos_compatibles_df.iterrows()]
    
    avisos_seleccionados = st.multiselect(
        "Seleccionar avisos para asociar a la OT:",
        options=avisos_opciones,
        default=avisos_opciones,  # Seleccionar todos por defecto
        help="Seleccione los avisos que desea asociar a esta OT"
    )
    
    if st.button("🔗 Asociar Avisos Seleccionados a OT", type="primary", use_container_width=True):
        if not avisos_seleccionados:
            st.error("Por favor, seleccione al menos un aviso para asociar.")
            return
        
        try:
            # Extraer códigos de mantenimiento de los avisos seleccionados
            codigos_mantto_seleccionados = [aviso.split(' - ')[0] for aviso in avisos_seleccionados]
            
            # Actualizar los avisos seleccionados
            c_avisos = conn_avisos.cursor()
            avisos_actualizados = 0
            
            for codigo_mantto in codigos_mantto_seleccionados:
                c_avisos.execute('''
                    UPDATE avisos 
                    SET estado = 'PROGRAMADO', 
                        codigo_ot_base = ?
                    WHERE codigo_mantto = ? AND estado = 'INGRESADO'
                ''', (codigo_ot_seleccionado, codigo_mantto))
                
                if c_avisos.rowcount > 0:
                    avisos_actualizados += 1
            
            conn_avisos.commit()
            
            st.success(f"✅ {avisos_actualizados} avisos asociados exitosamente a la OT '{codigo_ot_seleccionado}'!")
            st.success(f"✅ Los avisos han sido actualizados a estado PROGRAMADO")
            st.balloons()
            
            # Mostrar resumen
            with st.expander("📋 Ver resumen de la asociación", expanded=True):
                st.write(f"**OT Asociada:** {codigo_ot_seleccionado}")
                st.write(f"**Avisos Actualizados:** {avisos_actualizados}")
                
                st.write("**Avisos Asociados:**")
                for i, aviso in enumerate(avisos_seleccionados, 1):
                    st.write(f"{i}. {aviso}")
                
                # Mostrar avisos que no se pudieron actualizar
                if avisos_actualizados < len(codigos_mantto_seleccionados):
                    st.warning(f"⚠️ {len(codigos_mantto_seleccionados) - avisos_actualizados} avisos no pudieron ser actualizados (posiblemente ya estaban asociados a otra OT).")
        
        except Exception as e:
            st.error(f"❌ Error al asociar los avisos a la OT: {str(e)}")

def obtener_ot_programadas():
    """Obtener lista de OTs en estado PROGRAMADO"""
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_ot_base,
                codigo_padre,
                area,
                equipo,
                codigo_equipo,
                prioridad_nueva,
                responsable,
                clasificacion,
                sistema,
                fecha_estimada_inicio,
                duracion_estimada
            FROM ot_unicas 
            WHERE estado = 'PROGRAMADO'
            ORDER BY ot_base_creado_en DESC
        ''', conn_ot_unicas)
        return df
    except Exception as e:
        st.error(f"Error al cargar OTs programadas: {e}")
        return pd.DataFrame()

def obtener_avisos_compatibles(area, equipo):
    """Obtener avisos en estado INGRESADO que coincidan con área y equipo específicos"""
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_padre,
                codigo_mantto,
                area,
                equipo,
                codigo_equipo,
                descripcion_problema,
                tipo_mantenimiento,
                ingresado_por,
                ingresado_el,
                hay_riesgo
            FROM avisos 
            WHERE estado = 'INGRESADO' 
            AND area = ? 
            AND equipo = ?
            ORDER BY creado_en DESC
        ''', conn_avisos, params=(area, equipo))
        return df
    except Exception as e:
        st.error(f"Error al cargar avisos compatibles: {e}")
        return pd.DataFrame()

def validar_formato_duracion(duracion):
    """Valida el formato de duración hh:mm:ss"""
    import re
    patron = r'^([0-9]{2}):([0-9]{2}):([0-9]{2})$'
    return bool(re.match(patron, duracion))

def obtener_lista_ot():
    """Obtener lista de todas las órdenes de trabajo"""
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_ot_base,
                codigo_padre,
                codigo_mantto,
                estado,
                antiguedad,
                prioridad_nueva,
                area,
                equipo,
                responsable,
                clasificacion,
                sistema,
                fecha_estimada_inicio,
                duracion_estimada,
                ot_base_creado_en
            FROM ot_unicas 
            ORDER BY ot_base_creado_en DESC
        ''', conn_ot_unicas)
        return df
    except Exception as e:
        st.error(f"Error al cargar órdenes de trabajo: {e}")
        return pd.DataFrame()

def mostrar_lista_ot():
    """Muestra la lista de órdenes de trabajo existentes"""
    st.subheader("📊 Órdenes de Trabajo Registradas")
    
    # Filtros de búsqueda
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        busqueda = st.text_input("🔍 Buscar OT", placeholder="Buscar por código, equipo o descripción...", key="busqueda_ot")
    with col2:
        estados = ["Todos", "PROGRAMADO", "PENDIENTE", "CULMINADO", "CERRADO", "ANULADO"]
        estado_filtro = st.selectbox("Filtrar por estado", estados, key="estado_ot")
    with col3:
        prioridades = ["Todas", "1. ALTO", "2. MEDIO", "3. BAJO"]
        prioridad_filtro = st.selectbox("Filtrar por prioridad", prioridades, key="prioridad_ot")
    
    # Obtener todas las OT
    df = obtener_lista_ot()
    
    if df.empty:
        st.info("No hay órdenes de trabajo registradas aún.")
        return
    
    # Aplicar filtros
    if busqueda:
        mask = (df['codigo_ot_base'].str.contains(busqueda, case=False, na=False)) | \
               (df['codigo_padre'].str.contains(busqueda, case=False, na=False)) | \
               (df['codigo_mantto'].str.contains(busqueda, case=False, na=False)) | \
               (df['equipo'].str.contains(busqueda, case=False, na=False))
        df = df[mask]
    
    if estado_filtro != "Todos":
        df = df[df['estado'] == estado_filtro]
    
    if prioridad_filtro != "Todas":
        df = df[df['prioridad_nueva'] == prioridad_filtro]
    
    # Mostrar tabla de OT
    st.dataframe(
        df[['codigo_ot_base', 'codigo_mantto', 'estado', 'prioridad_nueva', 'area', 'equipo', 'responsable', 'fecha_estimada_inicio', 'ot_base_creado_en']],
        use_container_width=True,
        column_config={
            "codigo_ot_base": "Código OT",
            "codigo_mantto": "Código Aviso",
            "estado": "Estado",
            "prioridad_nueva": "Prioridad",
            "area": "Área",
            "equipo": "Equipo",
            "responsable": "Responsable",
            "fecha_estimada_inicio": "Fecha Estimada",
            "ot_base_creado_en": "Creado en"
        }
    )
    
    # Estadísticas rápidas
    st.subheader("📈 Estadísticas de OT")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total OT", len(df))
    with col2:
        st.metric("OT Programadas", len(df[df['estado'] == 'PROGRAMADO']))
    with col3:
        st.metric("Prioridad Alta", len(df[df['prioridad_nueva'] == '1. ALTO']))
    with col4:
        avg_antiguedad = df['antiguedad'].mean() if not df.empty else 0
        st.metric("Antigüedad Promedio", f"{avg_antiguedad:.1f} días")
    
    return df

def gestion_ot():
    """Función principal para la gestión de órdenes de trabajo"""
    st.title("📋 Órdenes de Trabajo")
    
    # Pestañas para diferentes funcionalidades
    tab1, tab2, tab3 = st.tabs([
        "➕ Crear OT desde Aviso", 
        "⚡ Crear OT Directa",
        "📋 Lista de Órdenes de Trabajo"
    ])
    
    with tab1:
        mostrar_formulario_ot()
    
    with tab2:
        mostrar_formulario_ot_directa()
    
    with tab3:
        mostrar_lista_ot()

def mostrar_ot_pendientes():
    """Muestra reporte de Órdenes de Trabajo pendientes"""
    st.header("📋 Reporte de OT Pendientes")
    
    # Pestañas para ver reporte y para iniciar mantenimiento
    tab1, tab2 = st.tabs(["📊 Ver Reporte", "🔧 Iniciar Mantenimiento"])
    
    with tab1:
        mostrar_reporte_ot_pendientes()
    
    with tab2:
        mostrar_formulario_inicio_mantenimiento()

def mostrar_reporte_ot_pendientes():
    """Muestra el reporte de OT pendientes"""
    # Obtener OT en estados pendientes (PROGRAMADO y PENDIENTE)
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_ot_base,
                codigo_mantto,
                codigo_padre,
                estado,
                prioridad_nueva,
                area,
                equipo,
                codigo_equipo,
                responsable,
                clasificacion,
                sistema,
                fecha_estimada_inicio,
                duracion_estimada,
                antiguedad,
                ot_base_creado_en
            FROM ot_unicas 
            WHERE estado IN ('PROGRAMADO', 'PENDIENTE')
            ORDER BY 
                CASE prioridad_nueva 
                    WHEN '1. ALTO' THEN 1
                    WHEN '2. MEDIO' THEN 2
                    WHEN '3. BAJO' THEN 3
                    ELSE 4
                END,
                fecha_estimada_inicio ASC
        ''', conn_ot_unicas)
    except Exception as e:
        st.error(f"Error al cargar OT pendientes: {e}")
        return
    
    if df.empty:
        st.success("🎉 No hay Órdenes de Trabajo pendientes en este momento.")
        return
    
    # Filtros
    st.subheader("🔍 Filtros")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        estado_filtro = st.selectbox(
            "Filtrar por Estado",
            options=["Todos", "PROGRAMADO", "PENDIENTE"],
            key="estado_pendientes"
        )
    
    with col2:
        prioridad_filtro = st.selectbox(
            "Filtrar por Prioridad",
            options=["Todas", "1. ALTO", "2. MEDIO", "3. BAJO"],
            key="prioridad_pendientes"
        )
    
    with col3:
        area_filtro = st.selectbox(
            "Filtrar por Área",
            options=["Todas"] + sorted(df['area'].unique().tolist()),
            key="area_pendientes"
        )
    
    with col4:
        responsable_filtro = st.selectbox(
            "Filtrar por Responsable",
            options=["Todos"] + sorted(df['responsable'].unique().tolist()),
            key="responsable_pendientes"
        )
    
    # Aplicar filtros
    df_filtrado = df.copy()
    
    if estado_filtro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['estado'] == estado_filtro]
    
    if prioridad_filtro != "Todas":
        df_filtrado = df_filtrado[df_filtrado['prioridad_nueva'] == prioridad_filtro]
    
    if area_filtro != "Todas":
        df_filtrado = df_filtrado[df_filtrado['area'] == area_filtro]
    
    if responsable_filtro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['responsable'] == responsable_filtro]
    
    # Métricas principales
    st.subheader("📈 Métricas de OT Pendientes")
    
    col_met1, col_met2, col_met3, col_met4, col_met5 = st.columns(5)
    
    with col_met1:
        total_ot = len(df_filtrado)
        st.metric("Total OT Pendientes", total_ot)
    
    with col_met2:
        ot_programadas = len(df_filtrado[df_filtrado['estado'] == 'PROGRAMADO'])
        st.metric("OT Programadas", ot_programadas)
    
    with col_met3:
        ot_pendientes = len(df_filtrado[df_filtrado['estado'] == 'PENDIENTE'])
        st.metric("OT en Progreso", ot_pendientes)
    
    with col_met4:
        alta_prioridad = len(df_filtrado[df_filtrado['prioridad_nueva'] == '1. ALTO'])
        st.metric("Prioridad ALTA", alta_prioridad)
    
    with col_met5:
        if not df_filtrado.empty:
            avg_antiguedad = df_filtrado['antiguedad'].mean()
            st.metric("Antigüedad Promedio", f"{avg_antiguedad:.1f} días")
        else:
            st.metric("Antigüedad Promedio", "0 días")
    
    # Gráficos
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        # Distribución por estado
        if not df_filtrado.empty:
            st.subheader("📊 Distribución por Estado")
            estado_counts = df_filtrado['estado'].value_counts()
            st.bar_chart(estado_counts)
    
    with col_chart2:
        # Distribución por prioridad
        if not df_filtrado.empty:
            st.subheader("🎯 Distribución por Prioridad")
            prioridad_counts = df_filtrado['prioridad_nueva'].value_counts()
            st.bar_chart(prioridad_counts)
    
    # Tabla detallada
    st.subheader("📋 Detalle de OT Pendientes")
    
    # Mostrar tabla con columnas seleccionadas
    columnas_mostrar = [
        'codigo_ot_base', 'codigo_mantto', 'estado', 'prioridad_nueva', 
        'area', 'equipo', 'responsable', 'fecha_estimada_inicio', 
        'antiguedad', 'ot_base_creado_en'
    ]
    
    st.dataframe(
        df_filtrado[columnas_mostrar],
        use_container_width=True,
        column_config={
            "codigo_ot_base": "Código OT",
            "codigo_mantto": "Código Aviso",
            "estado": "Estado",
            "prioridad_nueva": "Prioridad",
            "area": "Área",
            "equipo": "Equipo",
            "responsable": "Responsable",
            "fecha_estimada_inicio": "Fecha Estimada",
            "antiguedad": "Antigüedad (días)",
            "ot_base_creado_en": "Creado en"
        }
    )
    
    # Botón de exportación
    if not df_filtrado.empty:
        csv = df_filtrado.to_csv(index=False)
        st.download_button(
            label="📥 Exportar a CSV",
            data=csv,
            file_name=f"ot_pendientes_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )

def obtener_ot_para_inicio():
    """Obtener OT en estado PROGRAMADO y PENDIENTE para iniciar/continuar mantenimiento"""
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_ot_base,
                codigo_padre,
                area,
                equipo,
                codigo_equipo,
                responsable,
                descripcion_trabajo,
                descripcion_trabajo_realizado,  -- AÑADIR para acumular
                observaciones_cierre,           -- AÑADIR para acumular
                fecha_estimada_inicio,
                estado                         -- AÑADIR para saber el estado actual
            FROM ot_unicas 
            WHERE estado IN ('PROGRAMADO', 'PENDIENTE')
            ORDER BY 
                CASE estado 
                    WHEN 'PENDIENTE' THEN 1  -- Primero las que ya están en progreso
                    WHEN 'PROGRAMADO' THEN 2
                    ELSE 3
                END,
                fecha_estimada_inicio ASC
        ''', conn_ot_unicas)
        return df
    except Exception as e:
        st.error(f"Error al cargar OT para inicio: {e}")
        return pd.DataFrame()

def generar_codigo_ot_sufijo(codigo_ot_base):
    """Genera código OT con sufijo basado en el código OT base"""
    try:
        c = conn_ot_sufijos.cursor()
        
        # Contar cuántos sufijos existen para esta OT base
        c.execute('''
            SELECT COUNT(*) FROM ot_sufijos 
            WHERE codigo_ot_base = ?
        ''', (codigo_ot_base,))
        
        count = c.fetchone()[0]
        nuevo_sufijo = count + 1
        
        return f"{codigo_ot_base}-{nuevo_sufijo:02d}"
        
    except Exception as e:
        st.error(f"Error al generar código OT con sufijo: {e}")
        return f"{codigo_ot_base}-01"

def mostrar_formulario_inicio_mantenimiento():
    """Muestra formulario para iniciar/continuar mantenimiento de OT"""
    st.subheader("🔧 Iniciar/Continuar Mantenimiento de OT")
    
    # Obtener OT en estado PROGRAMADO y PENDIENTE
    ot_para_inicio_df = obtener_ot_para_inicio()
    
    if ot_para_inicio_df.empty:
        st.info("ℹ️ No hay Órdenes de Trabajo en estado 'PROGRAMADO' o 'PENDIENTE' disponibles.")
        return
    
    # Seleccionar OT para iniciar/continuar mantenimiento
    st.write("📋 Seleccionar OT para Iniciar/Continuar Mantenimiento")
    
    # Mostrar estado en la lista para diferenciar
    ot_lista = []
    for _, row in ot_para_inicio_df.iterrows():
        estado_icon = "🟡" if row['estado'] == 'PENDIENTE' else "🟢"
        ot_lista.append(f"{estado_icon} {row['codigo_ot_base']} - {row['equipo']} ({row['area']}) - {row['estado']}")
    
    ot_seleccionada = st.selectbox("Seleccionar OT *", options=ot_lista, key="ot_inicio_mantenimiento")
    
    if not ot_seleccionada:
        return
    
    # Obtener datos de la OT seleccionada
    codigo_ot_base_seleccionado = ot_seleccionada.split(' - ')[0].split(' ')[1]  # Extraer código sin el icono
    ot_data = ot_para_inicio_df[ot_para_inicio_df['codigo_ot_base'] == codigo_ot_base_seleccionado].iloc[0]
    
    # Determinar si es continuación o inicio nuevo
    es_continuacion = ot_data['estado'] == 'PENDIENTE'
    
    # Mostrar información de la OT seleccionada
    st.subheader("📄 Información de la OT Seleccionada")
    
    col_info1, col_info2 = st.columns(2)
    with col_info1:
        st.info(f"**Código OT Base:** {ot_data['codigo_ot_base']}")
        st.info(f"**Código Padre:** {ot_data['codigo_padre']}")
        st.info(f"**Área:** {ot_data['area']}")
        st.info(f"**Equipo:** {ot_data['equipo']}")
        st.info(f"**Estado Actual:** {ot_data['estado']}")
    
    with col_info2:
        st.info(f"**Código Equipo:** {ot_data['codigo_equipo']}")
        st.info(f"**Responsable:** {ot_data['responsable']}")
        st.info(f"**Fecha Estimada Inicio:** {ot_data['fecha_estimada_inicio']}")
        if es_continuacion:
            st.warning("🟡 **MANTENIMIENTO EN PROGRESO**")
        else:
            st.success("🟢 **LISTA PARA INICIAR**")
    
    if ot_data['descripcion_trabajo']:
        st.info(f"**Descripción del Trabajo Planificado:** {ot_data['descripcion_trabajo']}")
    
    # Mostrar descripción acumulada anterior si existe
    if es_continuacion and pd.notna(ot_data['descripcion_trabajo_realizado']):
        with st.expander("📝 Ver trabajo realizado anteriormente", expanded=True):
            st.info(f"**Trabajo acumulado:**\n\n{ot_data['descripcion_trabajo_realizado']}")
    
    # Mostrar observaciones acumuladas anteriores si existen
    if es_continuacion and pd.notna(ot_data['observaciones_cierre']):
        with st.expander("📋 Ver observaciones anteriores", expanded=False):
            st.info(f"**Observaciones acumuladas:**\n\n{ot_data['observaciones_cierre']}")
    
    st.markdown("---")
    
    # FORMULARIO PARA INICIAR/CONTINUAR MANTENIMIENTO
    with st.form("formulario_inicio_mantenimiento"):
        if es_continuacion:
            st.subheader("🔄 Continuar Mantenimiento en Progreso")
        else:
            st.subheader("🛠️ Iniciar Nuevo Mantenimiento")
        
        # Generar código OT con sufijo (solo para nuevos inicios)
        if not es_continuacion:
            codigo_ot_sufijo = generar_codigo_ot_sufijo(codigo_ot_base_seleccionado)
        else:
            codigo_ot_sufijo = "CONTINUACIÓN"  # O podrías generar un nuevo sufijo para cada continuación
        
        estado_nuevo = "PENDIENTE"  # Siempre queda en PENDIENTE hasta que se culmine
        
        # Mostrar información automática
        col_auto1, col_auto2 = st.columns(2)
        with col_auto1:
            st.info(f"**Código OT Base:** {codigo_ot_base_seleccionado}")
            if not es_continuacion:
                st.info(f"**Código OT Sufijo:** {codigo_ot_sufijo}")
            else:
                st.info(f"**Tipo:** Continuación")
        with col_auto2:
            st.info(f"**Estado:** {estado_nuevo}")
            st.info(f"**Código Padre:** {ot_data['codigo_padre']}")
        
        st.subheader("📅 Fechas y Horarios")
        
        col_fechas1, col_fechas2, col_fechas3 = st.columns(3)
        
        with col_fechas1:
            # Fecha de inicio/continuación de mantenimiento
            fecha_inicio_mantenimiento = st.date_input(
                "Fecha Mantenimiento *",
                value=date.today(),
                help="Fecha real en que se inicia/continúa el mantenimiento"
            )
        
        with col_fechas2:
            # Hora de inicio/continuación
            hora_inicio_mantenimiento = st.time_input(
                "Hora Inicio *",
                value=datetime.now().time(),
                help="Hora exacta de inicio/continuación del mantenimiento"
            )
        
        with col_fechas3:
            # Hora de finalización estimada
            hora_finalizacion_mantenimiento = st.time_input(
                "Hora Finalización Estimada *",
                value=(datetime.now() + timedelta(hours=2)).time(),
                help="Hora estimada de finalización del mantenimiento"
            )
        
        st.subheader("👥 Personal y Trabajo Realizado")
        
        # Responsables del comienzo/continuación
        responsables_comienzo = st.text_area(
            "Responsables *",
            placeholder="Ingrese los nombres de los responsables que trabajan en el mantenimiento...",
            height=60,
            help="Lista de responsables que participan en el mantenimiento"
        )
        
        # Descripción del trabajo realizado (ACUMULATIVA)
        st.write("**Descripción del Trabajo Realizado **")
        st.caption("💡 Esta descripción se acumulará con el trabajo anterior")
        
        # Mostrar descripción anterior como placeholder si existe
        placeholder_text = ""
        if es_continuacion and pd.notna(ot_data['descripcion_trabajo_realizado']):
            placeholder_text = ot_data['descripcion_trabajo_realizado'] + "\n\n--- NUEVO TRABAJO ---\n"
        
        nueva_descripcion_trabajo = st.text_area(
            "Agregar nueva descripción del trabajo realizado:",
            placeholder=placeholder_text + "Describa el trabajo adicional que se está realizando...",
            height=120,
            help="Descripción completa de las actividades adicionales que se están ejecutando"
        )
        
        st.subheader("⚡ Impacto en Producción")
        
        col_impacto1, col_impacto2 = st.columns(2)
        
        with col_impacto1:
            # Paro de línea
            paro_linea = st.selectbox(
                "¿Tuvo que parar la línea para el mantenimiento? *",
                options=["NO", "SI"],
                help="Indique si el mantenimiento requirió detener la línea de producción"
            )
        
        with col_impacto2:
            # Duración estimada del paro (si aplica)
            if paro_linea == "SI":
                duracion_paro = st.text_input(
                    "Duración estimada del paro (hh:mm)",
                    placeholder="Ej: 01:30",
                    help="Duración estimada del paro de línea"
                )
            else:
                duracion_paro = "NO APLICA"
        
        # Observaciones de cierre (ACUMULATIVAS)
        st.write("**Observaciones Adicionales**")
        st.caption("💡 Estas observaciones se acumularán con las anteriores")
        
        # Mostrar observaciones anteriores como placeholder si existen
        obs_placeholder = ""
        if es_continuacion and pd.notna(ot_data['observaciones_cierre']):
            obs_placeholder = ot_data['observaciones_cierre'] + "\n\n--- NUEVAS OBSERVACIONES ---\n"
        
        nuevas_observaciones = st.text_area(
            "Agregar nuevas observaciones:",
            placeholder=obs_placeholder + "Ingrese cualquier observación adicional...",
            height=80,
            help="Observaciones adicionales sobre el mantenimiento"
        )
        
        # Validación y envío
        st.markdown("**Campos obligatorios ***")
        
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        with col_btn2:
            if es_continuacion:
                button_text = "🔄 Continuar Mantenimiento"
            else:
                button_text = "🚀 Iniciar Mantenimiento"
                
            submitted = st.form_submit_button(button_text, use_container_width=True)
        
        if submitted:
            # Validar campos obligatorios
            campos_obligatorios = [
                responsables_comienzo,
                nueva_descripcion_trabajo
            ]
            
            if not all(campos_obligatorios):
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            try:
                # CONSTRUIR DESCRIPCIÓN ACUMULADA
                if es_continuacion and pd.notna(ot_data['descripcion_trabajo_realizado']):
                    descripcion_acumulada = f"{ot_data['descripcion_trabajo_realizado']}\n\n--- CONTINUACIÓN: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---\n{nueva_descripcion_trabajo}"
                else:
                    descripcion_acumulada = f"--- INICIO: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---\n{nueva_descripcion_trabajo}"
                
                # CONSTRUIR OBSERVACIONES ACUMULADAS
                if es_continuacion and pd.notna(ot_data['observaciones_cierre']):
                    observaciones_acumuladas = f"{ot_data['observaciones_cierre']}\n\n--- CONTINUACIÓN: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---\n{nuevas_observaciones}"
                else:
                    observaciones_acumuladas = nuevas_observaciones if nuevas_observaciones else None
                
                # 1. ACTUALIZAR OT_UNICAS (mantener estado PENDIENTE y acumular datos)
                c_ot_unicas = conn_ot_unicas.cursor()
                c_ot_unicas.execute('''
                    UPDATE ot_unicas 
                    SET estado = ?,
                        fecha_inicio_mantenimiento = COALESCE(fecha_inicio_mantenimiento, ?),
                        hora_inicio_mantenimiento = COALESCE(hora_inicio_mantenimiento, ?),
                        hora_finalizacion_mantenimiento = ?,
                        responsables_comienzo = ?,
                        descripcion_trabajo_realizado = ?,
                        observaciones_cierre = ?,
                        paro_linea = ?
                    WHERE codigo_ot_base = ?
                ''', (
                    estado_nuevo, 
                    fecha_inicio_mantenimiento, 
                    hora_inicio_mantenimiento.strftime('%H:%M:%S'),
                    hora_finalizacion_mantenimiento.strftime('%H:%M:%S'), 
                    responsables_comienzo,
                    descripcion_acumulada,
                    observaciones_acumuladas,
                    paro_linea, 
                    codigo_ot_base_seleccionado
                ))
                
                # 2. INSERTAR EN OT_SUFIJOS (solo para nuevos inicios)
                if not es_continuacion:
                    c_ot_sufijos = conn_ot_sufijos.cursor()
                    c_ot_sufijos.execute('''
                        INSERT INTO ot_sufijos 
                        (codigo_padre, codigo_mantto, codigo_ot_base, codigo_ot_sufijo,
                         ot_sufijo_creado_en, estado, area, equipo, codigo_equipo,
                         responsables_comienzo, fecha_inicio_mantenimiento, 
                         hora_inicio_mantenimiento, hora_finalizacion_mantenimiento,
                         descripcion_trabajo_realizado, paro_linea, observaciones_cierre)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        ot_data['codigo_padre'], None, codigo_ot_base_seleccionado, codigo_ot_sufijo,
                        datetime.now(), estado_nuevo, ot_data['area'], ot_data['equipo'], ot_data['codigo_equipo'],
                        responsables_comienzo, fecha_inicio_mantenimiento,
                        hora_inicio_mantenimiento.strftime('%H:%M:%S'), hora_finalizacion_mantenimiento.strftime('%H:%M:%S'),
                        descripcion_acumulada, paro_linea, observaciones_acumuladas
                    ))
                    conn_ot_sufijos.commit()
                
                # Confirmar transacciones
                conn_ot_unicas.commit()
                
                st.success(f"✅ {'Continuación' if es_continuacion else 'Inicio'} de mantenimiento exitoso para la OT '{codigo_ot_base_seleccionado}'!")
                st.success(f"✅ Estado actualizado a 'PENDIENTE'")
                if not es_continuacion:
                    st.success(f"✅ Código OT con sufijo creado: '{codigo_ot_sufijo}'")
                st.balloons()
                
                # Mostrar resumen
                with st.expander("📋 Ver resumen del mantenimiento", expanded=True):
                    col_res1, col_res2 = st.columns(2)
                    with col_res1:
                        st.write(f"**Código OT Base:** {codigo_ot_base_seleccionado}")
                        if not es_continuacion:
                            st.write(f"**Código OT Sufijo:** {codigo_ot_sufijo}")
                        st.write(f"**Tipo:** {'Continuación' if es_continuacion else 'Nuevo Inicio'}")
                        st.write(f"**Estado:** {estado_nuevo}")
                        st.write(f"**Código Padre:** {ot_data['codigo_padre']}")
                        st.write(f"**Área:** {ot_data['area']}")
                        st.write(f"**Equipo:** {ot_data['equipo']}")
                    
                    with col_res2:
                        st.write(f"**Fecha:** {fecha_inicio_mantenimiento}")
                        st.write(f"**Hora Inicio:** {hora_inicio_mantenimiento.strftime('%H:%M:%S')}")
                        st.write(f"**Hora Finalización:** {hora_finalizacion_mantenimiento.strftime('%H:%M:%S')}")
                        st.write(f"**Paro de Línea:** {paro_linea}")
                        if paro_linea == "SI" and duracion_paro != "NO APLICA":
                            st.write(f"**Duración Paro:** {duracion_paro}")
                    
                    st.write("**Responsables:**")
                    st.info(responsables_comienzo)
                    
                    st.write("**Descripción del Trabajo (Acumulada):**")
                    st.info(descripcion_acumulada)
                    
                    if observaciones_acumuladas:
                        st.write("**Observaciones (Acumuladas):**")
                        st.info(observaciones_acumuladas)
                
            except Exception as e:
                st.error(f"❌ Error al {'continuar' if es_continuacion else 'iniciar'} el mantenimiento: {str(e)}")

#========================OT CULMIANADAS==============================
def obtener_ot_para_culminacion():
    """Obtener OT en estado PROGRAMADO y PENDIENTE para culminar"""
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_ot_base,
                codigo_padre,
                codigo_mantto,
                area,
                equipo,
                codigo_equipo,
                responsable,
                descripcion_trabajo,
                descripcion_trabajo_realizado,
                fecha_inicio_mantenimiento,
                hora_inicio_mantenimiento,
                responsables_comienzo,
                estado
            FROM ot_unicas 
            WHERE estado IN ('PROGRAMADO', 'PENDIENTE')
            ORDER BY 
                CASE estado 
                    WHEN 'PENDIENTE' THEN 1
                    WHEN 'PROGRAMADO' THEN 2
                    ELSE 3
                END,
                fecha_inicio_mantenimiento ASC
        ''', conn_ot_unicas)
        
        # Convertir fecha_inicio_mantenimiento a datetime si existe
        if not df.empty and 'fecha_inicio_mantenimiento' in df.columns:
            df['fecha_inicio_mantenimiento'] = pd.to_datetime(df['fecha_inicio_mantenimiento'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Error al cargar OT para culminación: {e}")
        return pd.DataFrame()

def mostrar_formulario_culminacion_ot():
    """Muestra formulario para culminar OT pendientes"""
    st.subheader("✅ Culminar Orden de Trabajo")
    
    # Obtener OT en estado PROGRAMADO y PENDIENTE
    ot_para_culminar_df = obtener_ot_para_culminacion()
    
    if ot_para_culminar_df.empty:
        st.info("ℹ️ No hay Órdenes de Trabajo en estado 'PROGRAMADO' o 'PENDIENTE' disponibles para culminar.")
        return
    
    # Seleccionar OT para culminar
    st.write("📋 Seleccionar OT para Culminar")
    
    ot_lista = []
    for _, row in ot_para_culminar_df.iterrows():
        estado_icon = "🟡" if row['estado'] == 'PENDIENTE' else "🟢"
        ot_lista.append(f"{estado_icon} {row['codigo_ot_base']} - {row['equipo']} ({row['area']}) - {row['estado']}")
    
    ot_seleccionada = st.selectbox("Seleccionar OT *", options=ot_lista, key="ot_culminacion")
    
    if not ot_seleccionada:
        return
    
    # Obtener datos de la OT seleccionada
    codigo_ot_base_seleccionado = ot_seleccionada.split(' - ')[0].split(' ')[1]
    ot_data = ot_para_culminar_df[ot_para_culminar_df['codigo_ot_base'] == codigo_ot_base_seleccionado].iloc[0]
    
    # Mostrar información de la OT seleccionada
    st.subheader("📄 Información de la OT Seleccionada")
    
    col_info1, col_info2 = st.columns(2)
    with col_info1:
        st.info(f"**Código OT Base:** {ot_data['codigo_ot_base']}")
        if pd.notna(ot_data['codigo_mantto']):
            st.info(f"**Código Mantto:** {ot_data['codigo_mantto']}")
        st.info(f"**Código Padre:** {ot_data['codigo_padre']}")
        st.info(f"**Área:** {ot_data['area']}")
        st.info(f"**Equipo:** {ot_data['equipo']}")
    
    with col_info2:
        st.info(f"**Código Equipo:** {ot_data['codigo_equipo']}")
        st.info(f"**Responsable:** {ot_data['responsable']}")
        st.info(f"**Estado Actual:** {ot_data['estado']}")
        if pd.notna(ot_data['fecha_inicio_mantenimiento']):
            # Convertir a date para mostrar correctamente
            fecha_inicio = ot_data['fecha_inicio_mantenimiento'].date() if hasattr(ot_data['fecha_inicio_mantenimiento'], 'date') else ot_data['fecha_inicio_mantenimiento']
            st.info(f"**Fecha Inicio:** {fecha_inicio}")
            if pd.notna(ot_data['hora_inicio_mantenimiento']):
                st.info(f"**Hora Inicio:** {ot_data['hora_inicio_mantenimiento']}")
    
    if ot_data['descripcion_trabajo']:
        st.info(f"**Descripción del Trabajo Planificado:** {ot_data['descripcion_trabajo']}")
    
    # Mostrar trabajo realizado anterior si existe
    if pd.notna(ot_data['descripcion_trabajo_realizado']):
        with st.expander("📝 Ver trabajo realizado anteriormente", expanded=True):
            st.info(f"**Trabajo acumulado:**\n\n{ot_data['descripcion_trabajo_realizado']}")
    
    st.markdown("---")
    
    # FORMULARIO PARA CULMINAR OT
    with st.form("formulario_culminacion_ot"):
        st.subheader("✅ Datos de Culminación")
        
        estado_nuevo = "CULMINADO"
        
        # Mostrar información automática
        col_auto1, col_auto2 = st.columns(2)
        with col_auto1:
            st.info(f"**Código OT Base:** {ot_data['codigo_ot_base']}")
            st.info(f"**Estado Nuevo:** {estado_nuevo}")
        with col_auto2:
            st.info(f"**Código Padre:** {ot_data['codigo_padre']}")
            st.info(f"**Área:** {ot_data['area']}")
        
        st.subheader("📅 Fechas y Horarios de Culminación")
        
        col_fechas1, col_fechas2, col_fechas3 = st.columns(3)
        
        with col_fechas1:
            # Fecha de finalización - CORREGIDO: usar value=date.today() directamente
            fecha_finalizacion = st.date_input(
                "Fecha Finalización *",
                value=date.today(),  # Valor por defecto seguro
                help="Fecha real en que se culmina el mantenimiento"
            )
        
        with col_fechas2:
            # Hora de finalización
            hora_final = st.time_input(
                "Hora Finalización *",
                value=datetime.now().time(),
                help="Hora exacta de finalización del mantenimiento"
            )
        
        with col_fechas3:
            # Hora de inicio (SOLO para ot_sufijos, no se comparte con otras bases)
            st.write("**Hora Inicio (Registro)**")
            hora_inicio_mantenimiento = st.time_input(
                "Hora Inicio Real *",
                value=datetime.now().time() if pd.isna(ot_data['hora_inicio_mantenimiento']) else 
                     datetime.strptime(ot_data['hora_inicio_mantenimiento'], '%H:%M:%S').time() if isinstance(ot_data['hora_inicio_mantenimiento'], str) else
                     datetime.now().time(),
                help="Hora real en que inició el mantenimiento (solo para registro en OT sufijos)",
                key="hora_inicio_culminacion"
            )
        
        st.subheader("👥 Personal y Trabajo Finalizado")
        
        # Responsables de la finalización
        responsables_finalizacion = st.text_area(
            "Responsables de la Finalización *",
            placeholder="Ingrese los nombres de los responsables que culminaron el mantenimiento...",
            height=60,
            help="Lista de responsables que participaron en la culminación"
        )
        
        # Descripción del trabajo realizado final (ACUMULATIVA)
        st.write("**Descripción Final del Trabajo Realizado **")
        st.caption("💡 Esta descripción se acumulará con el trabajo anterior")
        
        placeholder_text = ""
        if pd.notna(ot_data['descripcion_trabajo_realizado']):
            placeholder_text = ot_data['descripcion_trabajo_realizado'] + "\n\n--- CULMINACIÓN ---\n"
        
        descripcion_final_trabajo = st.text_area(
            "Agregar descripción final del trabajo realizado:",
            placeholder=placeholder_text + "Describa el trabajo final realizado y los resultados...",
            height=120,
            help="Descripción completa de las actividades finales realizadas y resultados obtenidos"
        )
        
        st.subheader("📷 Evidencia Fotográfica Final")
        
        # Imagen final del trabajo
        imagen_final = st.file_uploader(
            "Subir imagen del trabajo culminado (Opcional)",
            type=['png', 'jpg', 'jpeg'],
            help="Foto que muestre el trabajo finalizado (opcional)"
        )
        
        st.subheader("📝 Comentarios y Observaciones Finales")
        
        # Comentario adicional
        comentario = st.text_area(
            "Comentarios Adicionales (Opcional)",
            placeholder="Ingrese cualquier comentario adicional sobre la culminación...",
            height=80,
            help="Comentarios opcionales sobre el trabajo culminado"
        )
        
        # Observaciones de cierre finales
        observaciones_cierre = st.text_area(
            "Observaciones Finales de Cierre (Opcional)",
            placeholder="Observaciones finales sobre el cierre del trabajo...",
            height=80,
            help="Observaciones finales de cierre"
        )
        
        # Validación y envío
        st.markdown("**Campos obligatorios ***")
        
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        with col_btn2:
            submitted = st.form_submit_button("✅ Culminar Orden de Trabajo", use_container_width=True)
        
        if submitted:
            # Validar campos obligatorios
            campos_obligatorios = [
                responsables_finalizacion,
                descripcion_final_trabajo
            ]
            
            if not all(campos_obligatorios):
                st.error("Por favor, complete todos los campos obligatorios (*)")
                return
            
            try:
                # Procesar imagen final si se subió
                imagen_final_nombre = None
                imagen_final_datos = None
                
                if imagen_final is not None:
                    imagen_final_nombre = imagen_final.name
                    imagen_final_datos = imagen_final.getvalue()
                
                # CONSTRUIR DESCRIPCIÓN ACUMULADA FINAL
                if pd.notna(ot_data['descripcion_trabajo_realizado']):
                    descripcion_acumulada_final = f"{ot_data['descripcion_trabajo_realizado']}\n\n--- CULMINACIÓN: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---\n{descripcion_final_trabajo}"
                else:
                    descripcion_acumulada_final = f"--- CULMINACIÓN: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---\n{descripcion_final_trabajo}"
                
                # 1. ACTUALIZAR OT_UNICAS (cambiar estado a CULMINADO y acumular descripción)
                c_ot_unicas = conn_ot_unicas.cursor()
                c_ot_unicas.execute('''
                    UPDATE ot_unicas 
                    SET estado = ?,
                        fecha_finalizacion = ?,
                        hora_final = ?,
                        responsables_finalizacion = ?,
                        descripcion_trabajo_realizado = ?,
                        imagen_final_nombre = ?,
                        imagen_final_datos = ?,
                        observaciones_cierre = ?,
                        comentario = ?
                    WHERE codigo_ot_base = ?
                ''', (
                    estado_nuevo,
                    fecha_finalizacion,
                    hora_final.strftime('%H:%M:%S'),
                    responsables_finalizacion,
                    descripcion_acumulada_final,  # Solo se acumula este campo
                    imagen_final_nombre,
                    imagen_final_datos,
                    observaciones_cierre,
                    comentario,
                    codigo_ot_base_seleccionado
                ))
                
                # 2. ACTUALIZAR AVISOS (cambiar estado a CULMINADO)
                c_avisos = conn_avisos.cursor()
                c_avisos.execute('''
                    UPDATE avisos 
                    SET estado = ?,
                        fecha_finalizacion = ?,
                        hora_final = ?,
                        responsables_finalizacion = ?,
                        descripcion_trabajo_realizado = ?,
                        imagen_final_nombre = ?,
                        imagen_final_datos = ?,
                        observaciones_cierre = ?,
                        comentario = ?
                    WHERE codigo_padre = ?
                ''', (
                    estado_nuevo,
                    fecha_finalizacion,
                    hora_final.strftime('%H:%M:%S'),
                    responsables_finalizacion,
                    descripcion_final_trabajo,  # No acumular en avisos
                    imagen_final_nombre,
                    imagen_final_datos,
                    observaciones_cierre,
                    comentario,
                    ot_data['codigo_padre']
                ))
                
                # 3. INSERTAR EN OT_SUFIJOS (registro completo de la culminación CON HORA INICIO)
                c_ot_sufijos = conn_ot_sufijos.cursor()
                c_ot_sufijos.execute('''
                    INSERT INTO ot_sufijos 
                    (codigo_padre, codigo_mantto, codigo_ot_base, codigo_ot_sufijo,
                     ot_sufijo_creado_en, estado, area, equipo, codigo_equipo,
                     fecha_inicio_mantenimiento, hora_inicio_mantenimiento,  -- SOLO en ot_sufijos
                     fecha_finalizacion, hora_final, responsables_finalizacion,
                     descripcion_trabajo_realizado, imagen_final_nombre, imagen_final_datos,
                     observaciones_cierre, comentario)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ot_data['codigo_padre'],
                    ot_data['codigo_mantto'],
                    codigo_ot_base_seleccionado,
                    f"{codigo_ot_base_seleccionado}-CULM",
                    datetime.now(),
                    estado_nuevo,
                    ot_data['area'],
                    ot_data['equipo'],
                    ot_data['codigo_equipo'],
                    fecha_finalizacion,  # Usamos fecha_finalizacion también como fecha_inicio para el registro
                    hora_inicio_mantenimiento.strftime('%H:%M:%S'),  # HORA INICIO SOLO EN OT_SUFIJOS
                    fecha_finalizacion,
                    hora_final.strftime('%H:%M:%S'),
                    responsables_finalizacion,
                    descripcion_final_trabajo,  # Descripción final sin acumular
                    imagen_final_nombre,
                    imagen_final_datos,
                    observaciones_cierre,
                    comentario
                ))
                
                # Confirmar todas las transacciones
                conn_ot_unicas.commit()
                conn_avisos.commit()
                conn_ot_sufijos.commit()
                
                st.success(f"✅ Orden de Trabajo '{codigo_ot_base_seleccionado}' culminada exitosamente!")
                st.success(f"✅ Estado actualizado a 'CULMINADO' en todas las bases de datos")
                st.balloons()
                
                # Mostrar resumen
                with st.expander("📋 Ver resumen de la culminación", expanded=True):
                    col_res1, col_res2 = st.columns(2)
                    with col_res1:
                        st.write(f"**Código OT Base:** {codigo_ot_base_seleccionado}")
                        st.write(f"**Código Padre:** {ot_data['codigo_padre']}")
                        st.write(f"**Estado:** {estado_nuevo}")
                        st.write(f"**Área:** {ot_data['area']}")
                        st.write(f"**Equipo:** {ot_data['equipo']}")
                        st.write(f"**Fecha Finalización:** {fecha_finalizacion}")
                    
                    with col_res2:
                        st.write(f"**Hora Inicio (registro):** {hora_inicio_mantenimiento.strftime('%H:%M:%S')}")
                        st.write(f"**Hora Finalización:** {hora_final.strftime('%H:%M:%S')}")
                        if imagen_final_nombre:
                            st.write(f"**Imagen Final:** {imagen_final_nombre}")
                    
                    st.write("**Responsables de la Finalización:**")
                    st.info(responsables_finalizacion)
                    
                    st.write("**Descripción Final del Trabajo:**")
                    st.info(descripcion_final_trabajo)
                    
                    if comentario:
                        st.write("**Comentario:**")
                        st.info(comentario)
                    
                    if observaciones_cierre:
                        st.write("**Observaciones de Cierre:**")
                        st.info(observaciones_cierre)
                
            except Exception as e:
                st.error(f"❌ Error al culminar la orden de trabajo: {str(e)}")

def mostrar_ot_culminadas():
    """Muestra reporte de Órdenes de Trabajo culminadas"""
    st.header("✅ Reporte de OT Culminadas")
    
    # Pestañas para ver reporte y para culminar OT
    tab1, tab2 = st.tabs(["📊 Ver Reporte", "✅ Culminar OT"])
    
    with tab1:
        mostrar_reporte_ot_culminadas()
    
    with tab2:
        mostrar_formulario_culminacion_ot()

def mostrar_reporte_ot_culminadas():
    """Muestra el reporte de OT culminadas"""
    # Obtener OT en estado CULMINADO y CERRADO
    try:
        df = pd.read_sql('''
            SELECT 
                codigo_ot_base,
                codigo_mantto,
                codigo_padre,
                estado,
                prioridad_nueva,
                area,
                equipo,
                codigo_equipo,
                responsable,
                clasificacion,
                sistema,
                fecha_estimada_inicio,
                fecha_inicio_mantenimiento,
                fecha_finalizacion,
                hora_final,
                responsables_finalizacion,
                duracion_estimada,
                descripcion_trabajo_realizado,
                observaciones_cierre,
                comentario,
                antiguedad,
                ot_base_creado_en
            FROM ot_unicas 
            WHERE estado IN ('CULMINADO', 'CERRADO')
            ORDER BY fecha_finalizacion DESC
        ''', conn_ot_unicas)
    except Exception as e:
        st.error(f"Error al cargar OT culminadas: {e}")
        return
    
    if df.empty:
        st.info("ℹ️ No hay Órdenes de Trabajo culminadas para mostrar.")
        return
    
    # Filtros
    st.subheader("🔍 Filtros")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        estado_filtro = st.selectbox(
            "Filtrar por Estado",
            options=["Todos", "CULMINADO", "CERRADO"],
            key="estado_culminadas"
        )
    
    with col2:
        prioridad_filtro = st.selectbox(
            "Filtrar por Prioridad",
            options=["Todas", "1. ALTO", "2. MEDIO", "3. BAJO"],
            key="prioridad_culminadas"
        )
    
    with col3:
        area_filtro = st.selectbox(
            "Filtrar por Área",
            options=["Todas"] + sorted(df['area'].unique().tolist()),
            key="area_culminadas"
        )
    
    with col4:
        # Filtro por fecha desde - CORREGIDO: usar date.today() directamente
        fecha_inicio = st.date_input(
            "Fecha desde",
            value=date.today() - timedelta(days=30),  # Últimos 30 días por defecto
            key="fecha_desde_culminadas"
        )
    
    # Aplicar filtros
    df_filtrado = df.copy()
    
    if estado_filtro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['estado'] == estado_filtro]
    
    if prioridad_filtro != "Todas":
        df_filtrado = df_filtrado[df_filtrado['prioridad_nueva'] == prioridad_filtro]
    
    if area_filtro != "Todas":
        df_filtrado = df_filtrado[df_filtrado['area'] == area_filtro]
    
    # Filtrar por fecha de finalización
    if not df_filtrado.empty and 'fecha_finalizacion' in df_filtrado.columns:
        df_filtrado['fecha_finalizacion'] = pd.to_datetime(df_filtrado['fecha_finalizacion'], errors='coerce')
        df_filtrado = df_filtrado[df_filtrado['fecha_finalizacion'] >= pd.to_datetime(fecha_inicio)]
    
    # Métricas principales
    st.subheader("📈 Métricas de OT Culminadas")
    
    col_met1, col_met2, col_met3, col_met4, col_met5 = st.columns(5)
    
    with col_met1:
        total_ot = len(df_filtrado)
        st.metric("Total OT Culminadas", total_ot)
    
    with col_met2:
        ot_culminadas = len(df_filtrado[df_filtrado['estado'] == 'CULMINADO'])
        st.metric("OT Culminadas", ot_culminadas)
    
    with col_met3:
        ot_cerradas = len(df_filtrado[df_filtrado['estado'] == 'CERRADO'])
        st.metric("OT Cerradas", ot_cerradas)
    
    with col_met4:
        if not df_filtrado.empty:
            avg_antiguedad = df_filtrado['antiguedad'].mean()
            st.metric("Antigüedad Promedio", f"{avg_antiguedad:.1f} días")
        else:
            st.metric("Antigüedad Promedio", "0 días")
    
    with col_met5:
        if not df_filtrado.empty:
            # Calcular tiempo promedio de culminación
            df_filtrado['dias_culminacion'] = (df_filtrado['fecha_finalizacion'] - pd.to_datetime(df_filtrado['fecha_estimada_inicio'])).dt.days
            avg_dias_culminacion = df_filtrado['dias_culminacion'].mean()
            st.metric("Días Promedio", f"{avg_dias_culminacion:.1f} días")
        else:
            st.metric("Días Promedio", "0 días")
    
    # Gráficos
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        # Distribución por estado
        if not df_filtrado.empty:
            st.subheader("📊 Distribución por Estado")
            estado_counts = df_filtrado['estado'].value_counts()
            st.bar_chart(estado_counts)
    
    with col_chart2:
        # OT culminadas por mes
        if not df_filtrado.empty:
            st.subheader("📅 OT Culminadas por Mes")
            df_filtrado['mes_culminacion'] = df_filtrado['fecha_finalizacion'].dt.to_period('M').astype(str)
            mes_counts = df_filtrado['mes_culminacion'].value_counts().sort_index()
            st.bar_chart(mes_counts)
    
    # Tabla detallada
    st.subheader("📋 Detalle de OT Culminadas")
    
    # Mostrar tabla con columnas seleccionadas
    columnas_mostrar = [
        'codigo_ot_base', 'codigo_mantto', 'estado', 'prioridad_nueva', 
        'area', 'equipo', 'responsable', 'fecha_estimada_inicio',
        'fecha_finalizacion', 'antiguedad'
    ]
    
    st.dataframe(
        df_filtrado[columnas_mostrar],
        use_container_width=True,
        column_config={
            "codigo_ot_base": "Código OT",
            "codigo_mantto": "Código Aviso",
            "estado": "Estado",
            "prioridad_nueva": "Prioridad",
            "area": "Área",
            "equipo": "Equipo",
            "responsable": "Responsable",
            "fecha_estimada_inicio": "Fecha Estimada",
            "fecha_finalizacion": "Fecha Finalización",
            "antiguedad": "Antigüedad (días)"
        }
    )
    
    # Detalles expandibles para cada OT
    if not df_filtrado.empty:
        st.subheader("📝 Detalles Adicionales")
        
        for _, ot in df_filtrado.iterrows():
            with st.expander(f"🔍 {ot['codigo_ot_base']} - {ot['equipo']} ({ot['area']})"):
                col_det1, col_det2 = st.columns(2)
                
                with col_det1:
                    st.write(f"**Código Padre:** {ot['codigo_padre']}")
                    st.write(f"**Código Equipo:** {ot['codigo_equipo']}")
                    st.write(f"**Clasificación:** {ot['clasificacion']}")
                    st.write(f"**Sistema:** {ot['sistema']}")
                    if pd.notna(ot['fecha_inicio_mantenimiento']):
                        st.write(f"**Fecha Inicio Mantenimiento:** {ot['fecha_inicio_mantenimiento']}")
                
                with col_det2:
                    st.write(f"**Duración Estimada:** {ot['duracion_estimada']}")
                    st.write(f"**Creado en:** {ot['ot_base_creado_en']}")
                    if pd.notna(ot['dias_culminacion']):
                        st.write(f"**Días para Culminar:** {ot['dias_culminacion']} días")
                
                if pd.notna(ot['descripcion_trabajo_realizado']):
                    st.write("**Descripción del Trabajo Realizado:**")
                    st.info(ot['descripcion_trabajo_realizado'])
                
                if pd.notna(ot['observaciones_cierre']):
                    st.write("**Observaciones de Cierre:**")
                    st.info(ot['observaciones_cierre'])
    
    # Botón de exportación
    if not df_filtrado.empty:
        csv = df_filtrado.to_csv(index=False)
        st.download_button(
            label="📥 Exportar a CSV",
            data=csv,
            file_name=f"ot_culminadas_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )

# ===============================VISUALIZACIÓN Y EXPORTACIÓN DE BASES DE DATOS================================

def mostrar_visualizacion_bases_datos():
    """Muestra una interfaz para visualizar y exportar todas las bases de datos"""
    
    permisos = st.session_state.get('permisos', {})
    puede_descargar_excel = permisos.get('puede_descargar_excel', False)
    
    st.title("📊 Visualización y Exportación de Bases de Datos")
    
    # Pestañas para cada base de datos
    tab_names = ["📝 Avisos", "📋 OT Únicas", "🔢 OT Sufijos", "🏭 Equipos", "👥 Colaboradores"]
    
    if puede_descargar_excel:
        tab_names.append("📁 Exportación Masiva")
    
    tabs = st.tabs(tab_names)
    
    # Contenido de las pestañas
    with tabs[0]:
        mostrar_base_avisos()
    with tabs[1]:
        mostrar_base_ot_unicas()
    with tabs[2]:
        mostrar_base_ot_sufijos()
    with tabs[3]:
        mostrar_base_equipos()
    with tabs[4]:
        mostrar_base_colaboradores()
    if puede_descargar_excel and len(tabs) > 5:
        with tabs[5]:
            mostrar_exportacion_masiva()

def mostrar_base_avisos():
    """Muestra y permite exportar la base de datos de avisos"""
    st.subheader("📝 Base de Datos: Avisos de Mantenimiento")
    
    try:
        # Obtener todos los avisos
        df = pd.read_sql('''
            SELECT 
                id, codigo_padre, codigo_mantto, codigo_ot_base, estado,
                antiguedad, area, equipo, codigo_equipo, componentes,
                descripcion_problema, ingresado_por, ingresado_el, hay_riesgo,
                tipo_mantenimiento, tipo_preventivo, prioridad, fecha_programada,
                creado_en
            FROM avisos 
            ORDER BY creado_en DESC
        ''', conn_avisos)
        
        if df.empty:
            st.info("No hay avisos registrados en la base de datos.")
            return
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            estado_filtro = st.selectbox(
                "Filtrar por estado",
                ["Todos"] + sorted(df['estado'].unique().tolist()),
                key="filtro_estado_avisos"
            )
        with col2:
            area_filtro = st.selectbox(
                "Filtrar por área",
                ["Todas"] + sorted(df['area'].unique().tolist()),
                key="filtro_area_avisos"
            )
        with col3:
            busqueda = st.text_input("🔍 Buscar...", key="busqueda_avisos")
        
        # Aplicar filtros
        df_filtrado = df.copy()
        if estado_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['estado'] == estado_filtro]
        if area_filtro != "Todas":
            df_filtrado = df_filtrado[df_filtrado['area'] == area_filtro]
        if busqueda:
            mask = (
                df_filtrado['codigo_padre'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['codigo_mantto'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['equipo'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['descripcion_problema'].str.contains(busqueda, case=False, na=False)
            )
            df_filtrado = df_filtrado[mask]
        
        # Mostrar estadísticas
        col_met1, col_met2, col_met3, col_met4 = st.columns(4)
        with col_met1:
            st.metric("Total Avisos", len(df_filtrado))
        with col_met2:
            st.metric("Estados Diferentes", df_filtrado['estado'].nunique())
        with col_met3:
            st.metric("Áreas Diferentes", df_filtrado['area'].nunique())
        with col_met4:
            st.metric("Con OT Asignada", df_filtrado['codigo_ot_base'].notna().sum())
        
        # Mostrar tabla
        st.dataframe(df_filtrado, use_container_width=True)
        
        # Obtener avisos con imágenes para visualización
        avisos_con_imagen = pd.read_sql('''
            SELECT codigo_mantto, imagen_aviso_nombre, imagen_aviso_datos 
            FROM avisos 
            WHERE imagen_aviso_datos IS NOT NULL
        ''', conn_avisos)
        
        # Visualización de imágenes
        if not avisos_con_imagen.empty:
            st.subheader("🖼️ Visualización de Imágenes de Avisos")
            aviso_seleccionado = st.selectbox(
                "Seleccionar aviso para ver imagen:",
                [f"{row['codigo_mantto']} - {row['imagen_aviso_nombre']}" for _, row in avisos_con_imagen.iterrows()],
                key="imagen_avisos"
            )
            
            if aviso_seleccionado:
                codigo_mantto = aviso_seleccionado.split(' - ')[0]
                imagen_data = avisos_con_imagen[avisos_con_imagen['codigo_mantto'] == codigo_mantto].iloc[0]
                
                if imagen_data['imagen_aviso_datos']:
                    st.image(imagen_data['imagen_aviso_datos'], caption=f"Imagen: {imagen_data['imagen_aviso_nombre']}", use_column_width=True)
        
        # Exportar a Excel (solo si tiene permiso)
        permisos = st.session_state.get('permisos', {})
        puede_descargar_excel = permisos.get('puede_descargar_excel', False)
        
        if not df_filtrado.empty and puede_descargar_excel:
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_filtrado.to_excel(writer, sheet_name='Avisos', index=False)
            
            st.download_button(
                label="📥 Exportar a Excel",
                data=excel_buffer.getvalue(),
                file_name=f"avisos_mantenimiento_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        elif not df_filtrado.empty and not puede_descargar_excel:
            st.info("ℹ️ No tiene permisos para exportar datos a Excel")
        
    except Exception as e:
        st.error(f"Error al cargar la base de datos de avisos: {e}")

def mostrar_base_ot_unicas():
    """Muestra y permite exportar la base de datos de OT únicas"""
    st.subheader("📋 Base de Datos: Órdenes de Trabajo Únicas")
    
    try:
        # Obtener todas las OT únicas
        df = pd.read_sql('''
            SELECT 
                id, codigo_padre, codigo_mantto, codigo_ot_base, estado,
                antiguedad, prioridad_nueva, area, equipo, codigo_equipo,
                componentes, descripcion_problema, descripcion_trabajo,
                responsable, clasificacion, sistema, materiales,
                fecha_estimada_inicio, duracion_estimada,
                fecha_inicio_mantenimiento, fecha_finalizacion,
                creado_en
            FROM ot_unicas 
            ORDER BY ot_base_creado_en DESC
        ''', conn_ot_unicas)
        
        if df.empty:
            st.info("No hay órdenes de trabajo únicas registradas.")
            return
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            estado_filtro = st.selectbox(
                "Filtrar por estado",
                ["Todos"] + sorted(df['estado'].unique().tolist()),
                key="filtro_estado_ot_unicas"
            )
        with col2:
            prioridad_filtro = st.selectbox(
                "Filtrar por prioridad",
                ["Todas"] + sorted(df['prioridad_nueva'].unique().tolist()),
                key="filtro_prioridad_ot_unicas"
            )
        with col3:
            busqueda = st.text_input("🔍 Buscar...", key="busqueda_ot_unicas")
        
        # Aplicar filtros
        df_filtrado = df.copy()
        if estado_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['estado'] == estado_filtro]
        if prioridad_filtro != "Todas":
            df_filtrado = df_filtrado[df_filtrado['prioridad_nueva'] == prioridad_filtro]
        if busqueda:
            mask = (
                df_filtrado['codigo_ot_base'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['codigo_mantto'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['equipo'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['descripcion_trabajo'].str.contains(busqueda, case=False, na=False)
            )
            df_filtrado = df_filtrado[mask]
        
        # Mostrar estadísticas
        col_met1, col_met2, col_met3, col_met4 = st.columns(4)
        with col_met1:
            st.metric("Total OT", len(df_filtrado))
        with col_met2:
            st.metric("OT Activas", len(df_filtrado[df_filtrado['estado'].isin(['PROGRAMADO', 'PENDIENTE'])]))
        with col_met3:
            st.metric("OT Culminadas", len(df_filtrado[df_filtrado['estado'] == 'CULMINADO']))
        with col_met4:
            st.metric("Prioridad Alta", len(df_filtrado[df_filtrado['prioridad_nueva'] == '1. ALTO']))
        
        # Mostrar tabla
        st.dataframe(df_filtrado, use_container_width=True)
        
        # Visualización de imágenes finales
        st.subheader("🖼️ Visualización de Imágenes Finales")
        ot_con_imagen = pd.read_sql('''
            SELECT codigo_ot_base, imagen_final_nombre, imagen_final_datos 
            FROM ot_unicas 
            WHERE imagen_final_datos IS NOT NULL
        ''', conn_ot_unicas)
        
        if not ot_con_imagen.empty:
            ot_seleccionada = st.selectbox(
                "Seleccionar OT para ver imagen final:",
                [f"{row['codigo_ot_base']} - {row['imagen_final_nombre']}" for _, row in ot_con_imagen.iterrows()],
                key="imagen_ot_unicas"
            )
            
            if ot_seleccionada:
                codigo_ot = ot_seleccionada.split(' - ')[0]
                imagen_data = ot_con_imagen[ot_con_imagen['codigo_ot_base'] == codigo_ot].iloc[0]
                
                if imagen_data['imagen_final_datos']:
                    st.image(imagen_data['imagen_final_datos'], caption=f"Imagen Final: {imagen_data['imagen_final_nombre']}", use_column_width=True)
        
        # Exportar a Excel
        permisos = st.session_state.get('permisos', {})
        puede_descargar_excel = permisos.get('puede_descargar_excel', False)
        
        if not df_filtrado.empty and puede_descargar_excel:
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_filtrado.to_excel(writer, sheet_name='OT_Unicas', index=False)
            
            st.download_button(
                label="📥 Exportar a Excel",
                data=excel_buffer.getvalue(),
                file_name=f"ot_unicas_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        elif not df_filtrado.empty and not puede_descargar_excel:
            st.info("ℹ️ No tiene permisos para exportar datos a Excel")
        
    except Exception as e:
        st.error(f"Error al cargar la base de datos de OT únicas: {e}")

def mostrar_base_ot_sufijos():
    """Muestra y permite exportar la base de datos de OT con sufijos"""
    st.subheader("🔢 Base de Datos: Órdenes de Trabajo con Sufijos")
    
    try:
        # Obtener todas las OT con sufijos
        df = pd.read_sql('''
            SELECT 
                id, codigo_padre, codigo_mantto, codigo_ot_base, codigo_ot_sufijo,
                estado, antiguedad, prioridad_nueva, area, equipo, codigo_equipo,
                fecha_inicio_mantenimiento, hora_inicio_mantenimiento,
                hora_finalizacion_mantenimiento, fecha_finalizacion, hora_final,
                responsables_comienzo, responsables_finalizacion,
                descripcion_trabajo_realizado, paro_linea, observaciones_cierre,
                comentario, ot_sufijo_creado_en
            FROM ot_sufijos 
            ORDER BY ot_sufijo_creado_en DESC
        ''', conn_ot_sufijos)
        
        if df.empty:
            st.info("No hay órdenes de trabajo con sufijos registradas.")
            return
        
        # Filtros
        col1, col2 = st.columns(2)
        with col1:
            estado_filtro = st.selectbox(
                "Filtrar por estado",
                ["Todos"] + sorted(df['estado'].unique().tolist()),
                key="filtro_estado_ot_sufijos"
            )
        with col2:
            busqueda = st.text_input("🔍 Buscar...", key="busqueda_ot_sufijos")
        
        # Aplicar filtros
        df_filtrado = df.copy()
        if estado_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['estado'] == estado_filtro]
        if busqueda:
            mask = (
                df_filtrado['codigo_ot_base'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['codigo_ot_sufijo'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['equipo'].str.contains(busqueda, case=False, na=False)
            )
            df_filtrado = df_filtrado[mask]
        
        # Mostrar estadísticas
        col_met1, col_met2, col_met3 = st.columns(3)
        with col_met1:
            st.metric("Total OT Sufijos", len(df_filtrado))
        with col_met2:
            st.metric("OT Base Diferentes", df_filtrado['codigo_ot_base'].nunique())
        with col_met3:
            st.metric("Con Paro de Línea", len(df_filtrado[df_filtrado['paro_linea'] == 'SI']))
        
        # Mostrar tabla
        st.dataframe(df_filtrado, use_container_width=True)
        
        # Exportar a Excel
        permisos = st.session_state.get('permisos', {})
        puede_descargar_excel = permisos.get('puede_descargar_excel', False)
        
        if not df_filtrado.empty and puede_descargar_excel:
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_filtrado.to_excel(writer, sheet_name='OT_Sufijos', index=False)
            
            st.download_button(
                label="📥 Exportar a Excel",
                data=excel_buffer.getvalue(),
                file_name=f"ot_sufijos_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        elif not df_filtrado.empty and not puede_descargar_excel:
            st.info("ℹ️ No tiene permisos para exportar datos a Excel")
        
    except Exception as e:
        st.error(f"Error al cargar la base de datos de OT sufijos: {e}")

def mostrar_base_equipos():
    """Muestra y permite exportar la base de datos de equipos"""
    st.subheader("🏭 Base de Datos: Equipos")
    
    try:
        # Obtener todos los equipos
        df = pd.read_sql('''
            SELECT 
                id, codigo_equipo, equipo, area, descripcion_funcionalidad,
                especificaciones_tecnica_nombre, informes_json,
                creado_en, actualizado_en
            FROM equipos 
            ORDER BY creado_en DESC
        ''', conn_equipos)
        
        if df.empty:
            st.info("No hay equipos registrados.")
            return
        
        # Filtros
        col1, col2 = st.columns(2)
        with col1:
            area_filtro = st.selectbox(
                "Filtrar por área",
                ["Todas"] + sorted(df['area'].unique().tolist()),
                key="filtro_area_equipos"
            )
        with col2:
            busqueda = st.text_input("🔍 Buscar...", key="busqueda_equipos")
        
        # Aplicar filtros
        df_filtrado = df.copy()
        if area_filtro != "Todas":
            df_filtrado = df_filtrado[df_filtrado['area'] == area_filtro]
        if busqueda:
            mask = (
                df_filtrado['codigo_equipo'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['equipo'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['descripcion_funcionalidad'].str.contains(busqueda, case=False, na=False)
            )
            df_filtrado = df_filtrado[mask]
        
        # Contar número de informes
        def contar_informes(informes_json):
            if informes_json and informes_json != "[]":
                try:
                    return len(json.loads(informes_json))
                except:
                    return 0
            return 0
        
        df_filtrado['num_informes'] = df_filtrado['informes_json'].apply(contar_informes)
        
        # Mostrar estadísticas
        col_met1, col_met2, col_met3, col_met4 = st.columns(4)
        with col_met1:
            st.metric("Total Equipos", len(df_filtrado))
        with col_met2:
            st.metric("Áreas Diferentes", df_filtrado['area'].nunique())
        with col_met3:
            st.metric("Con Especificaciones", df_filtrado['especificaciones_tecnica_nombre'].notna().sum())
        with col_met4:
            st.metric("Con Informes", (df_filtrado['num_informes'] > 0).sum())
        
        # Mostrar tabla sin la columna informes_json (muy larga)
        columnas_mostrar = [col for col in df_filtrado.columns if col != 'informes_json']
        st.dataframe(df_filtrado[columnas_mostrar], use_container_width=True)
        
        # Descarga de documentos de equipos
        st.subheader("📁 Descarga de Documentos de Equipos")
        
        col_doc1, col_doc2 = st.columns(2)
        
        with col_doc1:
            # Especificaciones técnicas
            st.write("**Especificaciones Técnicas**")
            equipos_con_espec = pd.read_sql('''
                SELECT codigo_equipo, equipo, especificaciones_tecnica_nombre, especificaciones_tecnica_datos
                FROM equipos 
                WHERE especificaciones_tecnica_datos IS NOT NULL
            ''', conn_equipos)
            
            if not equipos_con_espec.empty:
                equipo_espec = st.selectbox(
                    "Seleccionar equipo para descargar especificaciones:",
                    [f"{row['codigo_equipo']} - {row['equipo']} ({row['especificaciones_tecnica_nombre']})" 
                     for _, row in equipos_con_espec.iterrows()],
                    key="descarga_especificaciones"
                )
                
                if equipo_espec:
                    codigo_equipo = equipo_espec.split(' - ')[0]
                    espec_data = equipos_con_espec[equipos_con_espec['codigo_equipo'] == codigo_equipo].iloc[0]
                    
                    st.download_button(
                        label=f"📥 Descargar {espec_data['especificaciones_tecnica_nombre']}",
                        data=espec_data['especificaciones_tecnica_datos'],
                        file_name=espec_data['especificaciones_tecnica_nombre'],
                        mime="application/octet-stream",
                        use_container_width=True
                    )
        
        with col_doc2:
            # Informes técnicos
            st.write("**Informes Técnicos**")
            equipos_con_informes = []
            for _, row in df_filtrado.iterrows():
                if row['num_informes'] > 0:
                    try:
                        informes = json.loads(row['informes_json'])
                        for informe in informes:
                            equipos_con_informes.append({
                                'codigo_equipo': row['codigo_equipo'],
                                'equipo': row['equipo'],
                                'nombre_informe': informe['nombre'],
                                'datos_base64': informe.get('datos_base64'),
                                'tipo': informe.get('tipo', 'application/octet-stream')
                            })
                    except:
                        continue
            
            if equipos_con_informes:
                informe_seleccionado = st.selectbox(
                    "Seleccionar informe para descargar:",
                    [f"{row['codigo_equipo']} - {row['equipo']} ({row['nombre_informe']})" 
                     for row in equipos_con_informes],
                    key="descarga_informes"
                )
                
                if informe_seleccionado:
                    codigo_equipo = informe_seleccionado.split(' - ')[0]
                    nombre_informe = informe_seleccionado.split('(')[1].rstrip(')')
                    
                    for informe_data in equipos_con_informes:
                        if (informe_data['codigo_equipo'] == codigo_equipo and 
                            informe_data['nombre_informe'] == nombre_informe):
                            
                            # Decodificar base64
                            if informe_data['datos_base64']:
                                datos_bytes = base64.b64decode(informe_data['datos_base64'])
                                st.download_button(
                                    label=f"📥 Descargar {informe_data['nombre_informe']}",
                                    data=datos_bytes,
                                    file_name=informe_data['nombre_informe'],
                                    mime=informe_data['tipo'],
                                    use_container_width=True
                                )
                            break
        
        # Exportar a Excel
        permisos = st.session_state.get('permisos', {})
        puede_descargar_excel = permisos.get('puede_descargar_excel', False)
        
        if not df_filtrado.empty and puede_descargar_excel:
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_filtrado.to_excel(writer, sheet_name='Equipos', index=False)
            
            st.download_button(
                label="📥 Exportar a Excel",
                data=excel_buffer.getvalue(),
                file_name=f"equipos_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        elif not df_filtrado.empty and not puede_descargar_excel:
            st.info("ℹ️ No tiene permisos para exportar datos a Excel")
        
    except Exception as e:
        st.error(f"Error al cargar la base de datos de equipos: {e}")

def mostrar_base_colaboradores():
    """Muestra y permite exportar la base de datos de colaboradores"""
    st.subheader("👥 Base de Datos: Colaboradores")
    
    try:
        # Obtener todos los colaboradores (sin contraseñas por seguridad)
        df = pd.read_sql('''
            SELECT 
                codigo_id, nombre_colaborador, personal, cargo,
                creado_en, actualizado_en
            FROM colaboradores 
            ORDER BY cargo, nombre_colaborador
        ''', conn_colaboradores)
        
        if df.empty:
            st.info("No hay colaboradores registrados.")
            return
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            cargo_filtro = st.selectbox(
                "Filtrar por cargo",
                ["Todos"] + sorted(df['cargo'].unique().tolist()),
                key="filtro_cargo_colaboradores"
            )
        with col2:
            personal_filtro = st.selectbox(
                "Filtrar por tipo de personal",
                ["Todos"] + sorted(df['personal'].unique().tolist()),
                key="filtro_personal_colaboradores"
            )
        with col3:
            busqueda = st.text_input("🔍 Buscar...", key="busqueda_colaboradores")
        
        # Aplicar filtros
        df_filtrado = df.copy()
        if cargo_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cargo'] == cargo_filtro]
        if personal_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['personal'] == personal_filtro]
        if busqueda:
            mask = (
                df_filtrado['codigo_id'].str.contains(busqueda, case=False, na=False) |
                df_filtrado['nombre_colaborador'].str.contains(busqueda, case=False, na=False)
            )
            df_filtrado = df_filtrado[mask]
        
        # Mostrar estadísticas
        col_met1, col_met2, col_met3 = st.columns(3)
        with col_met1:
            st.metric("Total Colaboradores", len(df_filtrado))
        with col_met2:
            st.metric("Cargos Diferentes", df_filtrado['cargo'].nunique())
        with col_met3:
            st.metric("Tipos de Personal", df_filtrado['personal'].nunique())
        
        # Mostrar tabla
        st.dataframe(df_filtrado, use_container_width=True)
        
        # Exportar a Excel
        permisos = st.session_state.get('permisos', {})
        puede_descargar_excel = permisos.get('puede_descargar_excel', False)
        
        if not df_filtrado.empty and puede_descargar_excel:
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_filtrado.to_excel(writer, sheet_name='Colaboradores', index=False)
            
            st.download_button(
                label="📥 Exportar a Excel",
                data=excel_buffer.getvalue(),
                file_name=f"colaboradores_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        elif not df_filtrado.empty and not puede_descargar_excel:
            st.info("ℹ️ No tiene permisos para exportar datos a Excel")
        
    except Exception as e:
        st.error(f"Error al cargar la base de datos de colaboradores: {e}")

def mostrar_exportacion_masiva():
    """Permite exportar todas las bases de datos en un solo archivo Excel"""
    st.subheader("📁 Exportación Masiva de Todas las Bases de Datos")
    
    st.info("""
    **💡 Funcionalidad de Exportación Masiva**
    
    Esta herramienta permite exportar todas las bases de datos del sistema 
    en un solo archivo Excel con múltiples hojas.
    """)
    
    if st.button("🚀 Generar Archivo Excel con Todas las Bases de Datos", use_container_width=True):
        try:
            # Crear buffer para el archivo Excel
            excel_buffer = BytesIO()
            
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                # 1. Avisos
                try:
                    df_avisos = pd.read_sql('SELECT * FROM avisos', conn_avisos)
                    df_avisos.to_excel(writer, sheet_name='Avisos', index=False)
                except Exception as e:
                    st.error(f"Error al exportar avisos: {e}")
                
                # 2. OT Únicas
                try:
                    df_ot_unicas = pd.read_sql('SELECT * FROM ot_unicas', conn_ot_unicas)
                    df_ot_unicas.to_excel(writer, sheet_name='OT_Unicas', index=False)
                except Exception as e:
                    st.error(f"Error al exportar OT únicas: {e}")
                
                # 3. OT Sufijos
                try:
                    df_ot_sufijos = pd.read_sql('SELECT * FROM ot_sufijos', conn_ot_sufijos)
                    df_ot_sufijos.to_excel(writer, sheet_name='OT_Sufijos', index=False)
                except Exception as e:
                    st.error(f"Error al exportar OT sufijos: {e}")
                
                # 4. Equipos
                try:
                    df_equipos = pd.read_sql('SELECT * FROM equipos', conn_equipos)
                    df_equipos.to_excel(writer, sheet_name='Equipos', index=False)
                except Exception as e:
                    st.error(f"Error al exportar equipos: {e}")
                
                # 5. Colaboradores (sin contraseñas)
                try:
                    df_colaboradores = pd.read_sql('''
                        SELECT codigo_id, nombre_colaborador, personal, cargo, creado_en, actualizado_en 
                        FROM colaboradores
                    ''', conn_colaboradores)
                    df_colaboradores.to_excel(writer, sheet_name='Colaboradores', index=False)
                except Exception as e:
                    st.error(f"Error al exportar colaboradores: {e}")
                
                # 6. Hoja de resumen
                try:
                    resumen_data = {
                        'Base de Datos': ['Avisos', 'OT Únicas', 'OT Sufijos', 'Equipos', 'Colaboradores'],
                        'Total Registros': [
                            len(df_avisos) if 'df_avisos' in locals() else 0,
                            len(df_ot_unicas) if 'df_ot_unicas' in locals() else 0,
                            len(df_ot_sufijos) if 'df_ot_sufijos' in locals() else 0,
                            len(df_equipos) if 'df_equipos' in locals() else 0,
                            len(df_colaboradores) if 'df_colaboradores' in locals() else 0
                        ],
                        'Fecha Exportación': [datetime.now().strftime("%Y-%m-%d %H:%M")] * 5
                    }
                    df_resumen = pd.DataFrame(resumen_data)
                    df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
                except Exception as e:
                    st.error(f"Error al crear resumen: {e}")
            
            # Botón de descarga
            st.success("✅ Archivo Excel generado exitosamente!")
            
            st.download_button(
                label="📥 Descargar Archivo Completo",
                data=excel_buffer.getvalue(),
                file_name=f"backup_completo_sistema_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
        except Exception as e:
            st.error(f"Error al generar el archivo de exportación masiva: {e}")

# ===============================APLICACIÓN PRINCIPAL================================

def main():
    """Función principal de la aplicación"""
    
    # Configuración de la página
    st.set_page_config(
        page_title="Sistema de Mantenimiento",
        page_icon="🔧",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Inicializar sesión
    inicializar_sesion()
    
    # Si no está autenticado, mostrar login
    if not st.session_state.autenticado:
        mostrar_login()
        return
    
    # ===============================INTERFAZ PRINCIPAL (USUARIO AUTENTICADO)================================
    
    # Menú lateral para navegación
    st.sidebar.title("🔧 Sistema de Mantenimiento")
    st.sidebar.markdown("---")
    
    # Obtener permisos del usuario
    permisos = st.session_state.get('permisos', {})
    
    # Opciones del menú basadas en permisos
    menu_options = ["🏠 Inicio"]
    
    if permisos.get('acceso_avisos', False):
        menu_options.append("📝 Avisos de Mantenimiento")
    
    if permisos.get('acceso_ot', False):
        menu_options.append("📋 Órdenes de Trabajo")
    
    if permisos.get('acceso_equipos', False):
        menu_options.append("🏭 Gestión de Equipos")
    
    if permisos.get('acceso_colaboradores', False):
        menu_options.append("👥 Colaboradores de Mantto")
    
    if permisos.get('acceso_reportes', False):
        menu_options.append("📊 Reportes")
    
    if permisos.get('acceso_bases_datos', False):
        menu_options.append("💾 Bases de Datos")
    
    selected_menu = st.sidebar.selectbox("Navegación", menu_options)
    
    # Botones de acciones rápidas
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔄 Acciones Rápidas")
    
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("🔄 Actualizar", use_container_width=True, help="Actualizar la página actual"):
            st.sidebar.success("✅ Página actualizada!")
    
    with col2:
        if st.button("🧹 Limpiar", use_container_width=True, help="Limpiar cache temporal"):
            keys_to_clear = ['archivo_eliminado']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.sidebar.success("✅ Cache limpiado!")
    
    # Mostrar información de usuario y botón de logout
    mostrar_logout()
    
    st.sidebar.markdown("---")
    st.sidebar.info("💡 **Tip:** Los datos se actualizan automáticamente al cambiar de pestaña.")
    
    # Navegación entre secciones con verificación de permisos
    if selected_menu == "🏠 Inicio":
        mostrar_inicio_autenticado()
    
    elif selected_menu == "📝 Avisos de Mantenimiento":
        if verificar_acceso_seccion('avisos'):
            gestion_avisos()
    
    elif selected_menu == "📋 Órdenes de Trabajo":
        if verificar_acceso_seccion('ot'):
            gestion_ot()
    
    elif selected_menu == "🏭 Gestión de Equipos":
        if verificar_acceso_seccion('equipos'):
            gestion_equipos()
    
    elif selected_menu == "👥 Colaboradores de Mantto":
        if verificar_acceso_seccion('colaboradores'):
            gestion_colaboradores()
    
    elif selected_menu == "📊 Reportes":
        if verificar_acceso_seccion('reportes'):
            mostrar_reportes()
    
    elif selected_menu == "💾 Bases de Datos":
        if verificar_acceso_seccion('bases_datos'):
            mostrar_visualizacion_bases_datos()

# Funciones placeholder para las otras secciones
def mostrar_inicio():
    st.title("🏠 Inicio - Sistema de Gestión de Mantenimiento")
    st.write("Bienvenido al sistema de gestión de mantenimiento.")
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("**📝 Avisos de Mantenimiento**\n\nGestión de avisos y solicitudes de mantenimiento.")
    
    with col2:
        st.info("**📋 Órdenes de Trabajo**\n\nCreación y seguimiento de órdenes de trabajo.")
    
    with col3:
        st.info("**🏭 Gestión de Equipos**\n\nRegistro y administración de información técnica de equipos.")

def mostrar_reportes():
    """Función principal para mostrar reportes"""
    st.title("📊 Reportes")
    
    # Pestañas para diferentes tipos de reportes
    tab1, tab2 = st.tabs(["📋 OT PENDIENTES", "✅ OT CULMINADAS"])
    
    with tab1:
        mostrar_ot_pendientes()
    
    with tab2:
        mostrar_ot_culminadas()

if __name__ == "__main__":
    main()
