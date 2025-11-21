import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime, date
import hashlib
from io import BytesIO
import logging
import time
import sqlite3
import base64

# ==================== CONFIGURACIÓN DE LA PÁGINA - DEBE SER LO PRIMERO ====================
st.set_page_config(
    page_title="Sistema de Mantenimiento", 
    page_icon="🔧", 
    layout="wide"
)

# ==================== CONFIGURACIÓN DE LOGGING ====================
def setup_logging():
    """Configurar sistema de logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('mantenimiento_app.log'),
            logging.StreamHandler()
        ]
    )

def log_accion(usuario, accion, detalles=""):
    """Registrar acciones importantes"""
    logging.info(f"Usuario: {usuario} - Acción: {accion} - Detalles: {detalles}")

# ==================== FUNCIÓN PARA MANEJO SEGURO DE CONEXIONES ====================
def ejecutar_consulta_segura(conexion, consulta, parametros=None, max_intentos=3):
    """
    Ejecutar consulta SQL con manejo seguro de bloqueos
    """
    intento = 0
    while intento < max_intentos:
        try:
            if parametros:
                resultado = conexion.execute(consulta, parametros)
            else:
                resultado = conexion.execute(consulta)
            
            conexion.commit()
            return resultado
            
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                intento += 1
                print(f"⚠️ Intento {intento}: Base de datos bloqueada, reintentando...")
                time.sleep(0.5)  # Esperar medio segundo
            else:
                raise e
        except Exception as e:
            raise e
    
    # Si llegamos aquí, todos los intentos fallaron
    raise sqlite3.OperationalError("No se pudo ejecutar la consulta después de varios intentos")

# ==================== INICIALIZACIÓN DE BASES DE DATOS ====================
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
    
    c.execute('CREATE INDEX IF NOT EXISTS idx_codigo_base ON ot_sufijos(codigo_ot_base)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_estado ON ot_sufijos(estado)')
    
    conn.commit()
    return conn

# ==================== INICIALIZAR TODAS LAS BASES DE DATOS ====================
conn_avisos = init_avisos_db()
conn_ot_unicas = init_ot_unicas_db()
conn_ot_sufijos = init_ot_sufijos_db()

# ==================== FUNCIÓN PARA VERIFICAR COLUMNAS DE IMAGEN ====================
def verificar_y_crear_columnas_imagen():
    """Verificar y crear columnas de imagen si no existen"""
    try:
        # Verificar estructura de la tabla avisos
        estructura = pd.read_sql("PRAGMA table_info(avisos)", conn_avisos)
        columnas_existentes = estructura['name'].tolist()
        
        # Columnas que deben existir
        columnas_requeridas = [
            'imagen_aviso_nombre',
            'imagen_aviso_datos'
        ]
        
        for columna in columnas_requeridas:
            if columna not in columnas_existentes:
                if columna == 'imagen_aviso_nombre':
                    ejecutar_consulta_segura(conn_avisos, f'ALTER TABLE avisos ADD COLUMN {columna} TEXT')
                elif columna == 'imagen_aviso_datos':
                    ejecutar_consulta_segura(conn_avisos, f'ALTER TABLE avisos ADD COLUMN {columna} BLOB')
                st.sidebar.info(f"✅ Columna {columna} agregada")
        
    except Exception as e:
        st.sidebar.error(f"Error al verificar columnas: {e}")

# ==================== VERIFICAR COLUMNAS DE IMAGEN ====================
verificar_y_crear_columnas_imagen()

# ==================== FUNCIONES ADICIONALES ====================
def actualizar_estructura_todas_tablas():
    """Actualizar la estructura de todas las tablas de forma robusta"""
    try:
        # Columnas a verificar/agregar en avisos
        columnas_avisos = [
            "codigo_ot TEXT",
            "clasificacion TEXT", 
            "descripcion_trabajo TEXT",
            "sistema TEXT",
            "codigo_equipo TEXT",
            "prioridad_nueva TEXT",
            "responsable TEXT",
            "alimentador_proveedor TEXT",
            "materiales TEXT",
            "cantidad_mecanicos INTEGER DEFAULT 0",
            "cantidad_electricos INTEGER DEFAULT 0",
            "cantidad_soldadores INTEGER DEFAULT 0",
            "cantidad_op_vahos INTEGER DEFAULT 0",
            "cantidad_calderistas INTEGER DEFAULT 0",
            "fecha_estimada_inicio DATE",
            "duracion_estimada TEXT",
            "ot_base_creado_en TIMESTAMP"
        ]
        
        # Verificar y agregar columnas faltantes en avisos
        for columna in columnas_avisos:
            try:
                nombre_col = columna.split()[0]
                ejecutar_consulta_segura(conn_avisos, f'ALTER TABLE avisos ADD COLUMN {columna}')
                st.sidebar.info(f"✅ Columna {nombre_col} agregada a avisos")
            except Exception as e:
                if "duplicate column name" not in str(e):
                    st.sidebar.warning(f"⚠️ Columna {nombre_col} ya existe o error: {e}")
        
    except Exception as e:
        st.sidebar.error(f"Error al actualizar estructura: {e}")

def actualizar_estructura_ot_sufijos():
    """Actualizar estructura de ot_sufijos con columnas de imagen"""
    try:
        # Verificar estructura de la tabla ot_sufijos
        estructura = pd.read_sql("PRAGMA table_info(ot_sufijos)", conn_ot_sufijos)
        columnas_existentes = estructura['name'].tolist()
        
        # Columnas que deben existir para imágenes
        columnas_imagen = [
            'imagen_final_nombre',
            'imagen_final_datos'
        ]
        
        for columna in columnas_imagen:
            if columna not in columnas_existentes:
                if columna == 'imagen_final_nombre':
                    ejecutar_consulta_segura(conn_ot_sufijos, f'ALTER TABLE ot_sufijos ADD COLUMN {columna} TEXT')
                elif columna == 'imagen_final_datos':
                    ejecutar_consulta_segura(conn_ot_sufijos, f'ALTER TABLE ot_sufijos ADD COLUMN {columna} BLOB')
                st.sidebar.info(f"✅ Columna {columna} agregada a ot_sufijos")
        
    except Exception as e:
        st.sidebar.error(f"Error al actualizar ot_sufijos: {e}")

# Llama esta función después de inicializar las bases de datos
actualizar_estructura_ot_sufijos()

# ==================== FUNCIÓN FALTANTE CRÍTICA ====================
def acumular_valor_ot_unica(codigo_ot_base, campo, nuevo_valor):
    """Acumular valores en campos de texto de OT única"""
    try:
        # Primero verificar si la columna existe
        try:
            # Intentar leer la columna
            resultado_test = pd.read_sql(
                f'SELECT {campo} FROM ot_unicas WHERE codigo_ot_base = ? LIMIT 1',
                conn_ot_unicas,
                params=(codigo_ot_base,)
            )
        except Exception as e:
            # Si la columna no existe, crearla
            if "no such column" in str(e):
                st.warning(f"⚠️ La columna {campo} no existe, creándola...")
                try:
                    ejecutar_consulta_segura(conn_ot_unicas, f'ALTER TABLE ot_unicas ADD COLUMN {campo} TEXT')
                    st.success(f"✅ Columna {campo} creada exitosamente")
                except Exception as alter_error:
                    st.error(f"❌ Error creando columna {campo}: {alter_error}")
                    return False
            else:
                st.error(f"❌ Error verificando columna {campo}: {e}")
                return False
        
        # Obtener valor actual
        resultado = pd.read_sql(
            f'SELECT {campo} FROM ot_unicas WHERE codigo_ot_base = ?',
            conn_ot_unicas,
            params=(codigo_ot_base,)
        )
        
        valor_actual = ""
        if not resultado.empty and resultado.iloc[0][campo] is not None:
            valor_actual = resultado.iloc[0][campo] + " | "
        
        valor_nuevo = valor_actual + nuevo_valor
        
        # Actualizar el campo
        ejecutar_consulta_segura(
            conn_ot_unicas,
            f'UPDATE ot_unicas SET {campo} = ? WHERE codigo_ot_base = ?',
            (valor_nuevo, codigo_ot_base)
        )
        return True
    except Exception as e:
        st.error(f"Error acumulando {campo}: {e}")
        return False

# ==================== OPTIMIZACIÓN DE BASES DE DATOS ====================
def crear_indices_optimizacion():
    """Crear índices para optimizar consultas frecuentes"""
    indices_avisos = [
        "CREATE INDEX IF NOT EXISTS idx_avisos_estado ON avisos(estado)",
        "CREATE INDEX IF NOT EXISTS idx_avisos_area ON avisos(area)",
        "CREATE INDEX IF NOT EXISTS idx_avisos_codigo_padre ON avisos(codigo_padre)",
        "CREATE INDEX IF NOT EXISTS idx_avisos_ingresado_el ON avisos(ingresado_el)"
    ]
    
    indices_ot_unicas = [
        "CREATE INDEX IF NOT EXISTS idx_ot_unicas_estado ON ot_unicas(estado)",
        "CREATE INDEX IF NOT EXISTS idx_ot_unicas_base ON ot_unicas(codigo_ot_base)",
        "CREATE INDEX IF NOT EXISTS idx_ot_unicas_padre ON ot_unicas(codigo_padre)",
        "CREATE INDEX IF NOT EXISTS idx_ot_unicas_sistema ON ot_unicas(sistema)"
    ]
    
    indices_ot_sufijos = [
        "CREATE INDEX IF NOT EXISTS idx_ot_sufijos_base ON ot_sufijos(codigo_ot_base)",
        "CREATE INDEX IF NOT EXISTS idx_ot_sufijos_estado ON ot_sufijos(estado)",
        "CREATE INDEX IF NOT EXISTS idx_ot_sufijos_sufijo ON ot_sufijos(codigo_ot_sufijo)"
    ]
    
    # Crear índices para cada base de datos
    for indice in indices_avisos:
        try:
            ejecutar_consulta_segura(conn_avisos, indice)
        except Exception as e:
            pass
    
    for indice in indices_ot_unicas:
        try:
            ejecutar_consulta_segura(conn_ot_unicas, indice)
        except Exception as e:
            pass
            
    for indice in indices_ot_sufijos:
        try:
            ejecutar_consulta_segura(conn_ot_sufijos, indice)
        except Exception as e:
            pass

# ==================== SISTEMA DE LOGIN ====================
def init_login_db():
    """Inicializar base de datos de usuarios"""
    conn = sqlite3.connect('data/mantenimiento.db', check_same_thread=False, timeout=30)
    
    # Crear tabla de usuarios si no existe
    conn.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            nombre_completo TEXT NOT NULL,
            rol TEXT NOT NULL,
            activo INTEGER DEFAULT 1,
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Insertar usuarios por defecto (solo si no existen)
    usuarios_por_defecto = [
        ('diablo12xd', 'elamorylamentiranuncavanjuntos', 'diablo12xd', 'admin'),
        ('supervisor.mantto.mecanico', 'jvasquez123', 'Jhonar Vasquez', 'supervisor mecanico'),
        ('supervisor.mantto.electrico', 'dquispe123', 'Diego Quispe', 'supervisor electrico'),
        ('supervisor', 'super123', 'Supervisor', 'supervisor'),
        ('planner.mantto', 'wramos123', 'Wilmer Ramos', 'Planner de mantenimiento'),
        ('jefe.mantenimiento', 'jhuaman123', 'Jesus Huaman', 'Jefe de mantenimiento'),
        ('asistente.mantto', 'blazo123', 'Brayan Lazo', 'Asistente de mantenimiento'),
        ('operario.mantto.mecanico', 'mecanico123', 'Operarios Mecanicos', 'Operador mecanico'),
        ('operario.mantto.electrico', 'electrico123', 'Operarios Electricos', 'Operador electrico'),
        ('practicante.mantto', 'nalzamora123', 'Nayeli Alzamora', 'Practicante de mantenimiento')
    ]
    
    for username, password, nombre, rol in usuarios_por_defecto:
        existe = conn.execute(
            'SELECT 1 FROM usuarios WHERE username = ?', (username,)
        ).fetchone()
        
        if not existe:
            password_hash = hashlib.sha256(password.encode()).hexdigest()
            conn.execute(
                'INSERT INTO usuarios (username, password_hash, nombre_completo, rol) VALUES (?, ?, ?, ?)',
                (username, password_hash, nombre, rol)
            )
    
    conn.commit()
    conn.close()
    return

def verificar_login(username, password):
    """Verificar credenciales de usuario"""
    conn_login = sqlite3.connect('data/mantenimiento.db', check_same_thread=False, timeout=30)
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    resultado = conn_login.execute(
        'SELECT username, nombre_completo, rol FROM usuarios WHERE username = ? AND password_hash = ? AND activo = 1',
        (username, password_hash)
    ).fetchone()
    
    conn_login.close()
    return resultado

def mostrar_login():
    """Mostrar formulario de login"""
    st.markdown("""
        <style>
        .login-container {
            max-width: 400px;
            margin: 100px auto;
            padding: 30px;
            border: 1px solid #ddd;
            border-radius: 10px;
            background-color: #f9f9f9;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    
    st.title("🔐 Sistema de Mantenimiento")
    st.subheader("Iniciar Sesión")
    
    with st.form("login_form"):
        username = st.text_input("👤 Usuario", placeholder="Ingrese su usuario")
        password = st.text_input("🔒 Contraseña", type="password", placeholder="Ingrese su contraseña")
        
        col1, col2 = st.columns([1, 1])
        with col1:
            login_button = st.form_submit_button("🚀 Ingresar", use_container_width=True)
        with col2:
            st.form_submit_button("🔄 Limpiar", use_container_width=True)
        
        if login_button:
            if username and password:
                usuario = verificar_login(username, password)
                if usuario:
                    st.session_state.autenticado = True
                    st.session_state.usuario = usuario[0]
                    st.session_state.nombre_completo = usuario[1]
                    st.session_state.rol = usuario[2]
                    st.success(f"¡Bienvenido {usuario[1]}!")
                    log_accion(usuario[0], "LOGIN", "Inicio de sesión exitoso")
                    st.rerun()
                else:
                    st.error("❌ Usuario o contraseña incorrectos")
                    log_accion(username, "LOGIN_FALLIDO", "Credenciales incorrectas")
            else:
                st.warning("⚠️ Por favor ingrese usuario y contraseña")

#====================VERIFICAR ESTRCUCTURA DE TABLA================
def verificar_estructura_tabla():
    try:
        # Verificar estructura de ot_sufijos
        resultado = pd.read_sql("PRAGMA table_info(ot_sufijos)", conn_ot_sufijos)
        st.sidebar.write("Estructura de ot_sufijos:")
        st.sidebar.dataframe(resultado[['name', 'type']])
        return resultado
    except Exception as e:
        st.sidebar.error(f"Error al verificar estructura: {e}")

def mostrar_login():
    """Mostrar formulario de login"""
    st.markdown("""
        <style>
        .login-container {
            max-width: 400px;
            margin: 100px auto;
            padding: 30px;
            border: 1px solid #ddd;
            border-radius: 10px;
            background-color: #f9f9f9;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    
    st.title("🔐 Sistema de Mantenimiento")
    st.subheader("Iniciar Sesión")
    
    with st.form("login_form"):
        username = st.text_input("👤 Usuario", placeholder="Ingrese su usuario")
        password = st.text_input("🔒 Contraseña", type="password", placeholder="Ingrese su contraseña")
        
        col1, col2 = st.columns([1, 1])
        with col1:
            login_button = st.form_submit_button("🚀 Ingresar", use_container_width=True)
        with col2:
            st.form_submit_button("🔄 Limpiar", use_container_width=True)
        
        if login_button:
            if username and password:
                usuario = verificar_login(username, password)
                if usuario:
                    st.session_state.autenticado = True
                    st.session_state.usuario = usuario[0]
                    st.session_state.nombre_completo = usuario[1]
                    st.session_state.rol = usuario[2]
                    st.success(f"¡Bienvenido {usuario[1]}!")
                    log_accion(usuario[0], "LOGIN", "Inicio de sesión exitoso")
                    st.rerun()
                else:
                    st.error("❌ Usuario o contraseña incorrectos")
                    log_accion(username, "LOGIN_FALLIDO", "Credenciales incorrectas")
            else:
                st.warning("⚠️ Por favor ingrese usuario y contraseña")

def verificar_estructuras_completas():
    """Verificar y actualizar todas las estructuras de tablas"""
    verificar_y_crear_columnas_imagen()
    actualizar_estructura_ot_sufijos()
    actualizar_estructura_todas_tablas()

# Llama esta función después de inicializar las bases de datos
verificar_estructuras_completas()

# ==================== FUNCIONES PARA BASE DE DATOS DE AVISOS ====================

def obtener_avisos_ingresados():
    return pd.read_sql('SELECT * FROM avisos WHERE estado = "INGRESADO" ORDER BY codigo_padre DESC', conn_avisos)

def obtener_avisos_programados():
    return pd.read_sql('SELECT * FROM avisos WHERE estado = "PROGRAMADO" ORDER BY codigo_padre DESC', conn_avisos)

def obtener_todos_avisos():
    return pd.read_sql('SELECT * FROM avisos ORDER BY codigo_padre DESC', conn_avisos)

# ==================== FUNCIONES PARA BASE DE DATOS DE OT ====================
#===================GENERAR CODIGO OT CON SUFIJO=============================
def generar_codigo_ot_con_sufijo(codigo_base):
    """Generar código SUB OT con sufijo incremental - en base ot_sufijos"""
    # Buscar en la base de datos de ot_sufijos
    codigos_existentes = pd.read_sql(
        'SELECT codigo_ot_sufijo FROM ot_sufijos WHERE codigo_ot_sufijo LIKE ? ORDER BY codigo_ot_sufijo DESC', 
        conn_ot_sufijos,
        params=(f"{codigo_base}-%",)
    )
    
    if len(codigos_existentes) > 0:
        ultimo_codigo = codigos_existentes.iloc[0]['codigo_ot_sufijo']
        try:
            ultimo_sufijo = int(ultimo_codigo.split('-')[-1])
            nuevo_sufijo = ultimo_sufijo + 1
        except:
            nuevo_sufijo = 1
    else:
        nuevo_sufijo = 1
    
    return f"{codigo_base}-{nuevo_sufijo:02d}"

def obtener_ot_programadas():
    return pd.read_sql('SELECT * FROM ot_unicas WHERE estado = "PROGRAMADO" ORDER BY codigo_ot_base DESC', conn_ot_unicas)

def obtener_ot_pendientes():
    return pd.read_sql('SELECT * FROM ot_sufijos WHERE estado = "PENDIENTE" ORDER BY codigo_ot_sufijo DESC', conn_ot_sufijos)

def obtener_ot_culminadas():
    return pd.read_sql('SELECT * FROM ot_sufijos WHERE estado = "CULMINADO" ORDER BY codigo_ot_sufijo DESC', conn_ot_sufijos)

def obtener_todas_ot():
    return pd.read_sql('SELECT * FROM ot_sufijos ORDER BY codigo_ot_base DESC, codigo_ot_sufijo DESC', conn_ot_sufijos)

def obtener_codigos_ot_base():
    """Obtener todos los códigos OT base de la tabla ot_unicas - CORREGIDO"""
    return pd.read_sql('SELECT codigo_ot_base FROM ot_unicas WHERE codigo_ot_base IS NOT NULL', conn_ot_unicas)

def obtener_todas_ot_base():
    """Obtener todas las OT base de la tabla ot_unicas"""
    return pd.read_sql('SELECT * FROM ot_unicas ORDER BY codigo_ot_base DESC', conn_ot_unicas)

def obtener_todas_ot_sufijos():
    """Obtener todas las OT con sufijos"""
    return pd.read_sql('SELECT * FROM ot_sufijos ORDER BY codigo_ot_base DESC, codigo_ot_sufijo DESC', conn_ot_sufijos)

def generar_numero_ot():
    """Generar número secuencial para OT en formato 00000001"""
    ultimo_codigo = pd.read_sql(
        'SELECT codigo_ot_base FROM ot_unicas WHERE codigo_ot_base LIKE "OT - %" ORDER BY id DESC LIMIT 1', 
        conn_ot_unicas
    )
    
    if len(ultimo_codigo) > 0:
        try:
            # Extraer el número del formato "OT - 00000001"
            ultimo_numero_str = ultimo_codigo.iloc[0]['codigo_ot_base'].split(' - ')[1]
            ultimo_numero = int(ultimo_numero_str)
            nuevo_numero = ultimo_numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    
    return f"OT - {nuevo_numero:08d}"

def generar_codigo_mantto():
    """Generar código de mantenimiento único"""
    ultimo_codigo = pd.read_sql(
        'SELECT codigo_mantto FROM avisos WHERE codigo_mantto LIKE "AM-%" ORDER BY id DESC LIMIT 1', 
        conn_avisos
    )
    if len(ultimo_codigo) > 0:
        try:
            ultimo_numero = int(ultimo_codigo.iloc[0]['codigo_mantto'].split('-')[1])
            nuevo_numero = ultimo_numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    return f"AM-{nuevo_numero:08d}"

# Agrega esta función en la sección de funciones para OT
#=====================OBTENER OT POR PREFIJO===============================
def obtener_ot_por_prefijo(codigo_base):
    return pd.read_sql(
        'SELECT codigo_ot_sufijo, estado FROM ot_sufijos WHERE codigo_ot_base = ? ORDER BY codigo_ot_sufijo', 
        conn_ot_sufijos,
        params=(codigo_base,)
    )

#=======================ACTUALIZAR DATOS EN CASCADA===========================
def actualizar_estados_cascada(codigo_ot_base, codigo_padre):
    """
    Actualizar estados en cascada cuando se culmina una OT:
    1. Todas las OT con sufijo del mismo código base → "CULMINADO"
    2. La OT base → "CULMINADO" 
    3. El aviso original (codigo_padre) → "CULMINADO"
    """
    try:
        # 1. Actualizar todas las OT con sufijo del mismo código base
        ejecutar_consulta_segura(
            conn_ot_sufijos,
            'UPDATE ot_sufijos SET estado = "CULMINADO" WHERE codigo_ot_base = ?',
            (codigo_ot_base,)
        )
        
        # 2. Actualizar la OT base
        ejecutar_consulta_segura(
            conn_ot_unicas,
            'UPDATE ot_unicas SET estado = "CULMINADO" WHERE codigo_ot_base = ?',
            (codigo_ot_base,)
        )
        
        # 3. Actualizar el aviso original
        if codigo_padre:
            ejecutar_consulta_segura(
                conn_avisos,
                'UPDATE avisos SET estado = "CULMINADO" WHERE codigo_padre = ?',
                (codigo_padre,)
            )
        
        log_accion(st.session_state.usuario, "CASCADA_CULMINADA", f"OT: {codigo_ot_base}, Padre: {codigo_padre}")
        return True
    except Exception as e:
        st.error(f"Error en actualización en cascada: {e}")
        log_accion(st.session_state.usuario, "ERROR_CASCADA", f"OT: {codigo_ot_base}, Error: {str(e)}")
        return False

def actualizar_estados_a_pendiente(codigo_ot_base, codigo_padre):
    """
    Actualizar estados a PENDIENTE cuando se registra una OT pendiente:
    1. La OT base → "PENDIENTE" 
    2. El aviso original (codigo_padre) → "EN PROCESO"
    3. Las OT con sufijo se mantienen según su estado individual
    """
    try:
        # 1. Actualizar la OT base a PENDIENTE
        ejecutar_consulta_segura(
            conn_ot_unicas,
            'UPDATE ot_unicas SET estado = "PENDIENTE" WHERE codigo_ot_base = ?',
            (codigo_ot_base,)
        )
        
        # 2. Actualizar el aviso original a EN PROCESO
        if codigo_padre:
            ejecutar_consulta_segura(
                conn_avisos,
                'UPDATE avisos SET estado = "EN PROCESO" WHERE codigo_padre = ?',
                (codigo_padre,)
            )
        
        log_accion(st.session_state.usuario, "CASCADA_PENDIENTE", f"OT: {codigo_ot_base}, Padre: {codigo_padre}")
        return True
    except Exception as e:
        st.error(f"Error en actualización a pendiente: {e}")
        log_accion(st.session_state.usuario, "ERROR_CASCADA_PENDIENTE", f"OT: {codigo_ot_base}, Error: {str(e)}")
        return False

#==========================BUSCA CODIGO PADRE==========================
def obtener_codigo_padre_por_ot_base(codigo_ot_base):
    """Obtener el código padre relacionado con una OT base"""
    try:
        resultado = pd.read_sql(
            'SELECT codigo_padre FROM ot_unicas WHERE codigo_ot_base = ?',
            conn_ot_unicas,
            params=(codigo_ot_base,)
        )
        if not resultado.empty and resultado.iloc[0]['codigo_padre']:
            return resultado.iloc[0]['codigo_padre']
        return None
    except Exception as e:
        st.error(f"Error al obtener código padre: {e}")
        return None

# ==================== FUNCIONES DE MIGRACIÓN (OPCIONAL) ====================
def migrar_relaciones_avisos_ot():
    """Migrar relaciones entre avisos y OT existentes"""
    try:
        # Obtener todos los avisos que tienen código OT
        avisos_con_ot = pd.read_sql(
            'SELECT codigo_padre, codigo_ot FROM avisos WHERE codigo_ot IS NOT NULL', 
            conn_avisos
        )
        
        for _, aviso in avisos_con_ot.iterrows():
            ejecutar_consulta_segura(
                conn_ot_unicas,
                'UPDATE ot_unicas SET codigo_padre = ? WHERE codigo_ot_base = ?',
                (aviso['codigo_padre'], aviso['codigo_ot'])
            )
        
        st.sidebar.success("✅ Relaciones migradas correctamente")
        log_accion(st.session_state.usuario, "MIGRACION_RELACIONES", "Migración completada")
        
    except Exception as e:
        st.sidebar.info(f"ℹ️ No se pudieron migrar las relaciones existentes: {e}")

#==================Función para verificar estructura de tablas=================
def verificar_estructura_tablas():
    """Verificar que todas las tablas tengan la estructura correcta"""
    tablas = {
        'avisos': conn_avisos,
        'ot_unicas': conn_ot_unicas,
        'ot_sufijos': conn_ot_sufijos
    }
    
    for nombre_tabla, conexion in tablas.items():
        try:
            estructura = pd.read_sql(f"PRAGMA table_info({nombre_tabla})", conexion)
            st.sidebar.write(f"✅ {nombre_tabla}: {len(estructura)} columnas")
        except Exception as e:
            st.sidebar.error(f"❌ Error en {nombre_tabla}: {e}")

# ==================== NUEVA FUNCIÓN: GENERAR CÓDIGO PADRE PARA OT DIRECTA ====================
def generar_codigo_padre_ot_directa():
    """Generar código padre para OT directa con el mismo formato que avisos (CODP-00000001)"""
    ultimo_codigo = pd.read_sql(
        'SELECT codigo_padre FROM avisos WHERE codigo_padre LIKE "CODP-%" ORDER BY id DESC LIMIT 1', 
        conn_avisos
    )
    if len(ultimo_codigo) > 0:
        try:
            ultimo_numero = int(ultimo_codigo.iloc[0]['codigo_padre'].split('-')[1])
            nuevo_numero = ultimo_numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    return f"CODP-{nuevo_numero:08d}"

# ==================== FUNCIONES DE ELIMINACIÓN SEGURA ====================
#=======================ELIMINAR AVISOS=============================
def eliminar_aviso_por_codigo(codigo_padre):
    """Eliminar un aviso por código padre de forma segura"""
    try:
        # Verificar si existe el aviso
        aviso_existente = pd.read_sql(
            'SELECT * FROM avisos WHERE codigo_padre = ?', 
            conn_avisos, 
            params=(codigo_padre,)
        )
        
        if aviso_existente.empty:
            return False, "❌ El aviso no existe"
        
        # Verificar si tiene OT relacionadas
        ot_relacionadas = pd.read_sql(
            'SELECT * FROM ot_unicas WHERE codigo_padre = ?', 
            conn_ot_unicas, 
            params=(codigo_padre,)
        )
        
        if not ot_relacionadas.empty:
            return False, "❌ No se puede eliminar: tiene OT relacionadas"
        
        # Eliminar el aviso
        ejecutar_consulta_segura(
            conn_avisos,
            'DELETE FROM avisos WHERE codigo_padre = ?', 
            (codigo_padre,)
        )
        
        log_accion(st.session_state.usuario, "AVISO_ELIMINADO", f"Código: {codigo_padre}")
        return True, "✅ Aviso eliminado correctamente"
        
    except Exception as e:
        return False, f"❌ Error al eliminar aviso: {e}"

#========================ELIMINAR OT BASE=====================================
def eliminar_ot_base_por_codigo(codigo_ot_base):
    """Eliminar una OT base por código de forma segura"""
    try:
        # Verificar si existe la OT base
        ot_existente = pd.read_sql(
            'SELECT * FROM ot_unicas WHERE codigo_ot_base = ?', 
            conn_ot_unicas, 
            params=(codigo_ot_base,)
        )
        
        if ot_existente.empty:
            return False, "❌ La OT base no existe"
        
        # Verificar si tiene SUB-OT relacionadas
        sub_ot_relacionadas = pd.read_sql(
            'SELECT * FROM ot_sufijos WHERE codigo_ot_base = ?', 
            conn_ot_sufijos, 
            params=(codigo_ot_base,)
        )
        
        if not sub_ot_relacionadas.empty:
            return False, "❌ No se puede eliminar: tiene SUB-OT relacionadas"
        
        # Obtener el código padre para actualizar el aviso
        codigo_padre = ot_existente.iloc[0]['codigo_padre']
        
        # Eliminar la OT base
        ejecutar_consulta_segura(
            conn_ot_unicas,
            'DELETE FROM ot_unicas WHERE codigo_ot_base = ?', 
            (codigo_ot_base,)
        )
        
        # Si tiene aviso relacionado, actualizar su estado
        if codigo_padre:
            ejecutar_consulta_segura(
                conn_avisos,
                'UPDATE avisos SET estado = "INGRESADO", codigo_ot_base = NULL WHERE codigo_padre = ?',
                (codigo_padre,)
            )
        
        log_accion(st.session_state.usuario, "OT_BASE_ELIMINADA", f"Código: {codigo_ot_base}")
        return True, "✅ OT base eliminada correctamente"
        
    except Exception as e:
        return False, f"❌ Error al eliminar OT base: {e}"

#========================ELIMINAR OT SUFIJO====================
def eliminar_sub_ot_por_codigo(codigo_ot_sufijo):
    """Eliminar una SUB-OT por código de forma segura"""
    try:
        # Verificar si existe la SUB-OT
        sub_ot_existente = pd.read_sql(
            'SELECT * FROM ot_sufijos WHERE codigo_ot_sufijo = ?', 
            conn_ot_sufijos, 
            params=(codigo_ot_sufijo,)
        )
        
        if sub_ot_existente.empty:
            return False, "❌ La SUB-OT no existe"
        
        # Obtener información de la SUB-OT
        codigo_ot_base = sub_ot_existente.iloc[0]['codigo_ot_base']
        estado_sub_ot = sub_ot_existente.iloc[0]['estado']
        
        # Eliminar la SUB-OT
        ejecutar_consulta_segura(
            conn_ot_sufijos,
            'DELETE FROM ot_sufijos WHERE codigo_ot_sufijo = ?', 
            (codigo_ot_sufijo,)
        )
        
        # Verificar si era la única SUB-OT de la OT base
        sub_ots_restantes = pd.read_sql(
            'SELECT COUNT(*) as count FROM ot_sufijos WHERE codigo_ot_base = ?', 
            conn_ot_sufijos, 
            params=(codigo_ot_base,)
        )
        
        # Si no hay más SUB-OT, actualizar estado de la OT base
        if sub_ots_restantes.iloc[0]['count'] == 0:
            ejecutar_consulta_segura(
                conn_ot_unicas,
                'UPDATE ot_unicas SET estado = "PROGRAMADO" WHERE codigo_ot_base = ?',
                (codigo_ot_base,)
            )
        
        log_accion(st.session_state.usuario, "SUB_OT_ELIMINADA", f"Código: {codigo_ot_sufijo}")
        return True, "✅ SUB-OT eliminada correctamente"
        
    except Exception as e:
        return False, f"❌ Error al eliminar SUB-OT: {e}"

#=======================OBTENER TODOS LOS CODIGOS DE AVISOS======================
def obtener_todos_los_codigos_avisos():
    """Obtener todos los códigos de avisos"""
    try:
        return pd.read_sql(
            'SELECT codigo_padre, estado, area, equipo FROM avisos ORDER BY codigo_padre DESC', 
            conn_avisos
        )
    except Exception as e:
        st.error(f"Error al obtener avisos: {e}")
        return pd.DataFrame()

#=====================OBTENER TODOS LOS CODIGOS TO BASE============================
def obtener_todos_los_codigos_ot_base():
    """Obtener todos los códigos de OT base"""
    try:
        return pd.read_sql(
            'SELECT codigo_ot_base, estado, area, equipo FROM ot_unicas ORDER BY codigo_ot_base DESC', 
            conn_ot_unicas
        )
    except Exception as e:
        st.error(f"Error al obtener OT base: {e}")
        return pd.DataFrame()

#=====================OBTENER TODOS LOS CODIGOS SUB TO==========================
def obtener_todos_los_codigos_sub_ot():
    """Obtener todos los códigos de SUB-OT"""
    try:
        return pd.read_sql(
            'SELECT codigo_ot_sufijo, codigo_ot_base, estado FROM ot_sufijos ORDER BY codigo_ot_sufijo DESC', 
            conn_ot_sufijos
        )
    except Exception as e:
        st.error(f"Error al obtener SUB-OT: {e}")
        return pd.DataFrame()

#=====================ACTUALIZAR ESTRUCTURA DE OT SUFIJOS=================
def actualizar_estructura_ot_sufijos():
    """Actualizar estructura de ot_sufijos con columnas de imagen"""
    try:
        # Verificar estructura de la tabla ot_sufijos
        estructura = pd.read_sql("PRAGMA table_info(ot_sufijos)", conn_ot_sufijos)
        columnas_existentes = estructura['name'].tolist()
        
        # Columnas que deben existir para imágenes
        columnas_requeridas = [
            'imagen_final_nombre TEXT',
            'imagen_final_datos BLOB',
            'imagen_aviso_nombre TEXT', 
            'imagen_aviso_datos BLOB'
        ]
        
        for columna in columnas_requeridas:
            try:
                nombre_col = columna.split()[0]
                if nombre_col not in columnas_existentes:
                    ejecutar_consulta_segura(conn_ot_sufijos, f'ALTER TABLE ot_sufijos ADD COLUMN {columna}')
                    st.sidebar.info(f"Columna {nombre_col} agregada a ot_sufijos")
            except Exception as e:
                if "duplicate column name" not in str(e):
                    st.sidebar.warning(f"Columna {nombre_col} ya existe o error: {e}")
        
        st.sidebar.success("Estructura de ot_sufijos actualizada")
        
    except Exception as e:
        st.sidebar.error(f"Error al actualizar ot_sufijos: {e}")

# Llama esta función después de inicializar las bases de datos
actualizar_estructura_ot_sufijos()

#===========================BUSCAR Y VINCULAR POR EQUIPO SIMILAR===================
def buscar_equipos_similares(equipo_actual, area_actual, codigo_padre_excluir=None):
    """Buscar equipos similares en OT programadas, pendientes y avisos"""
    try:
        # Obtener código del equipo actual
        codigo_equipo_actual = obtener_codigo_equipo(equipo_actual)
        
        resultados = {
            'ot_programadas': pd.DataFrame(),
            'ot_pendientes': pd.DataFrame(),
            'avisos_ingresados': pd.DataFrame(),
            'avisos_en_proceso': pd.DataFrame(),
            'avisos_programados': pd.DataFrame()
        }
        
        # Buscar por código de equipo en OT programadas
        resultados['ot_programadas'] = pd.read_sql('''
            SELECT codigo_ot_base, area, equipo, codigo_equipo, estado, descripcion_problema, fecha_estimada_inicio
            FROM ot_unicas 
            WHERE codigo_equipo = ? AND estado = "PROGRAMADO"
        ''', conn_ot_unicas, params=(codigo_equipo_actual,))
        
        # Buscar por código de equipo en OT pendientes
        try:
            ot_base_pendientes = pd.read_sql('''
                SELECT codigo_ot_base, area, equipo, codigo_equipo, estado, descripcion_problema, fecha_inicio_mantenimiento
                FROM ot_unicas 
                WHERE codigo_equipo = ? AND estado = "PENDIENTE"
            ''', conn_ot_unicas, params=(codigo_equipo_actual,))
            
            if not ot_base_pendientes.empty:
                sub_ots_list = []
                for _, ot_base in ot_base_pendientes.iterrows():
                    sub_ots = pd.read_sql('''
                        SELECT codigo_ot_sufijo, codigo_ot_base, estado
                        FROM ot_sufijos 
                        WHERE codigo_ot_base = ? AND estado = "PENDIENTE"
                    ''', conn_ot_sufijos, params=(ot_base['codigo_ot_base'],))
                    
                    if not sub_ots.empty:
                        for _, sub_ot in sub_ots.iterrows():
                            sub_ots_list.append({
                                'codigo_ot_sufijo': sub_ot['codigo_ot_sufijo'],
                                'codigo_ot_base': ot_base['codigo_ot_base'],
                                'area': ot_base['area'],
                                'equipo': ot_base['equipo'],
                                'codigo_equipo': ot_base['codigo_equipo'],
                                'estado': sub_ot['estado'],
                                'descripcion_problema': ot_base['descripcion_problema'],
                                'fecha_inicio': ot_base['fecha_inicio_mantenimiento']
                            })
                
                if sub_ots_list:
                    resultados['ot_pendientes'] = pd.DataFrame(sub_ots_list)
        except Exception as e:
            st.error(f"Error en búsqueda de OT pendientes: {e}")
        
        # Buscar en avisos ingresados
        resultados['avisos_ingresados'] = pd.read_sql('''
            SELECT codigo_padre, area, equipo, estado, descripcion_problema, ingresado_el, hay_riesgo
            FROM avisos 
            WHERE equipo LIKE ? AND area = ? AND estado = "INGRESADO"
        ''', conn_avisos, params=(f'%{equipo_actual}%', area_actual))
        
        # Buscar en avisos en proceso
        resultados['avisos_en_proceso'] = pd.read_sql('''
            SELECT codigo_padre, area, equipo, estado, descripcion_problema, ingresado_el, hay_riesgo, codigo_ot_base
            FROM avisos 
            WHERE equipo LIKE ? AND area = ? AND estado = "EN PROCESO"
        ''', conn_avisos, params=(f'%{equipo_actual}%', area_actual))
        
        # NUEVO: Buscar en avisos programados - INCLUIR TODAS LAS COLUMNAS NECESARIAS
        resultados['avisos_programados'] = pd.read_sql('''
            SELECT codigo_padre, area, equipo, estado, descripcion_problema, ingresado_el, 
                   hay_riesgo, codigo_ot_base
            FROM avisos 
            WHERE equipo LIKE ? AND area = ? AND estado = "PROGRAMADO"
        ''', conn_avisos, params=(f'%{equipo_actual}%', area_actual))
        
        # Excluir el aviso actual si se proporciona
        if codigo_padre_excluir:
            for key in ['avisos_ingresados', 'avisos_en_proceso', 'avisos_programados']:
                if not resultados[key].empty:
                    resultados[key] = resultados[key][
                        resultados[key]['codigo_padre'] != codigo_padre_excluir
                    ]
        
        return resultados
        
    except Exception as e:
        st.error(f"Error en búsqueda de equipos similares: {e}")
        return {}
        
def contar_registros_similares(resultados_busqueda):
    """Contar total de registros similares encontrados"""
    total = 0
    for key, df in resultados_busqueda.items():
        if not df.empty:
            total += len(df)
    return total

def vincular_avisos(codigo_padre_principal, codigos_padre_vinculados):
    """Vincular múltiples avisos a una OT principal con manejo inteligente de códigos de mantenimiento"""
    try:
        # Obtener información del aviso principal
        aviso_principal = pd.read_sql(
            'SELECT codigo_ot_base, codigo_mantto, estado FROM avisos WHERE codigo_padre = ?',
            conn_avisos,
            params=(codigo_padre_principal,)
        ).iloc[0]
        
        codigo_ot_base_principal = aviso_principal['codigo_ot_base']
        codigo_mantto_principal = aviso_principal['codigo_mantto']
        estado_principal = aviso_principal['estado']
        
        # Determinar el código OT base que prevalecerá
        codigo_ot_base_final = codigo_ot_base_principal
        
        # Si el aviso principal no tiene OT base, buscar si algún aviso vinculado tiene
        if not codigo_ot_base_principal:
            for codigo_padre in codigos_padre_vinculados:
                aviso_info = pd.read_sql(
                    'SELECT codigo_mantto, codigo_ot_base, estado FROM avisos WHERE codigo_padre = ?',
                    conn_avisos,
                    params=(codigo_padre,)
                ).iloc[0]
                
                if aviso_info['codigo_ot_base'] and aviso_info['estado'] in ['PROGRAMADO', 'PENDIENTE', 'EN PROCESO']:
                    codigo_ot_base_final = aviso_info['codigo_ot_base']
                    st.info(f"📋 La OT base **{codigo_ot_base_final}** prevalecerá (OT existente en aviso vinculado)")
                    break
        
        st.success(f"🔄 **OT Base asociada:** {codigo_ot_base_final if codigo_ot_base_final else 'Ninguna (se creará nueva OT)'}")
        
        # Actualizar todos los avisos (principal + vinculados) con la misma OT base
        todos_los_avisos = [codigo_padre_principal] + codigos_padre_vinculados
        
        for codigo_padre in todos_los_avisos:
            # Obtener información actual del aviso
            aviso_actual = pd.read_sql(
                'SELECT codigo_mantto, comentario, estado FROM avisos WHERE codigo_padre = ?',
                conn_avisos,
                params=(codigo_padre,)
            ).iloc[0]
            
            codigo_mantto_anterior = aviso_actual['codigo_mantto']
            comentario_actual = aviso_actual['comentario']
            estado_anterior = aviso_actual['estado']
            
            # Generar NUEVO código mantto único para cada aviso si es necesario
            nuevo_codigo_mantto = codigo_mantto_anterior  # Por defecto mantener el mismo
            
            # Si el aviso va a cambiar de estado o necesita nuevo código, generarlo
            if estado_anterior == 'INGRESADO' or not codigo_mantto_anterior:
                nuevo_codigo_mantto = generar_codigo_mantto()
                st.info(f"🔄 {codigo_padre}: Nuevo código mantto generado: {nuevo_codigo_mantto}")
            
            # Preparar nuevo comentario
            nuevo_comentario = f"VINCULADO A {codigo_padre_principal} | Código Mantto anterior: {codigo_mantto_anterior} | Estado anterior: {estado_anterior}"
            if comentario_actual:
                nuevo_comentario = comentario_actual + f" | VINCULADO A {codigo_padre_principal} | Código Mantto anterior: {codigo_mantto_anterior} | Estado anterior: {estado_anterior}"
            
            # Actualizar el aviso
            ejecutar_consulta_segura(conn_avisos, '''
                UPDATE avisos SET 
                    estado = "VINCULADO",
                    codigo_ot_base = ?,
                    codigo_mantto = ?,
                    comentario = ?
                WHERE codigo_padre = ?
            ''', (
                codigo_ot_base_final,
                nuevo_codigo_mantto,
                nuevo_comentario,
                codigo_padre
            ))
            
            # Registrar el cambio si es diferente
            if codigo_mantto_anterior != nuevo_codigo_mantto:
                st.info(f"📝 {codigo_padre}: {codigo_mantto_anterior} → {nuevo_codigo_mantto}")
        
        # Resumen final
        st.success(f"✅ **Vinculación completada exitosamente**")
        st.success(f"🔗 **Avisos vinculados:** {len(todos_los_avisos)}")
        st.success(f"📄 **OT Base asociada:** {codigo_ot_base_final if codigo_ot_base_final else 'Por asignar'}")
        
        # Mostrar resumen de cambios
        st.info("**📋 Resumen de cambios:**")
        for codigo_padre in todos_los_avisos:
            aviso_final = pd.read_sql(
                'SELECT codigo_mantto, estado FROM avisos WHERE codigo_padre = ?',
                conn_avisos,
                params=(codigo_padre,)
            ).iloc[0]
            
            st.write(f"• {codigo_padre}: {aviso_final['codigo_mantto']} - {aviso_final['estado']}")
        
        return True, "✅ Avisos vinculados correctamente con códigos de mantenimiento únicos"
        
    except Exception as e:
        return False, f"❌ Error al vincular avisos: {e}"

def debug_busqueda_ot_programadas(equipo_actual, area_actual):
    """Función de debug para ver qué hay en las OT programadas"""
    try:
        # Ver todas las OT programadas en el área
        todas_ot_area = pd.read_sql('''
            SELECT codigo_ot_base, area, equipo, codigo_equipo, estado
            FROM ot_unicas 
            WHERE area = ? AND estado = "PROGRAMADO"
            ORDER BY equipo
        ''', conn_ot_unicas, params=(area_actual,))
        
        # Ver equipos únicos en el área
        equipos_unicos = pd.read_sql('''
            SELECT DISTINCT equipo, codigo_equipo
            FROM ot_unicas 
            WHERE area = ? AND estado = "PROGRAMADO"
            ORDER BY equipo
        ''', conn_ot_unicas, params=(area_actual,))
        
        return todas_ot_area, equipos_unicos
        
    except Exception as e:
        st.error(f"Error en debug: {e}")
        return pd.DataFrame(), pd.DataFrame()

# ==================== FUNCIONES DE BÚSQUEDA CORREGIDAS ====================

def buscar_ot_por_equipo(equipo):
    """Buscar OT por nombre de equipo - CORREGIDO con codigo_equipo"""
    try:
        # Buscar en ot_unicas usando codigo_equipo
        return pd.read_sql(
            'SELECT * FROM ot_unicas WHERE codigo_equipo LIKE ? ORDER BY codigo_ot_base DESC', 
            conn_ot_unicas,
            params=(f'%{equipo}%',)
        )
    except Exception as e:
        st.error(f"Error en búsqueda por equipo: {e}")
        return pd.DataFrame()

#==================BUSCAR OT POR EQUIPOS=====================
def buscar_ot_por_nombre_equipo(nombre_equipo):
    """Buscar OT por nombre de equipo en avisos - CORREGIDO"""
    try:
        # Primero buscar en avisos para obtener códigos padre relacionados con el equipo
        avisos_relacionados = pd.read_sql(
            'SELECT codigo_padre FROM avisos WHERE equipo LIKE ?', 
            conn_avisos,
            params=(f'%{nombre_equipo}%',)
        )
        
        if not avisos_relacionados.empty:
            # Obtener los códigos padre
            codigos_padre = avisos_relacionados['codigo_padre'].tolist()
            
            # Buscar en ot_unicas usando los códigos padre
            placeholders = ','.join('?' for _ in codigos_padre)
            query = f'SELECT * FROM ot_unicas WHERE codigo_padre IN ({placeholders}) ORDER BY codigo_ot_base DESC'
            
            return pd.read_sql(query, conn_ot_unicas, params=codigos_padre)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error en búsqueda por nombre de equipo: {e}")
        return pd.DataFrame()

def buscar_ot_unicas_por_codigo(codigo):
    """Buscar OT únicas por código (parcial o completo)"""
    try:
        return pd.read_sql(
            'SELECT * FROM ot_unicas WHERE codigo_ot_base LIKE ? ORDER BY codigo_ot_base DESC', 
            conn_ot_unicas,
            params=(f'%{codigo}%',)
        )
    except Exception as e:
        st.error(f"Error en búsqueda por código: {e}")
        return pd.DataFrame()

def buscar_ot_por_sistema(sistema):
    """Buscar OT por sistema"""
    try:
        return pd.read_sql(
            'SELECT * FROM ot_unicas WHERE sistema LIKE ? ORDER BY codigo_ot_base DESC', 
            conn_ot_unicas,
            params=(f'%{sistema}%',)
        )
    except Exception as e:
        st.error(f"Error en búsqueda por sistema: {e}")
        return pd.DataFrame()

def buscar_ot_por_estado(estado):
    """Buscar OT por estado"""
    try:
        return pd.read_sql(
            'SELECT * FROM ot_unicas WHERE estado = ? ORDER BY codigo_ot_base DESC', 
            conn_ot_unicas,
            params=(estado,)
        )
    except Exception as e:
        st.error(f"Error en búsqueda por estado: {e}")
        return pd.DataFrame()

def obtener_todas_ot_unicas():
    """Obtener todas las OT únicas"""
    try:
        return pd.read_sql(
            'SELECT * FROM ot_unicas ORDER BY codigo_ot_base DESC', 
            conn_ot_unicas
        )
    except Exception as e:
        st.error(f"Error al obtener OT únicas: {e}")
        return pd.DataFrame()

def obtener_info_completa_ot():
    """Obtener información completa de OT uniendo con avisos"""
    try:
        # Obtener OT únicas
        ot_unicas = obtener_todas_ot_unicas()
        
        if ot_unicas.empty:
            return pd.DataFrame()
        
        # Obtener avisos relacionados
        avisos = pd.read_sql('SELECT codigo_padre, area, equipo, descripcion_problema FROM avisos', conn_avisos)
        
        if not avisos.empty:
            # Hacer merge de los datos
            resultado = ot_unicas.merge(
                avisos, 
                on='codigo_padre', 
                how='left',
                suffixes=('', '_aviso')
            )
            return resultado
        else:
            return ot_unicas
            
    except Exception as e:
        st.error(f"Error al obtener información completa: {e}")
        return pd.DataFrame()

def obtener_codigos_equipo_para_busqueda():
    """Obtener lista de códigos de equipo únicos para búsqueda - CORREGIDO"""
    try:
        # Obtener códigos de equipo únicos de la tabla ot_unicas
        codigos_equipo = pd.read_sql(
            'SELECT DISTINCT codigo_equipo FROM ot_unicas WHERE codigo_equipo IS NOT NULL AND codigo_equipo != "" ORDER BY codigo_equipo', 
            conn_ot_unicas
        )
        return codigos_equipo
    except Exception as e:
        st.error(f"Error al obtener códigos de equipo: {e}")
        return pd.DataFrame()

def obtener_nombres_equipo_para_busqueda():
    """Obtener lista de nombres de equipo únicos para búsqueda"""
    try:
        # Obtener nombres de equipo únicos de la tabla avisos
        nombres_equipo = pd.read_sql(
            'SELECT DISTINCT equipo FROM avisos WHERE equipo IS NOT NULL AND equipo != "" ORDER BY equipo', 
            conn_avisos
        )
        return nombres_equipo
    except Exception as e:
        st.error(f"Error al obtener nombres de equipo: {e}")
        return pd.DataFrame()

#===============Función Adicional para Ver Estados====================
def ver_estado_actual_ot(codigo_ot_base):
    """Ver el estado actual de una OT y sus relaciones"""
    try:
        # Obtener OT base
        ot_base = pd.read_sql(
            'SELECT * FROM ot_unicas WHERE codigo_ot_base = ?',
            conn_ot_unicas,
            params=(codigo_ot_base,)
        )
        
        # Obtener SUB-OT relacionadas
        sub_ots = pd.read_sql(
            'SELECT codigo_ot_sufijo, estado FROM ot_sufijos WHERE codigo_ot_base = ?',
            conn_ot_sufijos,
            params=(codigo_ot_base,)
        )
        
        # Obtener aviso relacionado
        if not ot_base.empty and ot_base.iloc[0]['codigo_padre']:
            aviso = pd.read_sql(
                'SELECT codigo_padre, estado FROM avisos WHERE codigo_padre = ?',
                conn_avisos,
                params=(ot_base.iloc[0]['codigo_padre'],)
            )
        else:
            aviso = pd.DataFrame()
        
        return ot_base, sub_ots, aviso
        
    except Exception as e:
        st.error(f"Error al ver estado: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ==================== FUNCIONES DE CÁLCULO DE ANTIGÜEDAD MEJORADAS ====================
def calcular_antiguedad(fecha_ingreso):
    if isinstance(fecha_ingreso, str):
        try:
            fecha_ingreso = date.fromisoformat(fecha_ingreso)
        except:
            return 0
    hoy = date.today()
    return (hoy - fecha_ingreso).days

def actualizar_antiguedades_lotes():
    """Actualizar antigüedades en lotes para evitar bloqueos - VERSIÓN MEJORADA"""
    try:
        # Crear conexión temporal
        conn_temp = sqlite3.connect('data/avisos.db', check_same_thread=False, timeout=30)
        
        # Obtener todos los registros
        registros = pd.read_sql('SELECT id, ingresado_el FROM avisos', conn_temp)
        conn_temp.close()
        
        # Procesar en lotes pequeños
        lote_size = 10
        actualizaciones_realizadas = 0
        
        for i in range(0, len(registros), lote_size):
            lote = registros[i:i + lote_size]
            
            # Nueva conexión para cada lote
            conn_lote = sqlite3.connect('data/avisos.db', check_same_thread=False, timeout=30)
            
            for _, registro in lote.iterrows():
                if registro['ingresado_el']:
                    antiguedad = calcular_antiguedad(registro['ingresado_el'])
                    try:
                        ejecutar_consulta_segura(
                            conn_lote,
                            'UPDATE avisos SET antiguedad=? WHERE id=?', 
                            (antiguedad, registro['id'])
                        )
                        actualizaciones_realizadas += 1
                    except Exception as e:
                        print(f"Error actualizando registro {registro['id']}: {e}")
                        # Continuar con el siguiente registro
            
            conn_lote.close()
            time.sleep(0.1)  # Pequeña pausa entre lotes
        
        print(f"✅ Antigüedades actualizadas por lotes correctamente: {actualizaciones_realizadas} registros")
        return True
        
    except Exception as e:
        print(f"❌ Error en actualizar_antiguedades_lotes: {e}")
        return False

def actualizar_antiguedades():
    """Actualizar antigüedad de registros existentes - VERSIÓN SEGURA"""
    try:
        # Usar una conexión temporal
        conn_temp = sqlite3.connect('data/avisos.db', check_same_thread=False, timeout=30)
        registros = pd.read_sql('SELECT id, ingresado_el FROM avisos', conn_temp)
        
        actualizaciones = 0
        for _, registro in registros.iterrows():
            if registro['ingresado_el']:
                antiguedad = calcular_antiguedad(registro['ingresado_el'])
                try:
                    ejecutar_consulta_segura(
                        conn_temp,
                        'UPDATE avisos SET antiguedad=? WHERE id=?', 
                        (antiguedad, registro['id'])
                    )
                    actualizaciones += 1
                except sqlite3.OperationalError:
                    # Si hay bloqueo, continuar con el siguiente registro
                    continue
        
        conn_temp.close()
        print(f"✅ Antigüedades actualizadas: {actualizaciones} registros")
        return True
        
    except Exception as e:
        print(f"❌ Error en actualizar_antiguedades: {e}")
        return False

# ==================== INICIALIZAR SISTEMA ====================
if 'autenticado' not in st.session_state:
    st.session_state.autenticado = False

# Inicializar sistema de login
init_login_db()
setup_logging()

# Verificar autenticación - MOSTRAR LOGIN SI NO ESTÁ AUTENTICADO
if not st.session_state.autenticado:
    mostrar_login()
    st.stop()

# ==================== APLICACIÓN PRINCIPAL ====================
st.title("🔧 Sistema de Gestión de Mantenimiento")
st.markdown("**Sistema integral de avisos, órdenes de trabajo y gestión de equipos**")

#===========================Mapeo de áreas a equipos (con opción OTRO)========================================
EQUIPOS_POR_AREA = {
    "COCCION": ["DIGESTOR 1","TORNILLO HELICOIDAL 1","DIGESTOR 2","TORNILLO HELICOIDAL 2","DIGESTOR 3","TORNILLO HELICOIDAL 3","DIGESTOR 4","TORNILLO HELICOIDAL 4","DIGESTOR 5","TORNILLO HELICOIDAL 5","DIGESTOR 6","TORNILLO HELICOIDAL 6","DIGESTOR 7","TORNILLO HELICOIDAL 7","DIGESTOR 8","TORNILLO HELICOIDAL 8","DIGESTOR 9","TORNILLO HELICOIDAL 9","PERCOLADOR 1","PERCOLADOR 2","EXTRACTOR DE VAHOS 1","LAVADOR DE VAHOS 1","EXTRACTOR DE VAHOS 2","LAVADOR DE VAHOS 2","TANQUE DE DECANTACION","TH TANQUE DE HIDROLISIS","CICLON DE VAPOR","TANQUE DE HIDROLISIS","BOMBA CENTRIFUGA 1 VAHOS 1","BOMBA CENTRIFUGA 2 VAHOS 2","BOMBA CENTRIFUGA 2","TANQUE DE LAVADO","OTRO"],
    "TRITURADO": ["TH DE TOLVA DE CONCRETO","TH DE TOLVA AL TRITURADOR","TRITURADOR GRANDE","TH DE DESCARGA DE TRITURADOR","TRITURADOR PEQUEÑO", "OTRO"],
    "SECADO": ["TH ALIMENTADOR SECADOR 1","TAMBOR DE SECADOR 1","CAMARA DE FUEGO 1","TREN DE GAS DEL SECADOR 1","EXTRACTOR DEL SECADOR 1","TH DE SECADOR 1 A LANZA HARINA","TH DE SECADOR 1 A PURIFICADOR","TH SECADOR 1 AL ENFRIADOR","TH DE ENFRIADOR AL SECADOR 1","ENFRIADOR DE HARINA","EXTRACTOR DEL ENFRIADOR","TH DE ENFRIADOR AL PURIFICADOR","PURIFICADOR DE HARINA","TH DE PURIFICADOR A LANZA HARINA","TH CICLON DE FINOS","LANZA HARINA","VENTILADOR DE CAMARA SEC 1","VENTILADOR DE GASES SEC 1","CICLON DE FINOS 1","CICLON DE FINOS 2","CAMARA DE FUEGO 2","TREN DE GAS DEL SECADOR 2","EXTRACTOR DEL SECADOR 2","TAMBOR DE SECADOR 2","TH CICLON DE FINOS 2","TH 1 ALIMENTADOR SEC 2","TH 2 ALIMENTADOR SEC 2","TH 1 SALIDA SEC 2","TH 2 SALIDA SEC 2","TH 3 SALIDA SEC 2","TH 1 REPROCESO SEC 2","TH 2 REPROCESO SEC 2", "OTRO"],
    "MOLINO": ["TH ALIMENTADOR DE MOLINO 1","TH DE MOLINO 1 A MOLINO 2","TH DE MOLINO 2 A ZARANDA","TH DE CICLON DE FINOS A ZARANDA","TH DE CICLON 1 AL TH 1","MOLINO DE MATILLOS 1","MOLINO DE MARTILLOS 2","ZARANDA DE HARINA","CICLON DE LANZA HARINA","CICLON DE FINOS","VENTILADOR AEREO 1","VENTILADOR AEREO 2","BOMBA DOSIFICADORA 1","BOMBA DOSIFICADORA 2","BOMBA DOSIFICADORA 3", "OTRO"],
    "CALDERO": ["QUEMADOR DE GAS DE 900BHP","CALDERA DE 900BHP","TREN DE GAS DE CALDERA DE 900","CALDERA DE 400BHP","TREN DE GAS DE CALDERA DE 400BHP","QUEMADOR DE GAS DE 400BHP","MANIFOLD DE VAPOR", "OTRO"],
    "PLANTA": ["INSECTOCUTOR", "OTRO"]
}

# ==================== CÓDIGOS FIJOS PARA EQUIPOS ====================
CODIGOS_EQUIPOS = {
    # COCCION
    "DIGESTOR 1": "DG-COC-01",
    "TORNILLO HELICOIDAL 1": "TH-COC-01",
    "DIGESTOR 2": "DG-COC-02",
    "TORNILLO HELICOIDAL 2": "TH-COC-02",
    "DIGESTOR 3": "DG-COC-03",
    "TORNILLO HELICOIDAL 3": "TH-COC-03",
    "DIGESTOR 4": "DG-COC-04",
    "TORNILLO HELICOIDAL 4": "TH-COC-04",
    "DIGESTOR 5": "DG-COC-05",
    "TORNILLO HELICOIDAL 5": "TH-COC-05",
    "DIGESTOR 6": "DG-COC-06",
    "TORNILLO HELICOIDAL 6": "TH-COC-06",
    "DIGESTOR 7": "DG-COC-07",
    "TORNILLO HELICOIDAL 7": "TH-COC-07",
    "DIGESTOR 8": "DG-COC-08",
    "TORNILLO HELICOIDAL 8": "TH-COC-08",
    "DIGESTOR 9": "DG-COC-09",
    "TORNILLO HELICOIDAL 9": "TH-COC-09",
    "PERCOLADOR 1": "PE-COC-01",
    "PERCOLADOR 2": "PE-COC-02",
    "EXTRACTOR DE VAHOS 1": "EX-COC-01",
    "LAVADOR DE VAHOS 1": "LV-COC-01",
    "EXTRACTOR DE VAHOS 2": "EX-COC-02",
    "LAVADOR DE VAHOS 2": "LV-COC-02",
    "TANQUE DE DECANTACION": "TD-COC-01",
    "TH TANQUE DE HIDROLISIS": "TH-COC-11",
    "CICLON DE VAPOR": "CL-COC-01",
    "TANQUE DE HIDROLISIS": "TI-COC-01",
    "BOMBA CENTRIFUGA 1 VAHOS 1": "BC-COC-01",
    "BOMBA CENTRIFUGA 2 VAHOS 2": "BC-COC-02",
    "BOMBA CENTRIFUGA 2": "BC-COC-03",
    "TANQUE DE LAVADO": "TK-COC-01",
    
    # TRITURADO
    "TH DE TOLVA DE CONCRETO": "TH-TTR-01",
    "TH DE TOLVA AL TRITURADOR": "TH-TTR-02",
    "TRITURADOR GRANDE": "TH-TTR-03",
    "TH DE DESCARGA DE TRITURADOR": "TR-TTR-01",
    "TRITURADOR PEQUEÑO": "LNP-TRT-01",
    
    # SECADO
    "TH ALIMENTADOR SECADOR 1": "TH-SEC-01",
    "TAMBOR DE SECADOR 1": "TB-SEC-01",
    "CAMARA DE FUEGO 1": "CF-SEC-01",
    "TREN DE GAS DEL SECADOR 1": "TR-SEC-01",
    "EXTRACTOR DEL SECADOR 1": "EX-SEC-01",
    "TH DE SECADOR 1 A LANZA HARINA": "TH-SEC-02",
    "TH DE SECADOR 1 A PURIFICADOR": "TH-SEC-03",
    "TH SECADOR 1 AL ENFRIADOR": "TH-SEC-04",
    "TH DE ENFRIADOR AL SECADOR 1": "TH-SEC-05",
    "ENFRIADOR DE HARINA": "EF-SEC-01",
    "EXTRACTOR DEL ENFRIADOR": "EX-SEC-02",
    "TH DE ENFRIADOR AL PURIFICADOR": "TH-SEC-06",
    "PURIFICADOR DE HARINA": "PU-SEC-01",
    "TH DE PURIFICADOR A LANZA HARINA": "TH-SEC-07",
    "TH CICLON DE FINOS": "TH-SEC-08",
    "LANZA HARINA": "LZ-SEC-03",
    "VENTILADOR DE CAMARA SEC 1": "VT-SEC-04",
    "VENTILADOR DE GASES SEC 1": "VT-SEC-05",
    "CICLON DE FINOS 1": "CL-SEC-01",
    "CICLON DE FINOS 2": "CL-SEC-02",
    "CAMARA DE FUEGO 2": "CF-SEC-02",
    "TREN DE GAS DEL SECADOR 2": "TR-SEC-02",
    "EXTRACTOR DEL SECADOR 2": "EX-SEC-03",
    "TAMBOR DE SECADOR 2": "TB-SEC-02",
    "TH CICLON DE FINOS 2": "TH-SEC-09",
    "TH 1 ALIMENTADOR SEC 2": "TH-SEC-10",
    "TH 2 ALIMENTADOR SEC 2": "TH-SEC-11",
    "TH 1 SALIDA SEC 2": "TH-SEC-12",
    "TH 2 SALIDA SEC 2": "TH-SEC-13",
    "TH 3 SALIDA SEC 2": "TH-SEC-14",
    "TH 1 REPROCESO SEC 2": "TH-SEC-15",
    "TH 2 REPROCESO SEC 2": "TH-SEC-16",
    
    # MOLINO
    "TH ALIMENTADOR DE MOLINO 1": "TH-MOL-01",
    "TH DE MOLINO 1 A MOLINO 2": "TH-MOL-02",
    "TH DE MOLINO 2 A ZARANDA": "TH-MOL-03",
    "TH DE CICLON DE FINOS A ZARANDA": "TH-MOL-04",
    "TH DE CICLON 1 AL TH 1": "TH-MOL-05",
    "MOLINO DE MATILLOS 1": "ML-MOL-01",
    "MOLINO DE MARTILLOS 2": "ML-MOL-02",
    "ZARANDA DE HARINA": "ZR-MOL-01",
    "CICLON DE LANZA HARINA": "CL-MOL-01",
    "CICLON DE FINOS": "CL-MOL-02",
    "VENTILADOR AEREO 1": "VE-MOL-01",
    "VENTILADOR AEREO 2": "VE-MOL-02",
    "BOMBA DOSIFICADORA 1": "BD-MOL-01",
    "BOMBA DOSIFICADORA 2": "BD-MOL-02",
    "BOMBA DOSIFICADORA 3": "BD-MOL-03",
    
    # CALDERO
    "QUEMADOR DE GAS DE 900BHP": "QM-CAL-03",
    "CALDERA DE 900BHP": "CD-CAL-03",
    "TREN DE GAS DE CALDERA DE 900": "TG-CAL-03",
    "CALDERA DE 400BHP": "CD-CAL-04",
    "TREN DE GAS DE CALDERA DE 900": "TG-CAL-04",
    "QUEMADOR DE GAS DE 400BHP": "QM-CAL-04",
    "MANIFOLD DE VAPOR": "MF-CAL-01",
    
    # PLANTA
    "INSECTOCUTOR": "IN-PL-01",
    
    # OTRO (genérico)
    "OTRO": "ESTRUCTURAS"
}

# ==================== FUNCIÓN PARA OBTENER CÓDIGO DE EQUIPO ====================
def obtener_codigo_equipo(nombre_equipo):
    """Obtener el código fijo para un equipo"""
    if nombre_equipo in CODIGOS_EQUIPOS:
        return CODIGOS_EQUIPOS[nombre_equipo]
    else:
        # Para equipos no listados, generar código basado en el nombre
        # Usar las primeras 2 letras del nombre y hash único
        prefijo = nombre_equipo[:2].upper() if len(nombre_equipo) >= 2 else "EQ"
        hash_base = hashlib.md5(nombre_equipo.encode()).hexdigest()[:4].upper()
        return f"{prefijo}-GEN-{hash_base}"

#=======================Generar códigos automáticos para AVISOS=====================================
def generar_codigo_padre():
    ultimo_codigo = pd.read_sql(
        'SELECT codigo_padre FROM avisos WHERE codigo_padre LIKE "CODP-%" ORDER BY id DESC LIMIT 1', 
        conn_avisos
    )
    if len(ultimo_codigo) > 0:
        try:
            ultimo_numero = int(ultimo_codigo.iloc[0]['codigo_padre'].split('-')[1])
            nuevo_numero = ultimo_numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    return f"CODP-{nuevo_numero:08d}"

# Ejecutar actualización de antigüedades de forma segura
try:
    actualizar_antiguedades_lotes()
except Exception as e:
    print(f"⚠️ No se pudieron actualizar antigüedades: {e}")

# ==================== PESTAÑAS PRINCIPALES ====================
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "📝 NUEVO AVISO", 
    "📋 AVISOS INGRESADOS", 
    "🛠️ AVISOS EN TRATAMIENTO",
    "🚀 CREAR OT",
    "📊 Registro TOTAL",
    "⏳ OT PENDIENTES", 
    "🏁 OT CULMINADAS",
    "🔍 BUSCAR OT",
    "🗑️ ELIMINAR REGISTROS"
])

#=========================PESTAÑA 1: NUEVO AVISO (USANDO BASE AVISOS)==================================
with tab1:
    st.header("➕ Registrar Nuevo Aviso de Mantenimiento")
    
    # Inicializar session_state para el área y equipo
    if 'area_seleccionada' not in st.session_state:
        st.session_state.area_seleccionada = "COCCION"
    if 'equipo_seleccionado' not in st.session_state:
        st.session_state.equipo_seleccionado = EQUIPOS_POR_AREA["COCCION"][0]
    
    # ÁREA Y EQUIPO FUERA DEL FORM
    st.subheader("🔧 Selección de Área y Equipo")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Selección de área
        area = st.selectbox(
            "Área *",
            list(EQUIPOS_POR_AREA.keys()),
            key="area_select"
        )
        
        # Actualizar session_state
        st.session_state.area_seleccionada = area
    
    with col2:
        # Obtener equipos disponibles para el área seleccionada
        equipos_disponibles = EQUIPOS_POR_AREA.get(area, [])
        
        # Selección de equipo
        equipo = st.selectbox(
            "Equipo *",
            equipos_disponibles,
            key="equipo_select"
        )
        
        # Actualizar session_state
        st.session_state.equipo_seleccionado = equipo
        if 'equipo_seleccionado' in st.session_state and st.session_state.equipo_seleccionado:
           codigo_equipo_aviso = obtener_codigo_equipo(st.session_state.equipo_seleccionado)
           st.info(f"🔢 **Código de equipo asignado:** `{codigo_equipo_aviso}`")

    # FORMULARIO (sin área y equipo)
    with st.form("registro_mantto_form", clear_on_submit=True):
        st.subheader("📝 Información del Reporte")
        
        col3, col4 = st.columns(2)
        
        with col3:
            hay_riesgo = st.selectbox("¿Hay riesgo para las personas? *", 
                ["NO", "SI"]
            )
            
        with col4:
            # CAMPO OCULTO - Se asigna automáticamente el usuario de sesión
            ingresado_por = st.session_state.nombre_completo
            st.info(f"👤 **Reportado por:** {ingresado_por}")

        st.subheader("📝 Descripción del Problema")
        descripcion_problema = st.text_area(
            "Descripción detallada del problema o falla *",
            placeholder="Describa el problema de manera detallada...",
            height=100
        )
        
        # SUBIDA DE IMAGEN
        st.subheader("🖼️ Adjuntar Imagen (Opcional)")
        imagen_subida = st.file_uploader(
            "Seleccione una imagen del problema", 
            type=['jpg', 'jpeg', 'png', 'gif'],
            help="Formatos permitidos: JPG, JPEG, PNG, GIF"
        )
        
        # Mostrar vista previa si se subió una imagen
        if imagen_subida is not None:
            st.image(imagen_subida, caption="Vista previa de la imagen", width=300)
        
        submitted = st.form_submit_button("💾 Registrar Aviso de Mantenimiento")
        
        if submitted:
            # Usar los valores actuales de área y equipo (que están fuera del form)
            area_actual = st.session_state.area_seleccionada
            equipo_actual = st.session_state.equipo_seleccionado
            
            if area_actual and descripcion_problema and equipo_actual:
                try:
                    # Generar códigos
                    codigo_padre = generar_codigo_padre()
                    codigo_mantto = generar_codigo_mantto()
                    
                    # Fecha actual
                    fecha_actual = date.today()
                    
                    # Procesar imagen si se subió
                    imagen_aviso_nombre = None
                    imagen_aviso_datos = None
                    
                    if imagen_subida is not None:
                        imagen_aviso_nombre = imagen_subida.name
                        imagen_aviso_datos = imagen_subida.read()
                    
                    ejecutar_consulta_segura(conn_avisos, '''
                        INSERT INTO avisos (
                            codigo_padre, codigo_mantto, estado, antiguedad, area, equipo, 
                            descripcion_problema, ingresado_por, ingresado_el, hay_riesgo,
                            imagen_aviso_nombre, imagen_aviso_datos
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        codigo_padre, codigo_mantto, 'INGRESADO', 0, area_actual, equipo_actual,
                        descripcion_problema, st.session_state.nombre_completo, fecha_actual, hay_riesgo,
                        imagen_aviso_nombre, imagen_aviso_datos
                    ))
                    
                    st.success("✅ **Se registró correctamente**")
                    st.success(f"👤 **Reportado por:** {st.session_state.nombre_completo}")
                    log_accion(st.session_state.usuario, "NUEVO_AVISO", f"Código: {codigo_padre}, Área: {area_actual}")
                        
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    log_accion(st.session_state.usuario, "ERROR_AVISO", f"Error: {str(e)}")
            else:
                st.warning("⚠️ Los campos marcados con * son obligatorios")

#==============================PESTAÑA 2: AVISOS INGRESADOS==================================
with tab2:
    st.header("📋 Avisos Ingresados - Pendientes de Tratamiento")
    
    df_ingresados = obtener_avisos_ingresados()
    
    if not df_ingresados.empty:
        # Mostrar estadísticas rápidas - CORREGIR
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Avisos", len(df_ingresados))
        with col2:
            st.metric("Con Riesgo", len(df_ingresados[df_ingresados['hay_riesgo'] == 'SI']))
        with col3:
            # ✅ CORREGIDO - usar imagen_aviso_nombre en lugar de imagen_nombre
            if 'imagen_aviso_nombre' in df_ingresados.columns:
                avisos_con_imagen = len(df_ingresados[df_ingresados['imagen_aviso_nombre'].notna()])
            else:
                avisos_con_imagen = 0
            st.metric("Con Imagen", avisos_con_imagen)
        
        # Mostrar tabla principal
        columnas_mostrar = ['codigo_padre', 'codigo_mantto', 'estado', 'antiguedad', 'area', 'equipo', 
                           'hay_riesgo', 'ingresado_por', 'ingresado_el']
        st.dataframe(df_ingresados[columnas_mostrar], use_container_width=True)
        
        # SECCIÓN PARA VER IMÁGENES - CORREGIR
        st.subheader("🖼️ Visualizar Imágenes de Avisos")
        
        # ✅ CORREGIDO - usar imagen_aviso_nombre
        if 'imagen_aviso_nombre' in df_ingresados.columns:
            avisos_con_imagen = df_ingresados[df_ingresados['imagen_aviso_nombre'].notna()]
        else:
            avisos_con_imagen = pd.DataFrame()
        
        if not avisos_con_imagen.empty:
            codigo_seleccionado = st.selectbox(
                "Seleccione un aviso para ver la imagen:",
                avisos_con_imagen['codigo_padre'].tolist(),
                key="visor_imagen_ingresados"
            )
            
            if codigo_seleccionado:
                # ✅ CORREGIDO - usar imagen_aviso_nombre y imagen_aviso_datos
                resultado = conn_avisos.execute(
                    'SELECT imagen_aviso_nombre, imagen_aviso_datos FROM avisos WHERE codigo_padre = ?', 
                    (codigo_seleccionado,)
                ).fetchone()
                
                if resultado and resultado[1]:
                    imagen_nombre, imagen_datos = resultado
                    registro = df_ingresados[df_ingresados['codigo_padre'] == codigo_seleccionado].iloc[0]
                    
                    col1, col2 = st.columns([1, 2])
                    
                    with col1:
                        st.subheader("📋 Información del Aviso")
                        st.write(f"**Código:** {registro['codigo_padre']}")
                        st.write(f"**Área:** {registro['area']}")
                        st.write(f"**Equipo:** {registro['equipo']}")
                        st.write(f"**Estado:** {registro['estado']}")
                        st.write(f"**Riesgo:** {registro['hay_riesgo']}")
                        st.write(f"**Descripción:** {registro['descripcion_problema']}")
                    
                    with col2:
                        st.subheader(f"🖼️ {imagen_nombre}")
                        st.image(imagen_datos, use_column_width=True)
                        st.caption(f"Imagen adjunta al aviso {codigo_seleccionado}")
        else:
            st.info("📷 No hay avisos con imágenes adjuntas")
        
        # Gráfico de distribución por área
        st.subheader("📈 Distribución por Área")
        area_counts = df_ingresados['area'].value_counts()
        st.bar_chart(area_counts)
        
    else:
        st.info("🎉 No hay avisos ingresados pendientes de tratamiento.")

#=======================PESTAÑA 3: AVISOS EN TRATAMIENTO - CON FEEDBACK=====================
with tab3:
    st.header("🛠️ Avisos en Tratamiento")
    
    # Obtener avisos en estado INGRESADO para tratarlos
    df_ingresados = obtener_avisos_ingresados()
    
    if not df_ingresados.empty:
        # Mostrar estadísticas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Avisos por Tratar", len(df_ingresados))
        with col2:
            st.metric("Con Riesgo", len(df_ingresados[df_ingresados['hay_riesgo'] == 'SI']))
        with col3:
            if 'imagen_aviso_nombre' in df_ingresados.columns:
                avisos_con_imagen = len(df_ingresados[df_ingresados['imagen_aviso_nombre'].notna()])
            else:
                avisos_con_imagen = 0
            st.metric("Con Imagen", avisos_con_imagen)
        
        # Mostrar tabla principal de avisos INGRESADOS
        st.subheader("📋 Avisos Ingresados para Tratar")
        columnas_mostrar = ['codigo_padre', 'codigo_mantto', 'antiguedad', 'area', 'equipo', 
                           'hay_riesgo', 'ingresado_por', 'ingresado_el']
        st.dataframe(df_ingresados[columnas_mostrar], use_container_width=True)
        
        # FORMULARIO DE TRATAMIENTO PARA AVISOS INGRESADOS
        st.subheader("✏️ Tratar Aviso - Crear OT Programada")
        
        # SELECTOR PARA ELEGIR AVISO
        st.write("### 🔍 Seleccionar Aviso para Tratar")
        avisos_disponibles = df_ingresados['codigo_padre'].tolist()
        
        # Crear una lista más descriptiva para el selector
        opciones_avisos = []
        for _, aviso in df_ingresados.iterrows():
            descripcion = f"{aviso['codigo_padre']} - {aviso['equipo']} - {aviso['area']}"
            opciones_avisos.append(descripcion)
        
        aviso_seleccionado_desc = st.selectbox(
            "Seleccione un aviso para comenzar el tratamiento:",
            opciones_avisos,
            key="selector_aviso_tratar"
        )
        
        # Extraer el código del aviso seleccionado
        if aviso_seleccionado_desc:
            codigo_tratar = aviso_seleccionado_desc.split(" - ")[0]
            
            # Obtener datos FRESCOS del registro desde la base de datos
            registro_actual_df = pd.read_sql(
                'SELECT * FROM avisos WHERE codigo_padre = ?', 
                conn_avisos,
                params=(codigo_tratar,)
            )
            
            if not registro_actual_df.empty:
                registro_actual = registro_actual_df.iloc[0]
                equipo_actual = registro_actual['equipo']
                area_actual = registro_actual['area']
                
                # ==================== SECCIÓN DE FEEDBACK - EQUIPOS SIMILARES ====================
                st.markdown("---")
                st.subheader("🔍 Feedback - Registros Similares Encontrados")

                with st.spinner("Buscando equipos similares..."):
                    resultados_similares = buscar_equipos_similares(equipo_actual, area_actual, codigo_tratar)
                    total_similares = contar_registros_similares(resultados_similares)

                # ==================== DEBUG - SOLO SI NO ENCUENTRA RESULTADOS ====================
                if total_similares == 0:
                    with st.expander("🔧 Debug - Ver datos de OT programadas en esta área"):
                        st.write(f"**Buscando para:** Equipo='{equipo_actual}', Área='{area_actual}'")
                        
                        todas_ot, equipos_unicos = debug_busqueda_ot_programadas(equipo_actual, area_actual)
                        
                        if not todas_ot.empty:
                            st.write("**Todas las OT programadas en esta área:**")
                            st.dataframe(todas_ot, use_container_width=True)
                            
                            st.write("**Equipos únicos en OT programadas:**")
                            st.dataframe(equipos_unicos, use_container_width=True)
                            
                            st.info(f"Total de OT programadas en {area_actual}: {len(todas_ot)}")
                        else:
                            st.warning(f"No hay OT programadas en el área {area_actual}")

                # ==================== MOSTRAR RESULTADOS ====================
                if total_similares > 0:
                    st.warning(f"⚠️ Se encontraron {total_similares} registros similares para **{equipo_actual}** en **{area_actual}**")
                    
                    # Mostrar resumen rápido
                    col1, col2, col3, col4, col5 = st.columns(5)  # Agregué una columna más
                    with col1:
                        count_ot_prog = len(resultados_similares['ot_programadas'])
                        st.metric("OT Programadas", count_ot_prog, 
                                 delta="Coincidencias" if count_ot_prog > 0 else None)
                    with col2:
                        count_ot_pend = len(resultados_similares['ot_pendientes'])
                        st.metric("OT Pendientes", count_ot_pend,
                                 delta="Activas" if count_ot_pend > 0 else None)
                    with col3:
                        count_av_ing = len(resultados_similares['avisos_ingresados'])
                        st.metric("Avisos Ingresados", count_av_ing,
                                 delta="Por vincular" if count_av_ing > 0 else None)
                    with col4:
                        count_av_proc = len(resultados_similares['avisos_en_proceso'])
                        st.metric("Avisos en Proceso", count_av_proc,
                                 delta="En curso" if count_av_proc > 0 else None)
                    with col5:
                        # NUEVO: Métrica para avisos programados
                        count_av_prog = len(resultados_similares['avisos_programados'])
                        st.metric("Avisos Programados", count_av_prog,
                                 delta="Programados" if count_av_prog > 0 else None)
                    
                    # Mostrar resultados en pestañas - AGREGAR PESTAÑA PARA AVISOS PROGRAMADOS
                    tab_ot_prog, tab_ot_pend, tab_av_ing, tab_av_proc, tab_av_prog = st.tabs([
                        f"🔄 OT Programadas ({count_ot_prog})",
                        f"⏳ OT Pendientes ({count_ot_pend})", 
                        f"📋 Avisos Ingresados ({count_av_ing})",
                        f"🛠️ Avisos en Proceso ({count_av_proc})",
                        f"📅 Avisos Programados ({count_av_prog})"  # NUEVA PESTAÑA
                    ])
                    
                    with tab_ot_prog:
                        if not resultados_similares['ot_programadas'].empty:
                            st.write("**OT Programadas similares encontradas:**")
        
                            # Mostrar información relevante - VERIFICAR COLUMNAS DISPONIBLES
                            columnas_disponibles = resultados_similares['ot_programadas'].columns.tolist()
        
                            # Seleccionar solo las columnas que existen
                            columnas_a_mostrar = ['codigo_ot_base', 'equipo', 'codigo_equipo', 'descripcion_problema']
        
                            # Agregar columnas opcionales si existen
                            if 'fecha_estimada_inicio' in columnas_disponibles:
                                columnas_a_mostrar.append('fecha_estimada_inicio')
                            if 'estado' in columnas_disponibles:
                                columnas_a_mostrar.append('estado')
                            if 'area' in columnas_disponibles:
                                columnas_a_mostrar.append('area')
        
                            df_mostrar = resultados_similares['ot_programadas'][columnas_a_mostrar]
                            st.dataframe(df_mostrar, use_container_width=True)
        
                            st.info("""
                            💡 **Recomendación:** 
                            - Si el problema es similar, considere usar la OT existente
                            - Si es un problema diferente, puede crear una nueva OT
                            - Contacte al responsable de la OT existente para coordinar
                            """)
                        else:
                            st.success("✅ No hay OT programadas similares - Puede crear una nueva OT")
                    
                    with tab_ot_pend:
                        if not resultados_similares['ot_pendientes'].empty:
                            st.write("**OT Pendientes (SUB-OT) similares:**")
        
                            # Verificar columnas disponibles
                            columnas_disponibles = resultados_similares['ot_pendientes'].columns.tolist()
        
                            columnas_a_mostrar = ['codigo_ot_sufijo', 'codigo_ot_base', 'equipo', 'descripcion_problema']
        
                            # Agregar columnas opcionales si existen
                            if 'fecha_inicio' in columnas_disponibles:
                                columnas_a_mostrar.append('fecha_inicio')
                            if 'estado' in columnas_disponibles:
                                columnas_a_mostrar.append('estado')
                            if 'area' in columnas_disponibles:
                                columnas_a_mostrar.append('area')
        
                            df_mostrar = resultados_similares['ot_pendientes'][columnas_a_mostrar]
                            st.dataframe(df_mostrar, use_container_width=True)
        
                            st.info("💡 Ya existe una OT pendiente para este equipo. Puede agregar una nueva SUB-OT a la OT existente.")
                        else:
                            st.success("✅ No hay OT pendientes similares")
                    
                    with tab_av_ing:
                        if not resultados_similares['avisos_ingresados'].empty:
                            st.write("**Avisos Ingresados similares:**")
        
                            # Mostrar tabla de avisos
                            df_mostrar = resultados_similares['avisos_ingresados'][[
                                'codigo_padre', 'equipo', 'descripcion_problema', 
                                'ingresado_el', 'hay_riesgo'
                            ]]
                            st.dataframe(df_mostrar, use_container_width=True)
        
                            # Permitir seleccionar avisos para vincular
                            st.write("**¿Desea vincular alguno de estos avisos?**")

                            if len(resultados_similares['avisos_ingresados']) > 0:
                                # Seleccionar aviso principal
                                st.write("**Seleccione el aviso principal:**")
                                aviso_principal = st.selectbox(
                                    "Aviso principal (determinará el código de mantenimiento):",
                                    [codigo_tratar] + resultados_similares['avisos_ingresados']['codigo_padre'].tolist(),
                                    format_func=lambda x: f"{x} - {'(ACTUAL)' if x == codigo_tratar else ''}",
                                    key="selector_aviso_principal"
                                )

                                # Seleccionar avisos para vincular (excluyendo el principal si está en la lista)
                                avisos_disponibles_para_vincular = [
                                    aviso for aviso in resultados_similares['avisos_ingresados']['codigo_padre'].tolist() 
                                    if aviso != aviso_principal
                                ]

                                avisos_para_vincular = st.multiselect(
                                    "Seleccione avisos adicionales para vincular:",
                                    avisos_disponibles_para_vincular,
                                    format_func=lambda x: f"{x} - {resultados_similares['avisos_ingresados'][resultados_similares['avisos_ingresados']['codigo_padre'] == x]['equipo'].iloc[0]}",
                                    key="vincular_avisos_ingresados"
                                )

                                if avisos_para_vincular:
                                    col_btn1, col_btn2 = st.columns(2)
                                    with col_btn1:
                                        if st.button("🔗 Vincular Avisos Seleccionados", key="vincular_avisos", type="primary", use_container_width=True):
                                            success, mensaje = vincular_avisos(aviso_principal, avisos_para_vincular)
                                            if success:
                                                st.success(mensaje)
                                                st.rerun()
                                            else:
                                                st.error(mensaje)
                                    with col_btn2:
                                        if st.button("📋 Ver Detalles Completos", key="ver_detalles_avisos", use_container_width=True):
                                            st.write("**Resumen de vinculación:**")
                                            st.write(f"**Aviso principal:** {aviso_principal}")
                                            st.write(f"**Avisos a vincular:** {', '.join(avisos_para_vincular)}")
                                            st.write(f"**Total de avisos:** {len(avisos_para_vincular) + 1}")
                        else:
                            st.success("✅ No hay avisos ingresados similares")
                    
                    with tab_av_proc:
                        if not resultados_similares['avisos_en_proceso'].empty:
                            st.write("**Avisos en Proceso similares:**")
                            st.dataframe(resultados_similares['avisos_en_proceso'][[
                                'codigo_padre', 'equipo', 'descripcion_problema', 'codigo_ot_base'
                            ]], use_container_width=True)
                            st.info("ℹ️ Estos avisos ya están en proceso con otras OT")
                        else:
                            st.success("✅ No hay avisos en proceso similares")
                    
                    # NUEVA PESTAÑA: AVISOS PROGRAMADOS
                    with tab_av_prog:
                        if not resultados_similares['avisos_programados'].empty:
                            st.write("**Avisos Programados similares:**")
        
                            # Mostrar tabla de avisos programados - SOLO CON COLUMNAS EXISTENTES
                            # Primero verificar qué columnas están disponibles
                            columnas_disponibles = resultados_similares['avisos_programados'].columns.tolist()
        
                            # Seleccionar solo las columnas que existen
                            columnas_a_mostrar = ['codigo_padre', 'equipo', 'descripcion_problema', 'ingresado_el']
        
                            # Agregar columnas opcionales si existen
                            if 'hay_riesgo' in columnas_disponibles:
                                columnas_a_mostrar.append('hay_riesgo')
                            if 'codigo_ot_base' in columnas_disponibles:
                                columnas_a_mostrar.append('codigo_ot_base')
                            if 'estado' in columnas_disponibles:
                                columnas_a_mostrar.append('estado')
        
                            df_mostrar = resultados_similares['avisos_programados'][columnas_a_mostrar]
                            st.dataframe(df_mostrar, use_container_width=True)
                            
                            st.info("""
                            💡 **Información sobre Avisos Programados:**
                            - Estos avisos ya tienen una OT programada asociada
                            - Pueden estar vinculados a la misma OT base
                            - Considere coordinar con las OT existentes antes de crear una nueva
                            """)
        
                            # Permitir vincular avisos programados también
                            st.write("**¿Desea vincular alguno de estos avisos programados?**")
        
                            if len(resultados_similares['avisos_programados']) > 0:
                                avisos_prog_para_vincular = st.multiselect(
                                    "Seleccione avisos programados para vincular:",
                                    resultados_similares['avisos_programados']['codigo_padre'].tolist(),
                                    format_func=lambda x: f"{x} - {resultados_similares['avisos_programados'][resultados_similares['avisos_programados']['codigo_padre'] == x]['equipo'].iloc[0]}",
                                    key="vincular_avisos_programados"
                                )
            
                                if avisos_prog_para_vincular:
                                    col_btn3, col_btn4 = st.columns(2)
                                    with col_btn3:
                                        if st.button("🔗 Vincular Avisos Programados", key="vincular_avisos_prog", type="primary", use_container_width=True):
                                            success, mensaje = vincular_avisos(codigo_tratar, avisos_prog_para_vincular)
                                            if success:
                                                st.success(mensaje)
                                                st.rerun()
                                            else:
                                                st.error(mensaje)
                                    with col_btn4:
                                        if st.button("📋 Ver Detalles Avisos Prog", key="ver_detalles_avisos_prog", use_container_width=True):
                                            for aviso_codigo in avisos_prog_para_vincular:
                                                aviso_detalle = resultados_similares['avisos_programados'][
                                                    resultados_similares['avisos_programados']['codigo_padre'] == aviso_codigo
                                                ].iloc[0]
                                                st.write(f"**{aviso_codigo}:**")
                                                st.write(f"**Equipo:** {aviso_detalle['equipo']}")
                                                st.write(f"**Descripción:** {aviso_detalle['descripcion_problema']}")
                                                # Verificar si las columnas existen antes de mostrarlas
                                                if 'codigo_ot_base' in aviso_detalle and aviso_detalle['codigo_ot_base']:
                                                    st.write(f"**OT Base:** {aviso_detalle['codigo_ot_base']}")
                                                if 'hay_riesgo' in aviso_detalle:
                                                    st.write(f"**Riesgo:** {aviso_detalle['hay_riesgo']}")
                                                st.write("---")
                        else:
                            st.success("✅ No hay avisos programados similares")
                
                st.markdown("---")
                
                # ==================== FORMULARIOS EXISTENTES ====================
                # FORMULARIO 1: SOLO LECTURA (Datos de la pestaña 1)
                st.subheader("📄 Formulario 1: Datos del Reporte (Solo Lectura)")
                
                with st.form(f"form1_solo_lectura_{codigo_tratar}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**📋 Información Básica del Reporte**")
                        
                        # Campos de solo lectura con los datos de la pestaña 1
                        st.text_input(
                            "CÓDIGO PADRE *",
                            value=registro_actual['codigo_padre'],
                            disabled=True,
                            key=f"cod_padre_view_{codigo_tratar}"
                        )
                        
                        st.text_input(
                            "CÓDIGO MANTENIMIENTO *",
                            value=registro_actual['codigo_mantto'],
                            disabled=True,
                            key=f"cod_mantto_view_{codigo_tratar}"
                        )
                        
                        st.text_input(
                            "ÁREA *",
                            value=registro_actual['area'],
                            disabled=True,
                            key=f"area_view_{codigo_tratar}"
                        )
                        
                        st.text_input(
                            "EQUIPO *",
                            value=registro_actual['equipo'],
                            disabled=True,
                            key=f"equipo_view_{codigo_tratar}"
                        )
                        
                        st.text_input(
                            "¿HAY RIESGO? *",
                            value=registro_actual['hay_riesgo'],
                            disabled=True,
                            key=f"riesgo_view_{codigo_tratar}"
                        )
                    
                    with col2:
                        st.write("**👤 Información de Ingreso**")
                        
                        st.text_input(
                            "INGRESADO POR *",
                            value=registro_actual['ingresado_por'],
                            disabled=True,
                            key=f"ingresado_por_view_{codigo_tratar}"
                        )
                        
                        st.text_input(
                            "FECHA INGRESO *",
                            value=registro_actual['ingresado_el'],
                            disabled=True,
                            key=f"fecha_ingreso_view_{codigo_tratar}"
                        )
                        
                        st.text_input(
                            "ANTIGÜEDAD",
                            value=f"{registro_actual['antiguedad']} días",
                            disabled=True,
                            key=f"antiguedad_view_{codigo_tratar}"
                        )
                    
                    # Descripción del problema
                    st.write("**📝 Descripción del Problema**")
                    st.text_area(
                        "DESCRIPCIÓN DEL PROBLEMA *",
                        value=registro_actual['descripcion_problema'],
                        height=100,
                        disabled=True,
                        key=f"desc_problema_view_{codigo_tratar}"
                    )
                    
                    # Imagen adjunta
                    st.write("**🖼️ Evidencia Fotográfica**")
                    resultado = conn_avisos.execute(
                        'SELECT imagen_aviso_nombre, imagen_aviso_datos FROM avisos WHERE codigo_padre = ?', 
                        (codigo_tratar,)
                    ).fetchone()
                    
                    if resultado and resultado[1]:
                        imagen_nombre, imagen_datos = resultado
                        st.image(imagen_datos, 
                                caption=f"Imagen: {imagen_nombre}", 
                                use_column_width=True)
                    else:
                        st.info("📷 No hay imagen adjunta")
                    
                    st.form_submit_button("Formulario de Solo Lectura", disabled=True)
                
                # FORMULARIO 2: EDITABLE (Crear OT Programada)
                with st.form(f"form2_editable_{codigo_tratar}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**📋 Información Básica**")
                        
                        # Obtener valores existentes o usar defaults
                        clasificacion_actual = registro_actual.get('clasificacion', 'EQUIPO')
                        sistema_actual = registro_actual.get('sistema', 'SISTEMA ELÉCTRICO')
                        codigo_equipo_actual = registro_actual.get('codigo_equipo', '')
                        prioridad_actual = registro_actual.get('prioridad_nueva', '2. MEDIO')
                        responsable_actual = registro_actual.get('responsable', 'JHONAR VASQUEZ')
                        alimentador_actual = registro_actual.get('alimentador_proveedor', 'TÉCNICO')
                        
                        clasificacion = st.selectbox(
                            "CLASIFICACIÓN *",
                            ["EQUIPO", "INFRAESTRUCTURA"],
                            index=0 if clasificacion_actual == "EQUIPO" else 1,
                            key=f"clasif_{codigo_tratar}"
                        )
                        
                        sistema = st.selectbox(
                            "SISTEMA *",
                            ["SISTEMA ELÉCTRICO", "SISTEMA MECÁNICO", "SISTEMA HIDRÁULICO", "SISTEMA NEUMÁTICO", 
                             "SISTEMA ELECTRÓNICO", "SISTEMA ESTRUCTURAL", "OTRO"],
                            index=["SISTEMA ELÉCTRICO", "SISTEMA MECÁNICO", "SISTEMA HIDRÁULICO", "SISTEMA NEUMÁTICO", 
                                  "SISTEMA ELECTRÓNICO", "SISTEMA ESTRUCTURAL", "OTRO"].index(sistema_actual) 
                                  if sistema_actual in ["SISTEMA ELÉCTRICO", "SISTEMA MECÁNICO", "SISTEMA HIDRÁULICO", "SISTEMA NEUMÁTICO", 
                                                       "SISTEMA ELECTRÓNICO", "SISTEMA ESTRUCTURAL", "OTRO"] else 0,
                            key=f"sistema_{codigo_tratar}"
                        )
                        
                        codigo_equipo = st.text_input(
                            "CÓDIGO DE EQUIPO *",
                            value=codigo_equipo_actual,
                            placeholder="Ingrese código del equipo",
                            key=f"cod_eq_{codigo_tratar}"
                        )
                        
                        prioridad = st.selectbox(
                            "PRIORIDAD *",
                            ["1. ALTO", "2. MEDIO", "3. BAJO"],
                            index=["1. ALTO", "2. MEDIO", "3. BAJO"].index(prioridad_actual) 
                                  if prioridad_actual in ["1. ALTO", "2. MEDIO", "3. BAJO"] else 1,
                            key=f"prior_{codigo_tratar}"
                        )
                        
                        responsable = st.selectbox(
                            "RESPONSABLE *",
                            ["JHONAR VASQUEZ", 'DIEGO QUISPE'],
                            index=0 if responsable_actual == "JHONAR VASQUEZ" else 1,
                            key=f"resp_{codigo_tratar}"
                        )
                        
                        alimentador_proveedor = st.selectbox(
                            "ALIMENCORP O PROVEEDOR *",
                            ["TÉCNICO", "TERCERO"],
                            index=0 if alimentador_actual == "TÉCNICO" else 1,
                            key=f"alim_{codigo_tratar}"
                        )
                    
                    with col2:
                        st.write("**👥 Recursos Humanos**")
                        
                        cantidad_mecanicos = st.number_input(
                            "CANTIDAD DE MECÁNICOS",
                            min_value=0,
                            max_value=10,
                            value=int(registro_actual.get('cantidad_mecanicos', 0)),
                            key=f"mec_{codigo_tratar}"
                        )
                        
                        cantidad_electricos = st.number_input(
                            "CANTIDAD DE ELÉCTRICOS",
                            min_value=0,
                            max_value=10,
                            value=int(registro_actual.get('cantidad_electricos', 0)),
                            key=f"elec_{codigo_tratar}"
                        )
                        
                        cantidad_soldadores = st.number_input(
                            "CANTIDAD DE SOLDADORES",
                            min_value=0,
                            max_value=10,
                            value=int(registro_actual.get('cantidad_soldadores', 0)),
                            key=f"sold_{codigo_tratar}"
                        )
                        
                        cantidad_op_vahos = st.number_input(
                            "CANTIDAD DE OP. VAHOS",
                            min_value=0,
                            max_value=10,
                            value=int(registro_actual.get('cantidad_op_vahos', 0)),
                            key=f"vahos_{codigo_tratar}"
                        )
                        
                        cantidad_calderistas = st.number_input(
                            "CANTIDAD DE CALDERISTAS",
                            min_value=0,
                            max_value=10,
                            value=int(registro_actual.get('cantidad_calderistas', 0)),
                            key=f"cald_{codigo_tratar}"
                        )
                    
                    st.write("**📅 Programación**")
                    col3, col4 = st.columns(2)
                    
                    with col3:
                        # Manejar fecha existente
                        fecha_existente = registro_actual.get('fecha_estimada_inicio')
                        if fecha_existente:
                            try:
                                fecha_default = pd.to_datetime(fecha_existente).date()
                            except:
                                fecha_default = date.today()
                        else:
                            fecha_default = date.today()
                            
                        fecha_estimada_inicio = st.date_input(
                            "FECHA ESTIMADA DE INICIO *",
                            value=fecha_default,
                            key=f"fecha_ini_{codigo_tratar}"
                        )
                    
                    with col4:
                        duracion_estimada = st.text_input(
                            "DURACIÓN ESTIMADA *",
                            value=registro_actual.get('duracion_estimada', ''),
                            placeholder="Ej: 3 días, 2 semanas, 8 horas",
                            key=f"duracion_{codigo_tratar}"
                        )
                    
                    st.write("**📝 Descripción del Trabajo**")
                    descripcion_trabajo = st.text_area(
                        "DESCRIPCIÓN DEL TRABAJO A REALIZAR *",
                        value=registro_actual.get('descripcion_trabajo', ''),
                        placeholder="Describa detalladamente el trabajo a realizar...",
                        height=120,
                        key=f"desc_trab_{codigo_tratar}"
                    )
                    
                    st.write("**📦 Materiales**")
                    materiales = st.text_area(
                        "MATERIALES (separados por coma) *",
                        value=registro_actual.get('materiales', ''),
                        placeholder="Ej: Tornillos, Tuercas, Cable eléctrico, Pintura...",
                        height=80,
                        key=f"mat_{codigo_tratar}"
                    )
                    
                    st.write("**🔢 Código de OT Programada**")
                    codigo_ot_base = generar_numero_ot()
                    st.success(f"**Código OT:** {codigo_ot_base}")
                    st.info("💡 **Nota:** El Código SUB OT se generará automáticamente cuando se cree la OT Pendiente")
                    
                    # Botones de acción en el formulario editable
                    st.markdown("---")
                    col5, col6, col7 = st.columns([2, 1, 1])
                    
                    with col5:
                        st.info("💡 Complete todos los campos obligatorios (*) para crear la OT Programada")
                    
                    with col6:
                        crear_ot_programada = st.form_submit_button("🚀 Crear OT Programada", 
                                                                  type="primary", 
                                                                  use_container_width=True)
                    
                    with col7:
                        rechazar_aviso = st.form_submit_button("❌ Rechazar Aviso", 
                                                              use_container_width=True)
                    
                    # DENTRO DEL FORMULARIO - CREAR OT PROGRAMADA
                    if crear_ot_programada:
                        if (clasificacion and sistema and codigo_equipo and prioridad and 
                            responsable and alimentador_proveedor and descripcion_trabajo and 
                            materiales and duracion_estimada):
                            
                            try:
                                # USAR DESCRIPCIÓN DEL TRABAJO COMO COMPONENTES POR DEFECTO
                                componentes_por_defecto = descripcion_trabajo
                                
                                # 1. Crear OT en la base de datos OT con estado PROGRAMADO
                                ejecutar_consulta_segura(conn_ot_unicas, '''
                                    INSERT INTO ot_unicas (
                                        codigo_ot_base, codigo_padre, estado, tipo_mantenimiento,
                                        componentes, responsables_comienzo, descripcion_trabajo_realizado,
                                        fecha_inicio_mantenimiento, duracion_estimada,
                                        ingresado_por, ingresado_el, clasificacion, sistema,
                                        codigo_equipo, prioridad_nueva, responsable, 
                                        alimentador_proveedor, materiales, cantidad_mecanicos,
                                        cantidad_electricos, cantidad_soldadores, cantidad_op_vahos,
                                        cantidad_calderistas
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    codigo_ot_base, codigo_tratar, 'PROGRAMADO', 'CORRECTIVO',
                                    componentes_por_defecto, responsable, descripcion_trabajo,
                                    fecha_estimada_inicio, duracion_estimada,
                                    st.session_state.nombre_completo, date.today(),
                                    clasificacion, sistema, codigo_equipo, prioridad,
                                    responsable, alimentador_proveedor, materiales,
                                    cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                                    cantidad_op_vahos, cantidad_calderistas
                                ))
                                
                                # 2. Actualizar el aviso original a estado "PROGRAMADO" y agregar el código OT
                                ejecutar_consulta_segura(conn_avisos, '''
                                    UPDATE avisos SET 
                                        clasificacion=?, descripcion_trabajo=?, sistema=?, codigo_equipo=?,
                                        prioridad_nueva=?, responsable=?, alimentador_proveedor=?, materiales=?,
                                        cantidad_mecanicos=?, cantidad_electricos=?, cantidad_soldadores=?,
                                        cantidad_op_vahos=?, cantidad_calderistas=?, fecha_estimada_inicio=?,
                                        duracion_estimada=?, estado='PROGRAMADO', codigo_ot_base=?
                                    WHERE codigo_padre=?
                                ''', (
                                    clasificacion, descripcion_trabajo, sistema, codigo_equipo,
                                    prioridad, responsable, alimentador_proveedor, materiales,
                                    cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                                    cantidad_op_vahos, cantidad_calderistas, fecha_estimada_inicio,
                                    duracion_estimada, codigo_ot_base, codigo_tratar
                                ))
                                
                                st.success(f"✅ **OT Programada creada correctamente**")
                                st.success(f"📋 **Código OT:** {codigo_ot_base}")
                                log_accion(st.session_state.usuario, "OT_PROGRAMADA_CREADA", f"OT: {codigo_ot_base}, Aviso: {codigo_tratar}")
                                st.balloons()
                                
                                # Forzar recarga de la página
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error al crear OT programada: {e}")
                                log_accion(st.session_state.usuario, "ERROR_OT_PROGRAMADA", f"Error: {str(e)}")
                        else:
                            st.error("❌ **Complete todos los campos obligatorios marcados con ***")
                    
                    if rechazar_aviso:
                        ejecutar_consulta_segura(
                            conn_avisos,
                            'UPDATE avisos SET estado="RECHAZADO" WHERE codigo_padre=?',
                            (codigo_tratar,)
                        )
                        st.success("✅ **Aviso rechazado correctamente**")
                        log_accion(st.session_state.usuario, "AVISO_RECHAZADO", f"Aviso: {codigo_tratar}")
                        
                        # Forzar recarga de la página
                        st.rerun()
                        
            else:
                st.error("❌ No se pudo encontrar el aviso seleccionado en la base de datos")
        
    else:
        st.info("🎉 No hay avisos pendientes de tratamiento")

#============================PESTAÑA 4 CREAR OT DIRECTA =======================
with tab4:
    st.header("🚀 Crear Orden de Trabajo Directa")
    st.info("💡 **Crear OT sin necesidad de pasar por el proceso de avisos**")
    
    # Verificar estructura de la tabla solo una vez usando session_state
    if 'tabla_estandarizada' not in st.session_state:
        try:
            actualizar_estructura_todas_tablas()
            st.session_state.tabla_estandarizada = True
            st.success("✅ Estructura de tablas estandarizada")
        except Exception as e:
            st.error(f"❌ Error al estandarizar estructura: {e}")
    
    # Inicializar session_state para controlar el envío
    if 'ot_directa_enviada' not in st.session_state:
        st.session_state.ot_directa_enviada = False
    
    # Si se envió recientemente, mostrar mensaje y resetear
    if st.session_state.ot_directa_enviada:
        st.success("✅ **Orden de Trabajo creada correctamente**")
        st.session_state.ot_directa_enviada = False
        if st.button("🔄 Crear otra OT"):
            st.rerun()
    
    else:
        # CAMPOS FUERA DEL FORMULARIO PARA ACTUALIZACIÓN EN TIEMPO REAL
        st.subheader("🏭 Área y Equipo")
        col_area1, col_area2 = st.columns(2)
        
        with col_area1:
            area_seleccionada = st.selectbox(
                "Área *",
                list(EQUIPOS_POR_AREA.keys()),
                key="area_ot_directa"
            )
        
        with col_area2:
            # Obtener equipos disponibles para el área seleccionada
            equipos_disponibles = EQUIPOS_POR_AREA.get(area_seleccionada, [])
            equipo_seleccionado = st.selectbox(
                "Equipo *",
                equipos_disponibles,
                key="equipo_ot_directa"
            )
        
        st.markdown("---")
        
        st.subheader("🔧 Tipo de Mantenimiento")
        col_mantenimiento1, col_mantenimiento2 = st.columns(2)
        
        with col_mantenimiento1:
            tipo_mantenimiento = st.selectbox(
                "TIPO DE MANTENIMIENTO *",
                ["CORRECTIVO", "PREVENTIVO", "PREDICTIVO", "MANTENIBILIDAD", "OTRO"],
                key="tipo_mant_ot_directa"
            )
        
        with col_mantenimiento2:
            tipo_preventivo = st.selectbox(
                "TIPO DE PREVENTIVO",
                ["NO APLICA", "PROGRAMADO", "CONDICIONAL", "PREDICTIVO", "TAREAS MENORES"],
                disabled=(tipo_mantenimiento != "PREVENTIVO"),
                help="Solo aplica si el tipo de mantenimiento es PREVENTIVO",
                key="tipo_preventivo_ot_directa"
            )
        
        st.markdown("---")
        
        # FORMULARIO PRINCIPAL - Solo los campos que no necesitan actualización en tiempo real
        with st.form("crear_ot_directa_form", clear_on_submit=True):
            st.subheader("🔧 Información Básica")
            
            col_codigos1, col_codigos2, col_codigos3 = st.columns(3)
            with col_codigos1:
                st.info("**Código Padre:** Se generará al enviar")
            with col_codigos2:
                st.info("**Código Mantto:** Se generará al enviar")
            with col_codigos3:
                st.info("**Código OT Base:** Se generará al enviar")
            
            st.markdown("---")
            
            # Información básica de la OT
            st.subheader("📋 Información de la OT")
            col_info1, col_info2 = st.columns(2)
            
            with col_info1:
                clasificacion = st.selectbox(
                    "CLASIFICACIÓN *",
                    ["EQUIPO", "INFRAESTRUCTURA"],
                    key="clasif_ot_directa"
                )
                
                sistema = st.selectbox(
                    "SISTEMA *",
                    ["SISTEMA ELÉCTRICO", "SISTEMA MECÁNICO", "SISTEMA HIDRÁULICO", "SISTEMA NEUMÁTICO", 
                     "SISTEMA ELECTRÓNICO", "SISTEMA ESTRUCTURAL", "OTRO"],
                    key="sistema_ot_directa"
                )
                
                codigo_equipo = st.text_input(
                    "CÓDIGO DE EQUIPO *",
                    value=equipo_seleccionado,  # Se auto-completa con el equipo seleccionado
                    placeholder="Ingrese código del equipo",
                    key="cod_eq_ot_directa"
                )
                
                prioridad = st.selectbox(
                    "PRIORIDAD *",
                    ["1. ALTO", "2. MEDIO", "3. BAJO"],
                    key="prior_ot_directa"
                )
                
                responsable = st.selectbox(
                    "RESPONSABLE *",
                    ["JHONAR VASQUEZ", 'DIEGO QUISPE'],
                    key="resp_ot_directa"
                )
            
            with col_info2:
                alimentador_proveedor = st.selectbox(
                    "ALIMENCORP O PROVEEDOR *",
                    ["TÉCNICO", "TERCERO"],
                    key="alim_ot_directa"
                )
                
                componentes = st.text_area(
                    "COMPONENTES *",
                    placeholder="Lista de componentes involucrados...",
                    height=80,
                    key="componentes_ot_directa"
                )
                
                descripcion_problema = st.text_area(
                    "DESCRIPCIÓN DEL PROBLEMA *",
                    placeholder="Describa el problema o necesidad...",
                    height=80,
                    key="desc_problema_ot_directa"
                )
                
                hay_riesgo = st.selectbox(
                    "¿HAY RIESGO? *",
                    ["NO", "SI"],
                    key="riesgo_ot_directa"
                )
            
            st.markdown("---")
            
            # Recursos humanos
            st.subheader("👥 Recursos Humanos")
            col_rrhh1, col_rrhh2, col_rrhh3, col_rrhh4, col_rrhh5 = st.columns(5)
            
            with col_rrhh1:
                cantidad_mecanicos = st.number_input(
                    "MECÁNICOS",
                    min_value=0,
                    max_value=10,
                    value=0,
                    key="mec_ot_directa"
                )
            
            with col_rrhh2:
                cantidad_electricos = st.number_input(
                    "ELÉCTRICOS",
                    min_value=0,
                    max_value=10,
                    value=0,
                    key="elec_ot_directa"
                )
            
            with col_rrhh3:
                cantidad_soldadores = st.number_input(
                    "SOLDADORES",
                    min_value=0,
                    max_value=10,
                    value=0,
                    key="sold_ot_directa"
                )
            
            with col_rrhh4:
                cantidad_op_vahos = st.number_input(
                    "OP. VAHOS",
                    min_value=0,
                    max_value=10,
                    value=0,
                    key="vahos_ot_directa"
                )
            
            with col_rrhh5:
                cantidad_calderistas = st.number_input(
                    "CALDERISTAS",
                    min_value=0,
                    max_value=10,
                    value=0,
                    key="cald_ot_directa"
                )
            
            st.markdown("---")
            
            # Programación
            st.subheader("📅 Programación")
            col_prog1, col_prog2 = st.columns(2)
            
            with col_prog1:
                fecha_estimada_inicio = st.date_input(
                    "FECHA ESTIMADA DE INICIO *",
                    value=date.today(),
                    key="fecha_ini_ot_directa"
                )
            
            with col_prog2:
                duracion_estimada = st.text_input(
                    "DURACIÓN ESTIMADA *",
                    placeholder="Ej: 3 días, 2 semanas, 8 horas",
                    key="duracion_ot_directa"
                )
            
            st.markdown("---")
            
            # Descripción del trabajo y materiales
            st.subheader("📝 Descripción del Trabajo")
            descripcion_trabajo = st.text_area(
                "DESCRIPCIÓN DEL TRABAJO A REALIZAR *",
                placeholder="Describa detalladamente el trabajo a realizar...",
                height=120,
                key="desc_trab_ot_directa"
            )
            
            st.subheader("📦 Materiales")
            materiales = st.text_area(
                "MATERIALES (separados por coma) *",
                placeholder="Ej: Tornillos, Tuercas, Cable eléctrico, Pintura...",
                height=80,
                key="mat_ot_directa"
            )
            
            st.markdown("---")
            
            # Información del usuario que registra
            st.info(f"**👤 Registrado por:** {st.session_state.nombre_completo}")
            
            # Mostrar resumen de selecciones fuera del formulario
            st.info("**📋 Resumen de selección:**")
            col_res1, col_res2, col_res3 = st.columns(3)
            with col_res1:
                st.write(f"**Área:** {area_seleccionada}")
                st.write(f"**Equipo:** {equipo_seleccionado}")
            with col_res2:
                st.write(f"**Tipo Mantto:** {tipo_mantenimiento}")
                if tipo_mantenimiento == "PREVENTIVO":
                    st.write(f"**Tipo Preventivo:** {tipo_preventivo}")
            with col_res3:
                st.write(f"**Código Equipo:** {codigo_equipo}")
            
            # Botón de envío - DENTRO DEL FORM
            submitted = st.form_submit_button(
                "🚀 Crear Orden de Trabajo Directa", 
                type="primary",
                use_container_width=True
            )
            
            # LA LÓGICA DE ENVÍO DEBE ESTAR DENTRO DEL IF submitted
            if submitted:
                # Generar códigos solo cuando se envía el formulario
                codigo_padre = generar_codigo_padre_ot_directa()
                codigo_mantto = generar_codigo_mantto()
                codigo_ot_base = generar_numero_ot()
                
                if (area_seleccionada and equipo_seleccionado and tipo_mantenimiento and 
                    clasificacion and sistema and codigo_equipo and prioridad and 
                    responsable and alimentador_proveedor and componentes and 
                    descripcion_problema and hay_riesgo and descripcion_trabajo and 
                    materiales and duracion_estimada):
                    
                    try:
                        # Calcular antigüedad
                        antiguedad = 0
                        
                        # 1. Crear registro en ot_unicas con estructura estandarizada
                        ejecutar_consulta_segura(conn_ot_unicas, '''
                            INSERT INTO ot_unicas (
                                codigo_ot_base, codigo_padre, codigo_mantto, estado, tipo_mantenimiento, tipo_preventivo,
                                area, equipo, codigo_equipo, componentes, descripcion_problema,
                                ingresado_por, ingresado_el, antiguedad, hay_riesgo,
                                clasificacion, sistema, prioridad_nueva, responsable, 
                                alimentador_proveedor, descripcion_trabajo, materiales,
                                cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                                cantidad_op_vahos, cantidad_calderistas, fecha_estimada_inicio,
                                duracion_estimada, ot_base_creado_en
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            codigo_ot_base, codigo_padre, codigo_mantto, 'PROGRAMADO', tipo_mantenimiento,
                            tipo_preventivo if tipo_mantenimiento == "PREVENTIVO" else "NO APLICA",
                            area_seleccionada, equipo_seleccionado, codigo_equipo, componentes, descripcion_problema,
                            st.session_state.nombre_completo, date.today(), antiguedad, hay_riesgo,
                            clasificacion, sistema, prioridad, responsable, 
                            alimentador_proveedor, descripcion_trabajo, materiales,
                            cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                            cantidad_op_vahos, cantidad_calderistas, fecha_estimada_inicio,
                            duracion_estimada, datetime.now()
                        ))
                        
                        # 2. Crear registro en avisos (para mantener consistencia en el sistema)
                        ejecutar_consulta_segura(conn_avisos, '''
                            INSERT INTO avisos (
                                codigo_padre, codigo_mantto, codigo_ot_base, estado, area, equipo, codigo_equipo,
                                componentes, descripcion_problema, ingresado_por, ingresado_el, 
                                antiguedad, hay_riesgo, tipo_mantenimiento, tipo_preventivo,
                                clasificacion, sistema, prioridad_nueva, responsable, 
                                alimentador_proveedor, descripcion_trabajo, materiales,
                                cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                                cantidad_op_vahos, cantidad_calderistas, fecha_estimada_inicio,
                                duracion_estimada, ot_base_creado_en
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            codigo_padre, codigo_mantto, codigo_ot_base, 'PROGRAMADO', area_seleccionada, equipo_seleccionado, codigo_equipo,
                            componentes, descripcion_problema, st.session_state.nombre_completo, date.today(),
                            antiguedad, hay_riesgo, tipo_mantenimiento,
                            tipo_preventivo if tipo_mantenimiento == "PREVENTIVO" else "NO APLICA",
                            clasificacion, sistema, prioridad, responsable, 
                            alimentador_proveedor, descripcion_trabajo, materiales,
                            cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                            cantidad_op_vahos, cantidad_calderistas, fecha_estimada_inicio,
                            duracion_estimada, datetime.now()
                        ))
                        
                        # Guardar información para mostrar en el mensaje de éxito
                        st.session_state.ot_directa_enviada = True
                        st.session_state.ultima_ot_creada = {
                            'codigo_ot_base': codigo_ot_base,
                            'codigo_padre': codigo_padre,
                            'codigo_mantto': codigo_mantto,
                            'area': area_seleccionada,
                            'equipo': equipo_seleccionado,
                            'tipo_mantenimiento': tipo_mantenimiento,
                            'tipo_preventivo': tipo_preventivo if tipo_mantenimiento == "PREVENTIVO" else "NO APLICA",
                            'codigo_equipo': codigo_equipo
                        }
                        
                        log_accion(st.session_state.usuario, "OT_DIRECTA_CREADA", 
                                  f"OT: {codigo_ot_base}, Padre: {codigo_padre}, Mantto: {codigo_mantto}, Área: {area_seleccionada}, Equipo: {equipo_seleccionado}")
                        
                        # Forzar recarga de la página
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Error al crear OT directa: {e}")
                        log_accion(st.session_state.usuario, "ERROR_OT_DIRECTA", f"Error: {str(e)}")
                else:
                    st.error("❌ **Complete todos los campos obligatorios marcados con ***")

# ==================== PESTAÑA 5 REGISTRO TOTAL ====================
with tab5:
    st.header("📊 Registro TOTAL - Todas las Órdenes de Trabajo y Avisos")
    
    # Obtener todos los datos de ambas bases de datos
    df_todos_avisos = obtener_todos_avisos()
    df_todas_ot_base = obtener_todas_ot_base()
    df_todas_ot_sufijos = obtener_todas_ot_sufijos()
    
    # Estadísticas generales - MODIFICADO
    st.subheader("📈 Estadísticas Generales")
    
    # Calcular estadísticas de AVISOS
    avisos_ingresados = len(df_todos_avisos[df_todos_avisos['estado'] == 'INGRESADO']) if not df_todos_avisos.empty else 0
    avisos_programados = len(df_todos_avisos[df_todos_avisos['estado'] == 'PROGRAMADO']) if not df_todos_avisos.empty else 0
    avisos_en_proceso = len(df_todos_avisos[df_todos_avisos['estado'] == 'EN PROCESO']) if not df_todos_avisos.empty else 0
    avisos_culminados = len(df_todos_avisos[df_todos_avisos['estado'] == 'CULMINADO']) if not df_todos_avisos.empty else 0
    avisos_rechazados = len(df_todos_avisos[df_todos_avisos['estado'] == 'RECHAZADO']) if not df_todos_avisos.empty else 0
    
    # Calcular estadísticas de OT BASE - MODIFICADO
    ot_base_programadas = len(df_todas_ot_base[df_todas_ot_base['estado'] == 'PROGRAMADO']) if not df_todas_ot_base.empty else 0
    ot_base_pendientes = len(df_todas_ot_base[df_todas_ot_base['estado'] == 'PENDIENTE']) if not df_todas_ot_base.empty else 0
    ot_base_culminadas = len(df_todas_ot_base[df_todas_ot_base['estado'] == 'CULMINADO']) if not df_todas_ot_base.empty else 0
    
    # Calcular estadísticas de SUB-OT
    total_sub_ot = len(df_todas_ot_sufijos) if not df_todas_ot_sufijos.empty else 0
    
    # Mostrar métricas en columnas
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_avisos = len(df_todos_avisos)
        st.metric("📋 Total Avisos", total_avisos)
    
    with col2:
        total_ot_base = len(df_todas_ot_base)
        st.metric("🔢 OT Base", total_ot_base)
    
    with col3:
        st.metric("📦 Total SUB-OT", total_sub_ot)
    
    with col4:
        st.metric("📥 Avisos Ingresados", avisos_ingresados)
    
    with col5:
        st.metric("🛠️ Avisos en Proceso", avisos_en_proceso)
    
    col6, col7, col8, col9, col10 = st.columns(5)
    
    with col6:
        st.metric("📅 OT Programadas", ot_base_programadas)
    
    with col7:
        st.metric("⏳ OT Pendientes", ot_base_pendientes)  # MODIFICADO: OT en lugar de SUB-OT
    
    with col8:
        st.metric("✅ OT Culminadas", ot_base_culminadas)  # MODIFICADO: OT en lugar de SUB-OT
    
    with col9:
        st.metric("📋 Avisos Programados", avisos_programados)
    
    with col10:
        st.metric("❌ Avisos Rechazados", avisos_rechazados)
    
    st.markdown("---")
    
    # NUEVA SECCIÓN: TABLA DE TODAS LAS SUB-OT
    st.subheader("📋 Tabla de Todas las SUB-OT")
    
    if not df_todas_ot_sufijos.empty:
        st.info(f"📊 Mostrando {len(df_todas_ot_sufijos)} SUB-OT registradas")
        
        # Seleccionar columnas para mostrar
        columnas_sub_ot = [
            'codigo_ot_sufijo', 'codigo_ot_base', 'estado', 'tipo_mantenimiento', 
            'tipo_preventivo', 'componentes', 'responsables_comienzo', 
            'descripcion_trabajo_realizado', 'paro_linea', 'fecha_inicio_mantenimiento',
            'hora_inicio_mantenimiento', 'hora_finalizacion_mantenimiento',
            'responsables_finalizacion', 'fecha_finalizacion', 'comentario',
            'ingresado_por', 'ingresado_el'
        ]
        
        # Filtrar solo las columnas que existen
        columnas_mostrar_sub_ot = [col for col in columnas_sub_ot if col in df_todas_ot_sufijos.columns]
        
        # Mostrar la tabla
        st.dataframe(df_todas_ot_sufijos[columnas_mostrar_sub_ot], use_container_width=True)
        
        # Estadísticas de SUB-OT
        st.subheader("📈 Estadísticas de SUB-OT")
        
        col_sub1, col_sub2, col_sub3 = st.columns(3)
        
        with col_sub1:
            if 'estado' in df_todas_ot_sufijos.columns:
                sub_ot_pendientes = len(df_todas_ot_sufijos[df_todas_ot_sufijos['estado'] == 'PENDIENTE'])
                st.metric("⏳ SUB-OT Pendientes", sub_ot_pendientes)
        
        with col_sub2:
            if 'estado' in df_todas_ot_sufijos.columns:
                sub_ot_culminadas = len(df_todas_ot_sufijos[df_todas_ot_sufijos['estado'] == 'CULMINADO'])
                st.metric("✅ SUB-OT Culminadas", sub_ot_culminadas)
        
        with col_sub3:
            if 'tipo_mantenimiento' in df_todas_ot_sufijos.columns:
                tipos_sub_ot = df_todas_ot_sufijos['tipo_mantenimiento'].value_counts()
                st.metric("🔧 Tipos de SUB-OT", len(tipos_sub_ot))
        
        # Exportar SUB-OT a Excel
        st.subheader("📤 Exportar SUB-OT a Excel")
        
        col_exp1, col_exp2 = st.columns([1, 1])
        
        with col_exp1:
            if st.button("💾 Exportar SUB-OT a Excel", key="export_sub_ot"):
                try:
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_todas_ot_sufijos.to_excel(writer, sheet_name='Todas_SUB_OT', index=False)
                    
                    output.seek(0)
                    
                    st.download_button(
                        label="⬇️ Descargar SUB-OT en Excel",
                        data=output,
                        file_name=f"todas_sub_ot_{date.today()}.xlsx",
                        mime="application/vnd.ms-excel",
                        key="download_sub_ot"
                    )
                except Exception as e:
                    st.error(f"Error al exportar SUB-OT: {e}")
        
        with col_exp2:
            if st.button("💾 Exportar Filtrado a Excel", key="export_sub_ot_filtrado"):
                try:
                    # Crear un DataFrame con las columnas seleccionadas
                    df_export = df_todas_ot_sufijos[columnas_mostrar_sub_ot]
                    
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_export.to_excel(writer, sheet_name='SUB_OT_Filtrado', index=False)
                    
                    output.seek(0)
                    
                    st.download_button(
                        label="⬇️ Descargar SUB-OT Filtrado",
                        data=output,
                        file_name=f"sub_ot_filtrado_{date.today()}.xlsx",
                        mime="application/vnd.ms-excel",
                        key="download_sub_ot_filtrado"
                    )
                except Exception as e:
                    st.error(f"Error al exportar SUB-OT filtrado: {e}")
        
        # Gráficos de SUB-OT
        st.subheader("📊 Gráficos de SUB-OT")
        
        col_graf_sub1, col_graf_sub2 = st.columns(2)
        
        with col_graf_sub1:
            if 'estado' in df_todas_ot_sufijos.columns:
                st.write("**Distribución por Estado - SUB-OT**")
                estado_sub_counts = df_todas_ot_sufijos['estado'].value_counts()
                st.bar_chart(estado_sub_counts)
        
        with col_graf_sub2:
            if 'tipo_mantenimiento' in df_todas_ot_sufijos.columns:
                st.write("**Distribución por Tipo - SUB-OT**")
                tipo_sub_counts = df_todas_ot_sufijos['tipo_mantenimiento'].value_counts()
                st.bar_chart(tipo_sub_counts)
    
    else:
        st.info("📭 No hay SUB-OT registradas")
    
    st.markdown("---")
    
    # SECCIÓN EXISTENTE: TODAS LAS ÓRDENES DE TRABAJO BASE
    st.subheader("🏭 TODAS las Órdenes de Trabajo Base")
    
    if not df_todas_ot_base.empty:
        # Filtros para OT
        col_filtro1, col_filtro2, col_filtro3 = st.columns(3)
        
        with col_filtro1:
            filtrar_estado_ot = st.selectbox(
                "Filtrar por estado:",
                ["TODOS", "PROGRAMADO", "PENDIENTE", "CULMINADO"],
                key="filtro_estado_ot_base"
            )
        
        with col_filtro2:
            # Obtener sistemas únicos para el filtro
            sistemas = ["TODAS"]
            if 'sistema' in df_todas_ot_base.columns:
                sistemas.extend(sorted(df_todas_ot_base['sistema'].dropna().unique().tolist()))
            
            filtrar_sistema_ot = st.selectbox(
                "Filtrar por sistema:",
                sistemas,
                key="filtro_sistema_ot_base"
            )
        
        with col_filtro3:
            # Obtener prioridades únicas para el filtro
            prioridades = ["TODAS"]
            if 'prioridad_nueva' in df_todas_ot_base.columns:
                prioridades.extend(sorted(df_todas_ot_base['prioridad_nueva'].dropna().unique().tolist()))
            
            filtrar_prioridad_ot = st.selectbox(
                "Filtrar por prioridad:",
                prioridades,
                key="filtro_prioridad_ot_base"
            )
        
        # Aplicar filtros
        df_ot_filtrado = df_todas_ot_base.copy()
        
        if filtrar_estado_ot != "TODOS":
            df_ot_filtrado = df_ot_filtrado[df_ot_filtrado['estado'] == filtrar_estado_ot]
        
        if filtrar_sistema_ot != "TODAS" and 'sistema' in df_ot_filtrado.columns:
            df_ot_filtrado = df_ot_filtrado[df_ot_filtrado['sistema'] == filtrar_sistema_ot]
        
        if filtrar_prioridad_ot != "TODAS" and 'prioridad_nueva' in df_ot_filtrado.columns:
            df_ot_filtrado = df_ot_filtrado[df_ot_filtrado['prioridad_nueva'] == filtrar_prioridad_ot]
        
        # Mostrar estadísticas de los filtros
        st.info(f"📊 Mostrando {len(df_ot_filtrado)} de {len(df_todas_ot_base)} órdenes de trabajo base")
        
        # Seleccionar columnas para mostrar
        columnas_ot = [
            'codigo_padre', 'codigo_ot_base', 'estado', 'tipo_mantenimiento', 
            'componentes', 'responsables_comienzo', 'descripcion_trabajo_realizado',
            'fecha_inicio_mantenimiento', 'duracion_estimada', 'prioridad_nueva',
            'sistema', 'clasificacion', 'ingresado_por', 'ingresado_el'
        ]
        
        # Filtrar solo las columnas que existen
        columnas_mostrar_ot = [col for col in columnas_ot if col in df_ot_filtrado.columns]
        
        # Renombrar columnas para mejor presentación
        df_mostrar_ot = df_ot_filtrado[columnas_mostrar_ot].rename(columns={
            'codigo_padre': 'Código Padre',
            'codigo_ot_base': 'Código OT Base',
            'estado': 'Estado',
            'tipo_mantenimiento': 'Tipo Mantenimiento',
            'componentes': 'Componentes',
            'responsables_comienzo': 'Responsables',
            'descripcion_trabajo_realizado': 'Descripción Trabajo',
            'fecha_inicio_mantenimiento': 'Fecha Inicio',
            'duracion_estimada': 'Duración Estimada',
            'prioridad_nueva': 'Prioridad',
            'sistema': 'Sistema',
            'clasificacion': 'Clasificación',
            'ingresado_por': 'Registrado Por',
            'ingresado_el': 'Fecha Registro'
        })
        
        # Mostrar la tabla con todas las OT
        st.dataframe(df_mostrar_ot, use_container_width=True)
        
    else:
        st.info("📭 No hay órdenes de trabajo base registradas")
    
    st.markdown("---")
    
    # SECCIÓN 2: TODOS LOS AVISOS
    st.subheader("📋 TODOS los Avisos de Mantenimiento")
    
    if not df_todos_avisos.empty:
        # Filtros para avisos
        col_filtro4, col_filtro5, col_filtro6 = st.columns(3)
        
        with col_filtro4:
            filtrar_estado_aviso = st.selectbox(
                "Filtrar por estado:",
                ["TODOS", "INGRESADO", "PROGRAMADO", "RECHAZADO"],
                key="filtro_estado_aviso"
            )
        
        with col_filtro5:
            # Obtener áreas únicas para el filtro
            areas = ["TODAS"]
            if 'area' in df_todos_avisos.columns:
                areas.extend(sorted(df_todos_avisos['area'].dropna().unique().tolist()))
            
            filtrar_area_aviso = st.selectbox(
                "Filtrar por área:",
                areas,
                key="filtro_area_aviso"
            )
        
        with col_filtro6:
            filtrar_riesgo_aviso = st.selectbox(
                "Filtrar por riesgo:",
                ["TODOS", "SI", "NO"],
                key="filtro_riesgo_aviso"
            )
        
        # Aplicar filtros
        df_avisos_filtrado = df_todos_avisos.copy()
        
        if filtrar_estado_aviso != "TODOS":
            df_avisos_filtrado = df_avisos_filtrado[df_avisos_filtrado['estado'] == filtrar_estado_aviso]
        
        if filtrar_area_aviso != "TODAS" and 'area' in df_avisos_filtrado.columns:
            df_avisos_filtrado = df_avisos_filtrado[df_avisos_filtrado['area'] == filtrar_area_aviso]
        
        if filtrar_riesgo_aviso != "TODOS" and 'hay_riesgo' in df_avisos_filtrado.columns:
            df_avisos_filtrado = df_avisos_filtrado[df_avisos_filtrado['hay_riesgo'] == filtrar_riesgo_aviso]
        
        # Mostrar estadísticas de los filtros
        st.info(f"📊 Mostrando {len(df_avisos_filtrado)} de {len(df_todos_avisos)} avisos")
        
        # Seleccionar columnas para mostrar
        columnas_avisos = [
            'codigo_padre', 'codigo_mantto', 'estado', 'antiguedad', 'area', 'equipo',
            'hay_riesgo', 'ingresado_por', 'ingresado_el', 'descripcion_problema'
        ]
        
        # Filtrar solo las columnas que existen
        columnas_mostrar_avisos = [col for col in columnas_avisos if col in df_avisos_filtrado.columns]
        
        # Renombrar columnas para mejor presentación
        df_mostrar_avisos = df_avisos_filtrado[columnas_mostrar_avisos].rename(columns={
            'codigo_padre': 'Código Padre',
            'codigo_mantto': 'Código Mantenimiento',
            'estado': 'Estado',
            'antiguedad': 'Antigüedad (días)',
            'area': 'Área',
            'equipo': 'Equipo',
            'hay_riesgo': 'Hay Riesgo',
            'ingresado_por': 'Ingresado Por',
            'ingresado_el': 'Fecha Ingreso',
            'descripcion_problema': 'Descripción Problema'
        })
        
        # Mostrar la tabla con todos los avisos
        st.dataframe(df_mostrar_avisos, use_container_width=True)
        
        # Botón para exportar datos de avisos
        if st.button("📤 Exportar Datos Avisos a Excel", key="export_avisos"):
            try:
                # Crear un archivo Excel en memoria
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_mostrar_avisos.to_excel(writer, sheet_name='Avisos_Registro_Total', index=False)
                
                output.seek(0)
                
                # Descargar el archivo
                st.download_button(
                    label="⬇️ Descargar Archivo Excel Avisos",
                    data=output,
                    file_name=f"registro_total_avisos_{date.today()}.xlsx",
                    mime="application/vnd.ms-excel",
                    key="download_avisos"
                )
            except Exception as e:
                st.error(f"Error al exportar: {e}")
        
    else:
        st.info("📭 No hay avisos de mantenimiento registrados")

    # SECCIÓN 3: GRÁFICOS Y ESTADÍSTICAS
    st.markdown("---")
    st.subheader("📈 Estadísticas y Gráficos")
    
    col_graf1, col_graf2 = st.columns(2)
    
    with col_graf1:
        if not df_todas_ot_base.empty and 'estado' in df_todas_ot_base.columns:
            st.write("📊 Distribución de Estados - OT Base")
            estado_counts = df_todas_ot_base['estado'].value_counts()
            st.bar_chart(estado_counts)
    
    with col_graf2:
        if not df_todos_avisos.empty and 'estado' in df_todos_avisos.columns:
            st.write("📊 Distribución de Estados - Avisos")
            estado_avisos_counts = df_todos_avisos['estado'].value_counts()
            st.bar_chart(estado_avisos_counts)
    
    col_graf3, col_graf4 = st.columns(2)
    
    with col_graf3:
        if not df_todas_ot_base.empty and 'sistema' in df_todas_ot_base.columns:
            st.write("🔧 Distribución por Sistema - OT Base")
            sistema_counts = df_todas_ot_base['sistema'].value_counts().head(10)
            st.bar_chart(sistema_counts)
    
    with col_graf4:
        if not df_todos_avisos.empty and 'area' in df_todos_avisos.columns:
            st.write("🏭 Distribución por Área - Avisos")
            area_counts = df_todos_avisos['area'].value_counts()
            st.bar_chart(area_counts)

# ============================ PESTAÑA 6: OT PENDIENTES ======================================
with tab6:
    st.header("⏳ Registrar Nueva OT Pendiente")
    
    # SECCIÓN FUERA DEL FORMULARIO
    st.subheader("🔧 Información Básica de la OT")
    
    col_info1, col_info2, col_info3 = st.columns(3)
    
    with col_info1:
        st.write("🔢 Código de OT")
        
        # ✅ CORREGIDO: Buscar OT en estado PROGRAMADO o PENDIENTE
        try:
            codigos_base_df = pd.read_sql(
                'SELECT codigo_ot_base FROM ot_unicas WHERE estado IN ("PROGRAMADO", "PENDIENTE") ORDER BY codigo_ot_base DESC', 
                conn_ot_unicas
            )
            
            if not codigos_base_df.empty:
                codigos_base = codigos_base_df['codigo_ot_base'].tolist()
                codigo_ot_base = st.selectbox(
                    "Seleccione el código OT (PROGRAMADAS o PENDIENTES):",
                    codigos_base,
                    key="select_ot_base_pendientes"
                )
                
                # Mostrar estado actual de la OT seleccionada
                if codigo_ot_base:
                    estado_actual = pd.read_sql(
                        'SELECT estado FROM ot_unicas WHERE codigo_ot_base = ?',
                        conn_ot_unicas,
                        params=(codigo_ot_base,)
                    )
                    if not estado_actual.empty:
                        estado = estado_actual.iloc[0]['estado']
                        if estado == "PROGRAMADO":
                            st.warning(f"📋 Estado actual: {estado}")
                        else:
                            st.info(f"📋 Estado actual: {estado}")
                
                st.success(f"✅ OT seleccionada: {codigo_ot_base}")
            else:
                st.warning("⚠️ No hay OT PROGRAMADAS o PENDIENTES disponibles")
                st.info("Primero debe programar una OT en la pestaña '🛠️ AVISOS EN TRATAMIENTO'")
                codigo_ot_base = None
        except Exception as e:
            st.error(f"Error al obtener códigos OT: {e}")
            codigo_ot_base = None
        
        # Generar código SUB OT automático con sufijo solo si hay OT base seleccionada
        if codigo_ot_base:
            try:
                nuevo_codigo_sub_ot = generar_codigo_ot_con_sufijo(codigo_ot_base)
                st.success(f"**Código SUB OT generado:**")
                st.info(f"**{nuevo_codigo_sub_ot}**")
            except Exception as e:
                st.error(f"Error al generar código SUB OT: {e}")
                nuevo_codigo_sub_ot = f"{codigo_ot_base}-01"
        else:
            nuevo_codigo_sub_ot = None
    
    with col_info2:
        tipo_mantenimiento = st.selectbox(
            "Tipo de Mantenimiento *",
            ["CORRECTIVO", "PREVENTIVO", "PREDICTIVO", "MANTENIBILIDAD", "OTRO"],
            key="select_tipo_mant_pendientes"
        )
    
    with col_info3:
        tipo_preventivo = st.selectbox(
            "Tipo de Preventivo",
            ["NO APLICA", "PROGRAMADO", "CONDICIONAL", "PREDICTIVO", "TAREAS MENORES"],
            disabled=(tipo_mantenimiento != "PREVENTIVO"),
            help="Solo aplica si el tipo de mantenimiento es PREVENTIVO",
            key="select_tipo_preventivo_pendientes"
        )
    
    st.markdown("---")
    
    # FORMULARIO - Solo habilitado si hay OT base seleccionada
    with st.form("ot_pendientes_form", clear_on_submit=True):
        st.subheader("📝 Formulario de Registro - Detalles de la OT")
        
        # Deshabilitar el formulario si no hay OT base seleccionada
        if not codigo_ot_base:
            st.warning("🔒 Seleccione una OT PROGRAMADA para habilitar el formulario")
            disabled_form = True
        else:
            disabled_form = False
        
        col1, col2 = st.columns(2)
        
        with col1:
            componentes = st.text_area(
                "Componentes *",
                placeholder="Lista de componentes involucrados...",
                height=80,
                key="textarea_componentes_pendientes",
                disabled=disabled_form
            )
        
        with col2:
            responsables_comienzo = st.text_input(
                "Responsables del Comienzo de la Actividad *",
                placeholder="Nombres de los responsables...",
                key="input_responsables_pendientes",
                disabled=disabled_form
            )
        
        st.subheader("📋 Detalles del Trabajo")
        descripcion_trabajo_realizado = st.text_area(
            "Descripción del Trabajo Realizado *",
            placeholder="Describa detalladamente el trabajo realizado...",
            height=100,
            key="textarea_desc_trabajo_pendientes",
            disabled=disabled_form
        )
        
        st.subheader("⏰ Información de Tiempos")
        col3, col4, col5 = st.columns(3)
        
        with col3:
            paro_linea = st.selectbox(
                "¿Tuvo que parar la línea para hacer el mantenimiento? *",
                ["SI", "NO"],
                key="select_paro_linea_pendientes",
                disabled=disabled_form
            )
        
        with col4:
            fecha_inicio = st.date_input(
                "¿Cuándo inició el mantenimiento? *",
                value=date.today(),
                key="date_inicio_pendientes",
                disabled=disabled_form
            )
            
            hora_inicio = st.time_input(
                "Hora de inicio *",
                value=datetime.now().time(),
                key="time_inicio_pendientes",
                disabled=disabled_form
            )
        
        with col5:
            hora_finalizacion = st.time_input(
                "Hora que finalizó el mantenimiento *",
                value=datetime.now().time(),
                key="time_finalizacion_pendientes",
                disabled=disabled_form
            )
        
        # Información del usuario que registra
        st.info(f"**Registrado por:** {st.session_state.nombre_completo}")
        
        # Botón de envío - deshabilitado si no hay OT base
        submitted = st.form_submit_button(
            "💾 Registrar OT Pendiente", 
            disabled=disabled_form,
            type="primary" if not disabled_form else "secondary"
        )
        
        if submitted and codigo_ot_base:
            # Asegurarse de que nuevo_codigo_sub_ot esté definido
            if 'nuevo_codigo_sub_ot' not in locals() or not nuevo_codigo_sub_ot:
                nuevo_codigo_sub_ot = f"{codigo_ot_base}-01"
                
            if (nuevo_codigo_sub_ot and tipo_mantenimiento and componentes and 
                responsables_comienzo and descripcion_trabajo_realizado and 
                paro_linea and fecha_inicio and hora_inicio and hora_finalizacion):
                
                try:
                    # Obtener el código padre relacionado
                    codigo_padre = obtener_codigo_padre_por_ot_base(codigo_ot_base)
                    
                    # Insertar en base de datos OT usando el Código SUB OT
                    ejecutar_consulta_segura(conn_ot_sufijos, '''
                        INSERT INTO ot_sufijos (
                            codigo_ot_sufijo, codigo_ot_base, estado, tipo_mantenimiento, tipo_preventivo, componentes,
                            responsables_comienzo, descripcion_trabajo_realizado, paro_linea,
                            fecha_inicio_mantenimiento, hora_inicio_mantenimiento, hora_finalizacion_mantenimiento,
                            ingresado_por, ingresado_el
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        nuevo_codigo_sub_ot, codigo_ot_base, 'PENDIENTE', tipo_mantenimiento, 
                        tipo_preventivo if tipo_mantenimiento == "PREVENTIVO" else "NO APLICA",
                        componentes, responsables_comienzo, descripcion_trabajo_realizado, paro_linea,
                        fecha_inicio, hora_inicio.strftime('%H:%M:%S'), hora_finalizacion.strftime('%H:%M:%S'),
                        st.session_state.nombre_completo, date.today()
                    ))
                    
                    st.info("🔄 Actualizando estados relacionados...")
                    
                    if actualizar_estados_a_pendiente(codigo_ot_base, codigo_padre):
                        st.success("✅ **Actualización en cascada completada:**")
                        st.success(f"• OT Base {codigo_ot_base} → PENDIENTE")
                        if codigo_padre:
                            st.success(f"• Aviso original {codigo_padre} → EN PROCESO")
                        else:
                            st.warning("⚠️ No se encontró aviso original relacionado")
                    else:
                        st.error("❌ Error en la actualización en cascada")
                    
                    st.info("📝 Actualizando datos en OT única...")

                    # Campos que solo se actualizan en el primer pendiente (no se acumulan)
                    try:
                        # Verificar si ya existe fecha_inicio_mantenimiento
                        resultado = pd.read_sql(
                            'SELECT fecha_inicio_mantenimiento FROM ot_unicas WHERE codigo_ot_base = ?',
                            conn_ot_unicas,
                            params=(codigo_ot_base,)
                        )
                        
                        # Solo actualizar si no existe fecha_inicio_mantenimiento (primer pendiente)
                        if resultado.empty or resultado.iloc[0]['fecha_inicio_mantenimiento'] is None:
                            ejecutar_consulta_segura(conn_ot_unicas, '''
                                UPDATE ot_unicas SET 
                                    fecha_inicio_mantenimiento = ?,
                                    hora_inicio_mantenimiento = ?
                                WHERE codigo_ot_base = ?
                            ''', (
                                fecha_inicio, 
                                hora_inicio.strftime('%H:%M:%S'),
                                codigo_ot_base
                            ))
                            st.success("✅ Fecha y hora de inicio actualizadas (primer pendiente)")
                        else:
                            st.info("ℹ️ Fecha y hora de inicio ya estaban registradas")
                            
                    except Exception as e:
                        st.error(f"❌ Error actualizando fechas: {e}")

                    # Campos que se acumulan continuamente
                    campos_acumular_pendiente = [
                        ('componentes', componentes),
                        ('responsables_comienzo', responsables_comienzo),
                        ('descripcion_trabajo_realizado', descripcion_trabajo_realizado),
                        ('paro_linea', paro_linea),
                        ('tipo_preventivo', tipo_preventivo if tipo_mantenimiento == "PREVENTIVO" else "NO APLICA")
                    ]

                    for campo, valor in campos_acumular_pendiente:
                        if valor:  # Solo acumular si hay valor
                            if acumular_valor_ot_unica(codigo_ot_base, campo, valor):
                                st.success(f"✅ {campo} acumulado")
                            else:
                                st.error(f"❌ Error acumulando {campo}")
                    
                    st.success(f"✅ **OT Pendiente registrada correctamente**")
                    st.success(f"📋 **Código OT:** {codigo_ot_base}")
                    st.success(f"📋 **Código SUB OT:** {nuevo_codigo_sub_ot}")
                    log_accion(st.session_state.usuario, "OT_PENDIENTE_CREADA", f"OT: {codigo_ot_base}, SUB-OT: {nuevo_codigo_sub_ot}")
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"❌ Error al registrar: {e}")
                    log_accion(st.session_state.usuario, "ERROR_OT_PENDIENTE", f"Error: {str(e)}")
            else:
                st.error("❌ **Complete todos los campos obligatorios marcados con ***")

# ============================== PESTAÑA 7: OT CULMINADAS ===================================
with tab7:
    st.header("🏁 Registrar OT Culminada")
    
    # SECCIÓN FUERA DEL FORMULARIO
    st.subheader("🔧 Información Básica de la OT")
    
    col_info1, col_info2, col_info3 = st.columns(3)
    
    with col_info1:
        st.write("🔢 Código de OT")
        
        # Obtener códigos base de OT en estado PROGRAMADO o PENDIENTE
        try:
            codigos_base_df = pd.read_sql(
                'SELECT codigo_ot_base FROM ot_unicas WHERE estado IN ("PROGRAMADO", "PENDIENTE") ORDER BY codigo_ot_base DESC', 
                conn_ot_unicas
            )
            
            if not codigos_base_df.empty:
                codigos_base = codigos_base_df['codigo_ot_base'].tolist()
                codigo_ot_base = st.selectbox(
                    "Seleccione el código OT (PROGRAMADAS o PENDIENTES):",
                    codigos_base,
                    key="select_ot_base_culminadas"
                )
                
                # Mostrar estado actual de la OT seleccionada
                estado_actual = pd.read_sql(
                    'SELECT estado FROM ot_unicas WHERE codigo_ot_base = ?',
                    conn_ot_unicas,
                    params=(codigo_ot_base,)
                )
                if not estado_actual.empty:
                    estado = estado_actual.iloc[0]['estado']
                    if estado == "PROGRAMADO":
                        st.warning(f"📋 Estado actual: {estado}")
                    else:
                        st.info(f"📋 Estado actual: {estado}")
                
            else:
                st.warning("⚠️ No hay OT PROGRAMADAS o PENDIENTES disponibles")
                st.info("Primero debe programar una OT en la pestaña '🛠️ AVISOS EN TRATAMIENTO' o crear una OT pendiente")
                codigo_ot_base = None
        except Exception as e:
            st.error(f"Error al obtener códigos OT: {e}")
            codigo_ot_base = None
        
        # Generar código SUB OT automático con sufijo solo si hay OT base seleccionada
        if codigo_ot_base:
            try:
                nuevo_codigo_sub_ot = generar_codigo_ot_con_sufijo(codigo_ot_base)
                st.success(f"**Código SUB OT generado:**")
                st.info(f"**{nuevo_codigo_sub_ot}**")
            except Exception as e:
                st.error(f"Error al generar código SUB OT: {e}")
                nuevo_codigo_sub_ot = f"{codigo_ot_base}-01"
        else:
            nuevo_codigo_sub_ot = None
    
    with col_info2:
        tipo_mantenimiento = st.selectbox(
            "Tipo de Mantenimiento *",
            ["CORRECTIVO", "PREVENTIVO", "PREDICTIVO", "MANTENIBILIDAD", "OTRO"],
            key="select_tipo_mant_culminadas"
        )
    
    with col_info3:
        tipo_preventivo = st.selectbox(
            "Tipo de Preventivo",
            ["NO APLICA", "PROGRAMADO", "CONDICIONAL", "PREDICTIVO", "TAREAS MENORES"],
            disabled=(tipo_mantenimiento != "PREVENTIVO"),
            help="Solo aplica si el tipo de mantenimiento es PREVENTIVO",
            key="select_tipo_preventivo_culminadas"
        )
    
    st.markdown("---")
    
    # FORMULARIO PARA REGISTRAR OT CULMINADAS - Solo habilitado si hay OT base seleccionada
    with st.form("ot_culminadas_form", clear_on_submit=True):
        st.subheader("📝 Formulario de Registro - OT Culminada")
        
        # Deshabilitar el formulario si no hay OT base seleccionada
        if not codigo_ot_base:
            st.warning("🔒 Seleccione una OT PROGRAMADA o PENDIENTE para habilitar el formulario")
            disabled_form = True
        else:
            disabled_form = False
        
        # Componentes y Responsables
        col4, col5 = st.columns(2)
        
        with col4:
            componentes = st.text_area(
                "Componentes *",
                placeholder="Lista de componentes involucrados en el mantenimiento...",
                height=80,
                key="textarea_componentes_culminadas",
                disabled=disabled_form
            )
        
        with col5:
            responsables_finalizacion = st.text_input(
                "Responsables de la Finalización de la Actividad *",
                placeholder="Nombres de los responsables que finalizaron el trabajo...",
                key="input_responsables_culminadas",
                disabled=disabled_form
            )
        
        # Fechas y Horas
        st.subheader("⏰ Información de Tiempos")
        col6, col7, col8, col9 = st.columns(4)
        
        with col6:
            fecha_finalizacion = st.date_input(
                "¿Cuándo finalizó el mantenimiento? *",
                value=date.today(),
                key="date_finalizacion_culminadas",
                disabled=disabled_form
            )
        
        with col7:
            hora_inicio = st.time_input(
                "Hora de inicio *",
                value=datetime.now().time(),
                key="time_inicio_culminadas",
                disabled=disabled_form
            )
        
        with col8:
            hora_final = st.time_input(
                "Hora final *",
                value=datetime.now().time(),
                key="time_final_culminadas",
                disabled=disabled_form
            )
        
        with col9:
            paro_linea = st.selectbox(
                "¿Tuvo que parar la línea? *",
                ["SI", "NO"],
                key="select_paro_linea_culminadas",
                disabled=disabled_form
            )
        
        # Descripción del trabajo
        st.subheader("📋 Detalles del Trabajo")
        descripcion_trabajo_realizado = st.text_area(
            "Descripción del Trabajo Realizado *",
            placeholder="Describa detalladamente el trabajo realizado...",
            height=120,
            key="textarea_desc_trabajo_culminadas",
            disabled=disabled_form
        )
        
        # Comentarios adicionales
        st.subheader("💬 Comentarios Adicionales")
        comentario = st.text_area(
            "Comentario",
            placeholder="Agregue cualquier comentario adicional sobre el mantenimiento...",
            height=80,
            key="textarea_comentario_culminadas",
            disabled=disabled_form
        )
        
        # Adjuntar evidencia
        st.subheader("🖼️ Evidencia Fotográfica")
        evidencia_foto = st.file_uploader(
            "Adjuntar evidencia (foto) *",
            type=['jpg', 'jpeg', 'png', 'gif'],
            help="Suba una foto como evidencia del trabajo realizado",
            key="uploader_evidencia_culminadas",
            disabled=disabled_form
        )
        
        # Mostrar vista previa si se subió una imagen
        if evidencia_foto is not None:
            st.image(evidencia_foto, caption="Vista previa de la evidencia", width=300)
        
        # Información del usuario que registra
        st.info(f"**Registrado por:** {st.session_state.nombre_completo}")
        
        # Botón de envío - deshabilitado si no hay OT base
        submitted = st.form_submit_button(
            "💾 Registrar OT Culminada", 
            disabled=disabled_form,
            type="primary" if not disabled_form else "secondary"
        )
        
        if submitted and codigo_ot_base:
            # Asegurarse de que nuevo_codigo_sub_ot esté definido
            if 'nuevo_codigo_sub_ot' not in locals():
                nuevo_codigo_sub_ot = f"{codigo_ot_base}-01"
                
            if (nuevo_codigo_sub_ot and tipo_mantenimiento and componentes and 
                responsables_finalizacion and fecha_finalizacion and 
                hora_inicio and hora_final and paro_linea and 
                descripcion_trabajo_realizado and evidencia_foto):
                
                try:
                    # CORRECCION: Usar los nombres de columna CORRECTOS
                    imagen_final_nombre = evidencia_foto.name
                    imagen_final_datos = evidencia_foto.read()

                    # Obtener el código padre relacionado
                    codigo_padre = obtener_codigo_padre_por_ot_base(codigo_ot_base)

                    # ACTUALIZAR EL REGISTRO EXISTENTE EN LUGAR DE CREAR UNO NUEVO
                    # Buscar si ya existe una OT con este código
                    ot_existente = pd.read_sql(	
                        'SELECT * FROM ot_sufijos WHERE codigo_ot_sufijo = ?', 
                        conn_ot_sufijos,
                        params=(nuevo_codigo_sub_ot,)
                    )
                    
                    if not ot_existente.empty:
                        # Actualizar la OT existente a estado CULMINADO
                        ejecutar_consulta_segura(conn_ot_sufijos, '''
                            UPDATE ot_sufijos SET 
                                estado = "CULMINADO",
                                tipo_mantenimiento = ?, 
                                tipo_preventivo = ?, 
                                componentes = ?,
                                responsables_comienzo = ?, 
                                descripcion_trabajo_realizado = ?, 
                                paro_linea = ?,
                                fecha_inicio_mantenimiento = ?, 
                                hora_inicio_mantenimiento = ?, 
                                hora_finalizacion_mantenimiento = ?,
                                observaciones_cierre = ?, 
                                imagen_final_nombre = ?,  
                                imagen_final_datos = ?,   
                                responsables_finalizacion = ?,
                                fecha_finalizacion = ?,
                                hora_final = ?,
                                comentario = ?
                             WHERE codigo_ot_sufijo = ?
                        ''', (
                            tipo_mantenimiento, 
                            tipo_preventivo if tipo_mantenimiento == "PREVENTIVO" else "NO APLICA",
                            componentes, responsables_finalizacion, descripcion_trabajo_realizado, paro_linea,
                            fecha_finalizacion, hora_inicio.strftime('%H:%M:%S'), hora_final.strftime('%H:%M:%S'),
                            comentario, imagen_final_nombre, imagen_final_datos,  
                            responsables_finalizacion, fecha_finalizacion, hora_final.strftime('%H:%M:%S'),
                            comentario, nuevo_codigo_sub_ot
                        ))
                    else:
                        # Crear nuevo registro con estado CULMINADO (por si no existe)
                        ejecutar_consulta_segura(conn_ot_sufijos, '''
                            INSERT INTO ot_sufijos (
                                codigo_ot_sufijo, codigo_ot_base, estado, tipo_mantenimiento, tipo_preventivo, componentes,
                                responsables_comienzo, descripcion_trabajo_realizado, paro_linea,
                                fecha_inicio_mantenimiento, hora_inicio_mantenimiento, hora_finalizacion_mantenimiento,
                                observaciones_cierre, imagen_final_nombre, imagen_final_datos,  
                                responsables_finalizacion, fecha_finalizacion, hora_final, comentario,
                                ingresado_por, ingresado_el
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            nuevo_codigo_sub_ot, codigo_ot_base, 'CULMINADO', tipo_mantenimiento, 
                            tipo_preventivo if tipo_mantenimiento == "PREVENTIVO" else "NO APLICA",
                            componentes, responsables_finalizacion, descripcion_trabajo_realizado, paro_linea,
                            fecha_finalizacion, hora_inicio.strftime('%H:%M:%S'), hora_final.strftime('%H:%M:%S'),
                            comentario, imagen_final_nombre, imagen_final_datos,  
                            responsables_finalizacion, fecha_finalizacion, hora_final.strftime('%H:%M:%S'), comentario,
                            st.session_state.nombre_completo, date.today()
                        ))
                    
                    # ACTUALIZACIÓN EN CASCADA
                    st.info("🔄 Actualizando estados relacionados...")

                    if actualizar_estados_cascada(codigo_ot_base, codigo_padre):
                        st.success("✅ **Actualización en cascada completada:**")
                        st.success(f"• Todas las SUB-OT de {codigo_ot_base} → CULMINADO")
                        st.success(f"• OT Base {codigo_ot_base} → CULMINADO")
                        if codigo_padre:
                            st.success(f"• Aviso original {codigo_padre} → CULMINADO")
                        else:
                            st.warning("⚠️ No se encontró aviso original relacionado")
                    else:
                        st.error("❌ Error en la actualización en cascada")

                    # ACTUALIZAR CAMPOS EN OT ÚNICA - CULMINADOS
                    st.info("📝 Actualizando datos en OT única...")

                    # Campos específicos de culminados (no se acumulan, se sobreescriben)
                    try:
                        ejecutar_consulta_segura(conn_ot_unicas, '''
                            UPDATE ot_unicas SET 
                                fecha_finalizacion = ?,
                                hora_final = ?,
                                imagen_final_nombre = ?,  
                                imagen_final_datos = ?    
                            WHERE codigo_ot_base = ?
                        ''', (
                            fecha_finalizacion,
                            hora_final.strftime('%H:%M:%S'),
                            imagen_final_nombre,  
                            imagen_final_datos,   
                            codigo_ot_base
                        ))
                        st.success("✅ Campos de culminación actualizados")
                    except Exception as e:
                        st.error(f"❌ Error actualizando campos de culminación: {e}")

                    # Campos que se acumulan continuamente
                    campos_acumular_culminado = [
                        ('tipo_preventivo', tipo_preventivo if tipo_mantenimiento == "PREVENTIVO" else "NO APLICA"),
                        ('comentario', comentario)
                    ]

                    for campo, valor in campos_acumular_culminado:
                        if valor:
                            if acumular_valor_ot_unica(codigo_ot_base, campo, valor):
                                st.success(f"✅ {campo} acumulado")
                            else:
                                st.error(f"❌ Error acumulando {campo}")

                    # Campos que se acumulan de los datos del formulario
                    campos_extra_acumular = [
                        ('componentes', componentes),
                        ('responsables_finalizacion', responsables_finalizacion),
                        ('descripcion_trabajo_realizado', descripcion_trabajo_realizado),
                        ('paro_linea', paro_linea)
                    ]

                    for campo, valor in campos_extra_acumular:
                        if valor:
                            if acumular_valor_ot_unica(codigo_ot_base, campo, valor):
                                st.success(f"✅ {campo} acumulado")
                            else:
                                st.error(f"❌ Error acumulando {campo}")

                    st.success(f"✅ **OT Culminada registrada correctamente**")
                    st.success(f"📋 **Código OT:** {codigo_ot_base}")
                    st.success(f"📋 **Código SUB OT:** {nuevo_codigo_sub_ot}")
                    log_accion(st.session_state.usuario, "OT_CULMINADA_CREADA", f"OT: {codigo_ot_base}, SUB-OT: {nuevo_codigo_sub_ot}")
                    
                except Exception as e:
                    st.error(f"❌ Error al registrar OT culminada: {e}")
                    log_accion(st.session_state.usuario, "ERROR_OT_CULMINADA", f"Error: {str(e)}")
            else:
                st.error("❌ **Complete todos los campos obligatorios marcados con ***")

# =============================== PESTAÑA 8: BUSCAR OT ======================================
with tab8:
    st.header("🔍 Buscar Órdenes de Trabajo")
    
    # Búsqueda rápida en la misma pestaña
    busqueda_rapida = st.text_input(
        "🔍 Búsqueda rápida (código OT):",
        placeholder="Ingrese código OT...",
        key="busqueda_rapida_principal"
    )
    
    # Métodos de búsqueda
    metodo_busqueda = st.radio(
        "Seleccione método de búsqueda:",
        ["🔤 Por Código OT", "🔢 Por Código Equipo", "🔧 Por Nombre Equipo", "⚙️ Por Sistema", "📊 Por Estado", "👀 Ver Todas", "📋 Información Completa"],
        horizontal=True
    )
    
    resultados = pd.DataFrame()
    
    # Si hay búsqueda rápida, buscar por código
    if busqueda_rapida:
        st.info(f"🔍 Buscando código: '{busqueda_rapida}'")
        resultados = buscar_ot_unicas_por_codigo(busqueda_rapida)
    
    # Si no hay búsqueda rápida, usar el método seleccionado
    elif metodo_busqueda == "🔤 Por Código OT":
        st.subheader("Buscar por Código OT")
        codigo_buscar = st.text_input(
            "Ingrese código OT (puede ser parcial):",
            placeholder="Ej: OT - 00000001 o solo 00000001",
            key="codigo_buscar_detallado"
        )
        
        if codigo_buscar:
            resultados = buscar_ot_unicas_por_codigo(codigo_buscar)
    
    elif metodo_busqueda == "🔢 Por Código Equipo":
        st.subheader("Buscar por Código de Equipo")
        
        # Obtener códigos de equipo únicos de ot_unicas (CORREGIDO)
        codigos_equipo = obtener_codigos_equipo_para_busqueda()
        
        if not codigos_equipo.empty:
            codigo_seleccionado = st.selectbox(
                "Seleccione código de equipo:",
                codigos_equipo['codigo_equipo'].tolist(),
                key="codigo_equipo_selectbox"
            )
            
            if codigo_seleccionado:
                resultados = buscar_ot_por_equipo(codigo_seleccionado)
        else:
            st.info("No hay códigos de equipo registrados en las OT")
    
    elif metodo_busqueda == "🔧 Por Nombre Equipo":
        st.subheader("Buscar por Nombre de Equipo")
        
        # Obtener nombres de equipo únicos de avisos
        nombres_equipo = obtener_nombres_equipo_para_busqueda()
        
        if not nombres_equipo.empty:
            nombre_seleccionado = st.selectbox(
                "Seleccione nombre de equipo:",
                nombres_equipo['equipo'].tolist(),
                key="nombre_equipo_selectbox"
            )
            
            if nombre_seleccionado:
                resultados = buscar_ot_por_nombre_equipo(nombre_seleccionado)
        else:
            st.info("No hay nombres de equipo registrados en los avisos")
    
    elif metodo_busqueda == "⚙️ Por Sistema":
        st.subheader("Buscar por Sistema")
        
        # Obtener sistemas únicos de ot_unicas
        try:
            sistemas_unicos = pd.read_sql(
                'SELECT DISTINCT sistema FROM ot_unicas WHERE sistema IS NOT NULL AND sistema != "" ORDER BY sistema', 
                conn_ot_unicas
            )
            
            if not sistemas_unicos.empty:
                sistema_seleccionado = st.selectbox(
                    "Seleccione sistema:",
                    sistemas_unicos['sistema'].tolist(),
                    key="sistema_selectbox"
                )
                
                if sistema_seleccionado:
                    resultados = buscar_ot_por_sistema(sistema_seleccionado)
            else:
                st.info("No hay sistemas registrados en las OT")
        except Exception as e:
            st.error(f"Error al obtener sistemas: {e}")
            st.info("La columna 'sistema' podría no existir en la tabla")
    
    elif metodo_busqueda == "📊 Por Estado":
        st.subheader("Buscar por Estado")
        
        estado_seleccionado = st.selectbox(
            "Seleccione estado:",
            ["PROGRAMADO", "CULMINADO", "PENDIENTE"],
            key="estado_selectbox"
        )
        
        if estado_seleccionado:
            resultados = buscar_ot_por_estado(estado_seleccionado)
    
    elif metodo_busqueda == "👀 Ver Todas":
        st.subheader("Todas las Órdenes de Trabajo")
        resultados = obtener_todas_ot_unicas()
    
    elif metodo_busqueda == "📋 Información Completa":
        st.subheader("Información Completa de OT con datos de Avisos")
        resultados = obtener_info_completa_ot()
    
    # Mostrar resultados
    if not resultados.empty:
        st.success(f"✅ Se encontraron {len(resultados)} resultado(s)")
        
        # Mostrar tabla principal
        st.dataframe(resultados, use_container_width=True)
        
        # Estadísticas rápidas
        if len(resultados) > 0:
            st.subheader("📈 Estadísticas de los Resultados")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if 'estado' in resultados.columns:
                    estados_count = resultados['estado'].value_counts()
                    st.metric("Estados diferentes", len(estados_count))
                else:
                    st.metric("Registros", len(resultados))
            
            with col2:
                if 'sistema' in resultados.columns:
                    sistemas_count = resultados['sistema'].value_counts()
                    st.metric("Sistemas diferentes", len(sistemas_count))
            
            with col3:
                if 'tipo_mantenimiento' in resultados.columns:
                    tipos_count = resultados['tipo_mantenimiento'].value_counts()
                    st.metric("Tipos de mantenimiento", len(tipos_count))
            
            with col4:
                if 'prioridad_nueva' in resultados.columns:
                    prioridades_count = resultados['prioridad_nueva'].value_counts()
                    st.metric("Prioridades diferentes", len(prioridades_count))
    
    elif busqueda_rapida or metodo_busqueda not in ["👀 Ver Todas", "📋 Información Completa"]:
        st.info("🔍 No se encontraron resultados para la búsqueda")
    
    else:
        st.info("📭 No hay órdenes de trabajo registradas")

# ==================================== PESTAÑA 9: ELIMINAR REGISTROS ===================================
with tab9:
    st.header("🗑️ Eliminar Registros")
    st.warning("⚠️ **ADVERTENCIA:** Esta acción no se puede deshacer. Use con precaución.")
    
    # Selección del tipo de registro a eliminar
    tipo_registro = st.radio(
        "Seleccione el tipo de registro a eliminar:",
        ["📋 Avisos", "🔧 OT Base", "📦 SUB-OT"],
        horizontal=True
    )
    
    if tipo_registro == "📋 Avisos":
        st.subheader("Eliminar Avisos")
        
        # Obtener todos los avisos
        df_avisos = obtener_todos_los_codigos_avisos()
        
        if not df_avisos.empty:
            st.info(f"📊 Total de avisos: {len(df_avisos)}")
            
            # Mostrar tabla de avisos
            st.dataframe(df_avisos, use_container_width=True)
            
            # Seleccionar aviso a eliminar
            avisos_disponibles = df_avisos['codigo_padre'].tolist()
            aviso_seleccionado = st.selectbox(
                "Seleccione el aviso a eliminar:",
                avisos_disponibles,
                key="select_aviso_eliminar"
            )
            
            if aviso_seleccionado:
                # Mostrar información del aviso seleccionado
                aviso_info = df_avisos[df_avisos['codigo_padre'] == aviso_seleccionado].iloc[0]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Código:** {aviso_info['codigo_padre']}")
                    st.write(f"**Estado:** {aviso_info['estado']}")
                with col2:
                    st.write(f"**Área:** {aviso_info['area']}")
                    st.write(f"**Equipo:** {aviso_info['equipo']}")
                
                # Confirmación de eliminación
                st.error("🚨 **ACCIÓN IRREVERSIBLE**")
                confirmacion = st.checkbox(
                    f"¿Está seguro de que desea eliminar el aviso {aviso_seleccionado}?",
                    key="confirm_aviso_eliminar"
                )
                
                if confirmacion:
                    if st.button("🗑️ Eliminar Aviso", type="secondary"):
                        with st.spinner("Eliminando aviso..."):
                            success, mensaje = eliminar_aviso_por_codigo(aviso_seleccionado)
                            
                            if success:
                                st.success(mensaje)
                                st.rerun()
                            else:
                                st.error(mensaje)
        else:
            st.info("📭 No hay avisos registrados")
    
    elif tipo_registro == "🔧 OT Base":
        st.subheader("Eliminar Órdenes de Trabajo Base")
        
        # Obtener todas las OT base
        df_ot_base = obtener_todos_los_codigos_ot_base()
        
        if not df_ot_base.empty:
            st.info(f"📊 Total de OT base: {len(df_ot_base)}")
            
            # Mostrar tabla de OT base
            st.dataframe(df_ot_base, use_container_width=True)
            
            # Seleccionar OT base a eliminar
            ot_base_disponibles = df_ot_base['codigo_ot_base'].tolist()
            ot_base_seleccionada = st.selectbox(
                "Seleccione la OT base a eliminar:",
                ot_base_disponibles,
                key="select_ot_base_eliminar"
            )
            
            if ot_base_seleccionada:
                # Mostrar información de la OT base seleccionada
                ot_info = df_ot_base[df_ot_base['codigo_ot_base'] == ot_base_seleccionada].iloc[0]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Código:** {ot_info['codigo_ot_base']}")
                    st.write(f"**Estado:** {ot_info['estado']}")
                with col2:
                    st.write(f"**Área:** {ot_info['area']}")
                    st.write(f"**Equipo:** {ot_info['equipo']}")
                
                # Verificar SUB-OT relacionadas
                sub_ots = pd.read_sql(
                    'SELECT codigo_ot_sufijo FROM ot_sufijos WHERE codigo_ot_base = ?',
                    conn_ot_sufijos,
                    params=(ot_base_seleccionada,)
                )
                
                if not sub_ots.empty:
                    st.warning(f"⚠️ Esta OT base tiene {len(sub_ots)} SUB-OT relacionadas:")
                    st.dataframe(sub_ots, use_container_width=True)
                    st.error("No se puede eliminar una OT base que tiene SUB-OT activas.")
                else:
                    # Confirmación de eliminación
                    st.error("🚨 **ACCIÓN IRREVERSIBLE**")
                    confirmacion = st.checkbox(
                        f"¿Está seguro de que desea eliminar la OT base {ot_base_seleccionada}?",
                        key="confirm_ot_base_eliminar"
                    )
                    
                    if confirmacion:
                        if st.button("🗑️ Eliminar OT Base", type="secondary"):
                            with st.spinner("Eliminando OT base..."):
                                success, mensaje = eliminar_ot_base_por_codigo(ot_base_seleccionada)
                                
                                if success:
                                    st.success(mensaje)
                                    st.rerun()
                                else:
                                    st.error(mensaje)
        else:
            st.info("📭 No hay OT base registradas")
    
    elif tipo_registro == "📦 SUB-OT":
        st.subheader("Eliminar SUB-OT")
        
        # Obtener todas las SUB-OT
        df_sub_ot = obtener_todos_los_codigos_sub_ot()
        
        if not df_sub_ot.empty:
            st.info(f"📊 Total de SUB-OT: {len(df_sub_ot)}")
            
            # Mostrar tabla de SUB-OT
            st.dataframe(df_sub_ot, use_container_width=True)
            
            # Seleccionar SUB-OT a eliminar
            sub_ot_disponibles = df_sub_ot['codigo_ot_sufijo'].tolist()
            sub_ot_seleccionada = st.selectbox(
                "Seleccione la SUB-OT a eliminar:",
                sub_ot_disponibles,
                key="select_sub_ot_eliminar"
            )
            
            if sub_ot_seleccionada:
                # Mostrar información de la SUB-OT seleccionada
                sub_ot_info = df_sub_ot[df_sub_ot['codigo_ot_sufijo'] == sub_ot_seleccionada].iloc[0]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Código SUB-OT:** {sub_ot_info['codigo_ot_sufijo']}")
                    st.write(f"**Estado:** {sub_ot_info['estado']}")
                with col2:
                    st.write(f"**Código OT Base:** {sub_ot_info['codigo_ot_base']}")
                
                # Confirmación de eliminación
                st.error("🚨 **ACCIÓN IRREVERSIBLE**")
                confirmacion = st.checkbox(
                    f"¿Está seguro de que desea eliminar la SUB-OT {sub_ot_seleccionada}?",
                    key="confirm_sub_ot_eliminar"
                )
                
                if confirmacion:
                    if st.button("🗑️ Eliminar SUB-OT", type="secondary"):
                        with st.spinner("Eliminando SUB-OT..."):
                            success, mensaje = eliminar_sub_ot_por_codigo(sub_ot_seleccionada)
                            
                            if success:
                                st.success(mensaje)
                                st.rerun()
                            else:
                                st.error(mensaje)
        else:
            st.info("📭 No hay SUB-OT registradas")

#===================Sidebar========================
st.sidebar.header("⚙️ Configuración")

# ✅ ACTUALIZACIÓN DE ESTRUCTURA COMPLETA
if st.sidebar.button("🔄 Actualizar Estructura de Todas las Tablas"):
    actualizar_estructura_todas_tablas()
    st.sidebar.success("✅ Todas las tablas actualizadas")
    st.sidebar.info("💡 Presiona F5 para ver los cambios")

#=================MIGRACION=============================
if st.sidebar.button("🔗 Migrar Relaciones Avisos-OT"):
    migrar_relaciones_avisos_ot()
    st.sidebar.info("💡 Presiona F5 para ver los cambios")

#==============Información del usuario en sidebar======================
st.sidebar.markdown("---")
st.sidebar.header(f"👤 {st.session_state.nombre_completo}")
st.sidebar.info(f"Rol: {st.session_state.rol}")

#==============Funciones de mantenimiento en sidebar================
st.sidebar.markdown("---")
st.sidebar.header("🧹 Mantenimiento")

if st.sidebar.button("🔄 Actualizar Antigüedades"):
    try:
        if actualizar_antiguedades_lotes():
            st.sidebar.success("✅ Antigüedades actualizadas")
        else:
            st.sidebar.warning("⚠️ Algunas antigüedades no se pudieron actualizar")
    except Exception as e:
        st.sidebar.error(f"❌ Error: {e}")
    st.sidebar.info("💡 Presiona F5 para ver los cambios")

#=====================Agregar función para mostrar códigos en el sidebar===================
st.sidebar.markdown("---")
st.sidebar.header("📋 Códigos de Equipos")

if st.sidebar.button("👀 Ver Todos los Códigos"):
    # Crear un expander por área
    areas_organizadas = {
        "COCCION": [],
        "TRITURADO": [],
        "SECADO": [],
        "MOLINO": [],
        "CALDERO": [],
        "PLANTA": [],
        "OTROS": []
    }
    
    for equipo, codigo in CODIGOS_EQUIPOS.items():
        if codigo.startswith(('DG-COC', 'TH-COC', 'PE-COC', 'EX-COC', 'LV-COC', 'TD-COC', 'CL-COC', 'TI-COC', 'BC-COC', 'TK-COC')):
            areas_organizadas["COCCION"].append((equipo, codigo))
        elif codigo.startswith(('TH-TTR', 'TR-TTR', 'LNP-TRT')):
            areas_organizadas["TRITURADO"].append((equipo, codigo))
        elif codigo.startswith(('TH-SEC', 'TB-SEC', 'CF-SEC', 'TR-SEC', 'EX-SEC', 'EF-SEC', 'PU-SEC', 'LZ-SEC', 'VT-SEC', 'CL-SEC')):
            areas_organizadas["SECADO"].append((equipo, codigo))
        elif codigo.startswith(('TH-MOL', 'ML-MOL', 'ZR-MOL', 'CL-MOL', 'VE-MOL', 'BD-MOL')):
            areas_organizadas["MOLINO"].append((equipo, codigo))
        elif codigo.startswith(('QM-CAL', 'CD-CAL', 'TG-CAL', 'MF-CAL')):
            areas_organizadas["CALDERO"].append((equipo, codigo))
        elif codigo.startswith('IN-PL'):
            areas_organizadas["PLANTA"].append((equipo, codigo))
        else:
            areas_organizadas["OTROS"].append((equipo, codigo))
    
    for area, equipos in areas_organizadas.items():
        if equipos:  # Solo mostrar áreas que tienen equipos
            with st.sidebar.expander(f"🏭 {area} ({len(equipos)} equipos)"):
                for equipo, codigo in sorted(equipos):
                    st.sidebar.write(f"**{equipo}:** `{codigo}`")

# Búsqueda rápida de código
st.sidebar.markdown("---")
st.sidebar.subheader("🔍 Buscar Código")
equipo_buscar = st.sidebar.selectbox(
    "Seleccionar equipo:",
    [""] + list(CODIGOS_EQUIPOS.keys()),
    key="buscar_codigo_equipo"
)

if equipo_buscar:
    codigo = obtener_codigo_equipo(equipo_buscar)
    st.sidebar.success(f"**Código:** `{codigo}`")

#=================Verificación de estructura==================
if st.sidebar.button("🔍 Verificar Estructura"):
    verificar_estructura_tablas()

#=================Optimización de base de datos==================
if st.sidebar.button("⚡ Optimizar Bases de Datos"):
    crear_indices_optimizacion()
    st.sidebar.success("✅ Índices de optimización creados")
    st.sidebar.info("💡 Las consultas serán más rápidas")

# ==================== CERRAR CONEXIONES ====================
def close_connections():
    conn_avisos.close()
    conn_ot_unicas.close()
    conn_ot_sufijos.close()

import atexit
atexit.register(close_connections)
