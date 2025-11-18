import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime, date
import hashlib  # Nuevo import para el login

# ==================== SISTEMA DE LOGIN ====================
def init_login_db():
    """Inicializar base de datos de usuarios"""
    conn = sqlite3.connect('data/mantenimiento.db', check_same_thread=False)
    
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
    return conn

def verificar_login(username, password):
    """Verificar credenciales de usuario"""
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    resultado = conn.execute(
        'SELECT username, nombre_completo, rol FROM usuarios WHERE username = ? AND password_hash = ? AND activo = 1',
        (username, password_hash)
    ).fetchone()
    
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
                    st.rerun()
                else:
                    st.error("❌ Usuario o contraseña incorrectos")
            else:
                st.warning("⚠️ Por favor ingrese usuario y contraseña")

# ==================== INICIALIZAR SISTEMA DE LOGIN ====================
if 'autenticado' not in st.session_state:
    st.session_state.autenticado = False

# Configuración de la página
st.set_page_config(
    page_title="Sistema de Mantenimiento", 
    page_icon="🔧", 
    layout="wide"
)

# Inicializar base de datos principal
def init_database():
    if not os.path.exists('data'):
        os.makedirs('data')
    
    conn = sqlite3.connect('data/mantenimiento.db', check_same_thread=False)
    c = conn.cursor()
    
    # Crear tabla base si no existe
    c.execute('''
        CREATE TABLE IF NOT EXISTS registros (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_padre TEXT UNIQUE,
            codigo_mantto TEXT UNIQUE,
            codigo_ot TEXT UNIQUE,
            estado TEXT DEFAULT 'INGRESADO',
            antiguedad INTEGER,
            area TEXT,
            equipo TEXT,
            descripcion_problema TEXT,
            ingresado_por TEXT,
            ingresado_el DATE,
            hay_riesgo TEXT,
            imagen_nombre TEXT,
            imagen_datos BLOB,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Verificar y agregar columnas nuevas si no existen
    c.execute("PRAGMA table_info(registros)")
    columnas_existentes = [col[1] for col in c.fetchall()]
    
    # Lista de TODAS las nuevas columnas a agregar
    nuevas_columnas = [
        ('tipo_ot', 'TEXT DEFAULT "CORRECTIVO"'),
        ('prioridad', 'TEXT DEFAULT "MEDIA"'),
        ('fecha_programada', 'DATE'),
        ('tecnico_asignado', 'TEXT'),
        ('horas_estimadas', 'INTEGER'),
        ('observaciones_cierre', 'TEXT'),
        ('clasificacion', 'TEXT'),
        ('descripcion_trabajo', 'TEXT'),
        ('sistema', 'TEXT'),
        ('codigo_equipo', 'TEXT'),
        ('prioridad_nueva', 'TEXT'),
        ('responsable', 'TEXT'),
        ('alimentador_proveedor', 'TEXT'),
        ('materiales', 'TEXT'),
        ('cantidad_mecanicos', 'INTEGER DEFAULT 0'),
        ('cantidad_electricos', 'INTEGER DEFAULT 0'),
        ('cantidad_soldadores', 'INTEGER DEFAULT 0'),
        ('cantidad_op_vahos', 'INTEGER DEFAULT 0'),
        ('cantidad_calderistas', 'INTEGER DEFAULT 0'),
        ('fecha_estimada_inicio', 'DATE'),
        ('duracion_estimada', 'TEXT')
    ]
    
    # Agregar columnas que no existen
    for columna, tipo in nuevas_columnas:
        if columna not in columnas_existentes:
            try:
                c.execute(f'ALTER TABLE registros ADD COLUMN {columna} {tipo}')
            except Exception as e:
                pass  # Silenciar el error en producción
    
    conn.commit()
    return conn

# Inicializar conexión a base de datos
conn = init_database()
# Inicializar sistema de login
init_login_db()

# Verificar autenticación - MOSTRAR LOGIN SI NO ESTÁ AUTENTICADO
if not st.session_state.autenticado:
    mostrar_login()
    st.stop()

# ==================== APLICACIÓN PRINCIPAL (SOLO SI ESTÁ AUTENTICADO) ====================

# Barra superior con información del usuario
col_user1, col_user2, col_user3 = st.columns([3, 1, 1])
with col_user1:
    st.title("🔧 Sistema de Gestión de Mantenimiento")
with col_user2:
    st.info(f"👤 {st.session_state.nombre_completo}")
with col_user3:
    if st.button("🚪 Cerrar Sesión"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

st.markdown("**Sistema integral de avisos y órdenes de trabajo**")

# Función para verificar y agregar la columna codigo_ot si no existe
def verificar_columna_codigo_ot():
    c = conn.cursor()
    c.execute("PRAGMA table_info(registros)")
    columnas_existentes = [col[1] for col in c.fetchall()]
    
    if 'codigo_ot' not in columnas_existentes:
        try:
            c.execute('ALTER TABLE registros ADD COLUMN codigo_ot TEXT UNIQUE')
            conn.commit()
            st.sidebar.success("✅ Columna 'codigo_ot' agregada a la base de datos")
        except Exception as e:
            st.sidebar.error(f"❌ Error agregando columna codigo_ot: {e}")

# Llamar a esta función después de init_database()
verificar_columna_codigo_ot()

# Generar código OT automático
def generar_codigo_ot():
    ultimo_codigo = pd.read_sql(
        'SELECT codigo_ot FROM registros WHERE codigo_ot LIKE "OT-%" ORDER BY id DESC LIMIT 1', 
        conn
    )
    if len(ultimo_codigo) > 0:
        try:
            ultimo_numero = int(ultimo_codigo.iloc[0]['codigo_ot'].split('-')[1])
            nuevo_numero = ultimo_numero + 1
        except:
            nuevo_numero = 1
    else:
        nuevo_numero = 1
    return f"OT-{nuevo_numero:08d}"

# Obtener datos con diferentes filtros
def obtener_avisos_ingresados():
    return pd.read_sql('SELECT * FROM registros WHERE estado = "INGRESADO" ORDER BY codigo_padre DESC', conn)

def obtener_avisos_en_proceso():
    return pd.read_sql('SELECT * FROM registros WHERE estado = "EN PROCESO" ORDER BY codigo_padre DESC', conn)

def obtener_ot_programadas():
    return pd.read_sql('SELECT * FROM registros WHERE estado = "EN PROCESO" ORDER BY codigo_padre DESC', conn)

def obtener_ot_culminadas():
    return pd.read_sql('SELECT * FROM registros WHERE estado IN ("RESUELTO", "CERRADO") ORDER BY codigo_padre DESC', conn)

def obtener_todos_registros():
    return pd.read_sql('SELECT * FROM registros ORDER BY codigo_padre DESC', conn)

# Generar códigos automáticos
def generar_codigo_padre():
    ultimo_codigo = pd.read_sql(
        'SELECT codigo_padre FROM registros WHERE codigo_padre LIKE "CODP-%" ORDER BY id DESC LIMIT 1', 
        conn
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

def generar_codigo_mantto():
    ultimo_codigo = pd.read_sql(
        'SELECT codigo_mantto FROM registros WHERE codigo_mantto LIKE "AM-%" ORDER BY id DESC LIMIT 1', 
        conn
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

# Calcular antigüedad
def calcular_antiguedad(fecha_ingreso):
    if isinstance(fecha_ingreso, str):
        try:
            fecha_ingreso = date.fromisoformat(fecha_ingreso)
        except:
            return 0
    hoy = date.today()
    return (hoy - fecha_ingreso).days

# Mapeo de áreas a equipos (con opción OTRO)
EQUIPOS_POR_AREA = {
    "COCCION": ["DIGESTOR 1","TH 1","DIGESTOR 2","TH 2","DIGESTOR 3","TH 3","DIGESTOR 4","TH 4","DIGESTOR 5","TH 5","DIGESTOR 6","TH 6","DIGESTOR 7","TH 7","DIGESTOR 8","TH 8","DIGESTOR 9","TH 9","PERCOLADOR 1","PERCOLADOR 2", "OTRO"],
    "RMP": ["HIDROLAVADORA 1","HIDROLAVADORA 2","HIDROLAVADORA 3", "OTRO"],
    "TRITURADO": ["TRITURADOR 100HP","TH DE SILO HUESOS","TH ALIMENTADOR DE TRITURADOR","TH SALIDA TRITURADOR 100HP","HIDROLAVADORA 4","TRITURADO 75HP","TH SALIDA TRITURADOR 75HP", "OTRO"],
    "SECADO": ["TH ALIMENTADOR SEC 1","CAMARA DE FUEGO SEC 1","VENTILADOR DE GASES 1 SEC 1","VENTILADOR DE GASES 2 SEC 1","TAMBOR ROTATIVO SEC 1","CAMARA MATA HUMO SEC 1","CICLON DE FINOS SEC 1","EXHAUSTOR SEC 1","TH FINOS SEC 1","TH SALIDA SEC 1","TH REPROCESO SEC 1","TH 1 INGRESO ENFRIADOR SEC 1","TH 2 INGRESO ENFRIADOR SEC 1","TH 1 ALIMENTADOR SEC 2","TH 2 ALIMENTADOR SEC 2","CAMARA DE FUEGO SEC 2","VENTILADOR DE GASES 1 SEC 2","VENTILADOR DE GASES 2 SEC 2","TAMBOR ROTATIVO SEC 2","CAMARA MATA HUMO SEC 2","CICLON DE FINOS SEC 2","EXHAUSTOR SEC 2","TH FINOS SEC 2","TH SALIDA SEC 2","TH 1 REPROCESO SEC 2","TH 2 REPROCESO SEC 2","TH 1 INGRESO ENFRIADOR SEC 2","TH 2 INGRESO ENFRIADOR SEC 2","TH 3 INGRESO ENFRIADOR SEC 2","TAMBOR ENFRIADOR","SISTEMA DE INYECION DE AIRE","EXHAUSTOR DE ENFRIADOR","TH SALIDA DE ENFRIADOR","PURIFICADOR","TH SALIDA DE PURIFICADOR","TURBINA DE LANZAHARINA","EXCLUSA DE LANZAHARINA","VALVULA ROTATIVA DE LANZAHARINA", "OTRO"],
    "MOLINO": ["TH ALIMENTADOR DE MOL 1","MOLINO 1","TH ALIMENTADOR DE MOL 2","MOLINO 2","CICLON MATA PLOVO","CICLON DE LLEGADA","CICLON DE ENSAQUE","ZARANDA VIBRATORIA","TH ALIMENTAD ZARANDA","FAJA TRANSPORTADORA","EXTRACTOR 1","EXTRACTOR 2","INYECTOR 1","INYECTOR 2","BOMBA DOSIFICADORA 1","BOMBA DOSIFICADORA 2","BOMBA DOSIFICADORA 3", "OTRO"],
    "VAHOS": ["LAVADOR DE VAHOS 1","CICLON DE HIDROLISIS","TANQUE DE GASES DE HIDROLISIS","TANQUE DE DECANTACION","TANQUE DE COMPENSACION","TH HIDROLISIS","EXHAUSTOR DE VAHOS 1","LAVADOR DE VAHOS 2","ELECTROBOMBA","EXHAUSTOR DE VAHOS 2","BOMBA DE TORRE","VENTILADOR 1","VENTILADOR 2", "OTRO"],
    "CALDERO": ["QUEMADOR DE 900BHP","SISTEMA DE BOMBEO GRUNDFUS 900BHP","MANIFOLD","QUEMADOR DE 400BHP","SISTEMA DE BOMBEO GRUNDFUS 400BHP","TANQUE DE CONDENSADO 1 - 5M3","TANQUE DE CONDENSADO 2 - 9M3","ABLANDADOR AUTOMATICO","OSMOSIS 1","BOMBA DE OSMOSIS 1","OSMOSIS 2","BOMBA DE OSMOSIS 2","OSMOSIS 3","BOMBA DE OSMOSIS 3","BOMBA ALIMENTADOR 1","BOMBA ALIMENTADOR 2","BOMBA ALIMENTADOR PLATAFORMA","BOMBA DE AGUA DURA","BOMBA DE TANQUE DESAIRADOR", "OTRO"],
    "SUBESTACION": ["TFC-02","TFC-05","TD-13 TABLERO DE DISTRIBUCION SERVICIOS AUXILIARES","TD-14 TABLERO GENERAL  SUB ESTACION NUEVA","TD-15A TABLERO BARRAS DE DISTRIBUCION","TD-15B TABLERO BARRAS DE DISTRIBUCION #2","TD-16 TABLERO DE DISTRIBUCION DIGESTORES","TD-18A TABLERO GENERAL 220V","TD-18B TABLERO BARRAS DE DISTRIBUCION #2 220V","TC-34", "OTRO"]
}

# Actualizar antigüedad de registros existentes
def actualizar_antiguedades():
    registros = pd.read_sql('SELECT id, ingresado_el FROM registros', conn)
    for _, registro in registros.iterrows():
        if registro['ingresado_el']:
            antiguedad = calcular_antiguedad(registro['ingresado_el'])
            conn.execute('UPDATE registros SET antiguedad=? WHERE id=?', (antiguedad, registro['id']))
    conn.commit()

# Ejecutar actualización de antigüedades
actualizar_antiguedades()

# Sidebar
st.sidebar.header("⚙️ Configuración")

# Información del usuario en sidebar
st.sidebar.markdown("---")
st.sidebar.header(f"👤 {st.session_state.nombre_completo}")
st.sidebar.info(f"Rol: {st.session_state.rol}")

# Crear pestañas (ELIMINADA LA PESTAÑA DE AVISOS TRATADOS)
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📝 NUEVO AVISO", 
    "📋 AVISOS INGRESADOS", 
    "🛠️ AVISOS EN TRATAMIENTO",
    "📅 OT PROGRAMADAS",
    "🏁 OT CULMINADAS"
])

# PESTAÑA 1: NUEVO AVISO
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
            # No mostramos el campo en el formulario, pero usamos el valor de sesión
            ingresado_por = st.session_state.nombre_completo
            # Mostramos un mensaje informativo en lugar del campo de entrada
            st.info(f"👤 **Reportado por:** {ingresado_por}")
        
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
        
        st.subheader("📝 Descripción del Problema")
        descripcion_problema = st.text_area(
            "Descripción detallada del problema o falla *",
            placeholder="Describa el problema de manera detallada...",
            height=100
        )
        
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
                    imagen_nombre = None
                    imagen_datos = None
                    
                    if imagen_subida is not None:
                        imagen_nombre = imagen_subida.name
                        imagen_datos = imagen_subida.read()
                    
                    # EL CAMPO 'ingresado_por' SE TOMA AUTOMÁTICAMENTE DE st.session_state.nombre_completo
                    conn.execute('''
                        INSERT INTO registros (
                            codigo_padre, codigo_mantto, estado, antiguedad, area, equipo, 
                            descripcion_problema, ingresado_por, ingresado_el, hay_riesgo,
                            imagen_nombre, imagen_datos
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        codigo_padre, codigo_mantto, 'INGRESADO', 0, area_actual, equipo_actual,
                        descripcion_problema, st.session_state.nombre_completo, fecha_actual, hay_riesgo,
                        imagen_nombre, imagen_datos
                    ))
                    conn.commit()
                    
                    # SOLO ESTE MENSAJE DE FEEDBACK
                    st.success("✅ **Se registró correctamente**")
                    st.success(f"👤 **Reportado por:** {st.session_state.nombre_completo}")
                        
                except Exception as e:
                    st.error(f"❌ Error: {e}")
            else:
                st.warning("⚠️ Los campos marcados con * son obligatorios")

# PESTAÑA 2: AVISOS INGRESADOS
with tab2:
    st.header("📋 Avisos Ingresados - Pendientes de Tratamiento")
    
    df_ingresados = obtener_avisos_ingresados()
    
    if not df_ingresados.empty:
        # Mostrar estadísticas rápidas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Avisos", len(df_ingresados))
        with col2:
            st.metric("Con Riesgo", len(df_ingresados[df_ingresados['hay_riesgo'] == 'SI']))
        with col3:
            # Verificar si existe la columna imagen_nombre
            if 'imagen_nombre' in df_ingresados.columns:
                avisos_con_imagen = len(df_ingresados[df_ingresados['imagen_nombre'].notna()])
            else:
                avisos_con_imagen = 0
            st.metric("Con Imagen", avisos_con_imagen)
        
        # Mostrar tabla principal
        columnas_mostrar = ['codigo_padre', 'codigo_mantto', 'estado', 'antiguedad', 'area', 'equipo', 
                           'hay_riesgo', 'ingresado_por', 'ingresado_el']
        st.dataframe(df_ingresados[columnas_mostrar], use_container_width=True)
        
        # SECCIÓN PARA VER IMÁGENES
        st.subheader("🖼️ Visualizar Imágenes de Avisos")
        
        # Verificar si existe la columna y hay imágenes
        if 'imagen_nombre' in df_ingresados.columns:
            avisos_con_imagen = df_ingresados[df_ingresados['imagen_nombre'].notna()]
        else:
            avisos_con_imagen = pd.DataFrame()
        
        if not avisos_con_imagen.empty:
            codigo_seleccionado = st.selectbox(
                "Seleccione un aviso para ver la imagen:",
                avisos_con_imagen['codigo_padre'].tolist(),
                key="visor_imagen_ingresados"
            )
            
            if codigo_seleccionado:
                # Obtener los datos de la imagen
                resultado = conn.execute(
                    'SELECT imagen_nombre, imagen_datos FROM registros WHERE codigo_padre = ?', 
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

# PESTAÑA 3: AVISOS EN TRATAMIENTO
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
            if 'imagen_nombre' in df_ingresados.columns:
                avisos_con_imagen = len(df_ingresados[df_ingresados['imagen_nombre'].notna()])
            else:
                avisos_con_imagen = 0
            st.metric("Con Imagen", avisos_con_imagen)
        
        # Mostrar tabla principal de avisos INGRESADOS
        st.subheader("📋 Avisos Ingresados para Tratar")
        columnas_mostrar = ['codigo_padre', 'codigo_mantto', 'antiguedad', 'area', 'equipo', 
                           'hay_riesgo', 'ingresado_por', 'ingresado_el']
        st.dataframe(df_ingresados[columnas_mostrar], use_container_width=True)
        
        # FORMULARIO DE TRATAMIENTO PARA AVISOS INGRESADOS
        st.subheader("✏️ Tratar Aviso - Agregar Información de Mantenimiento")
        
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
                f'SELECT * FROM registros WHERE codigo_padre = "{codigo_tratar}"', 
                conn
            )
            
            if not registro_actual_df.empty:
                registro_actual = registro_actual_df.iloc[0]
                
                # FORMULARIO 1: SOLO LECTURA (Datos de la pestaña 1)
                st.markdown("---")
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
                    resultado = conn.execute(
                        'SELECT imagen_nombre, imagen_datos FROM registros WHERE codigo_padre = ?', 
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
                
                # FORMULARIO 2: EDITABLE (Datos de mantenimiento)
                st.markdown("---")
                st.subheader("🛠️ Formulario 2: Información de Mantenimiento (Editable)")
                
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
                            ["JHONAR VASQUEZ", "DIEGO QUISPE"],
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
                    
                    # Botones de acción en el formulario editable
                    st.markdown("---")
                    col5, col6, col7 = st.columns([2, 1, 1])
                    
                    with col5:
                        st.info("💡 Complete todos los campos obligatorios (*) antes de iniciar el tratamiento")
                    
                    with col6:
                        iniciar_tratamiento = st.form_submit_button("🚀 Iniciar Tratamiento", 
                                                                  type="primary", 
                                                                  use_container_width=True)
                    
                    with col7:
                        rechazar_aviso = st.form_submit_button("❌ Rechazar Aviso", 
                                                              use_container_width=True)
                    
                    if iniciar_tratamiento:
                        if (clasificacion and sistema and codigo_equipo and prioridad and 
                            responsable and alimentador_proveedor and descripcion_trabajo and 
                            materiales and duracion_estimada):
                            
                            # GENERAR CÓDIGO OT AUTOMÁTICAMENTE
                            codigo_ot = generar_codigo_ot()
                            
                            # Actualizar la base de datos con toda la información INCLUYENDO EL CÓDIGO OT
                            conn.execute('''
                                UPDATE registros SET 
                                    clasificacion=?, descripcion_trabajo=?, sistema=?, codigo_equipo=?,
                                    prioridad_nueva=?, responsable=?, alimentador_proveedor=?, materiales=?,
                                    cantidad_mecanicos=?, cantidad_electricos=?, cantidad_soldadores=?,
                                    cantidad_op_vahos=?, cantidad_calderistas=?, fecha_estimada_inicio=?,
                                    duracion_estimada=?, estado='EN PROCESO', codigo_ot=?
                                WHERE codigo_padre=?
                            ''', (
                                clasificacion, descripcion_trabajo, sistema, codigo_equipo,
                                prioridad, responsable, alimentador_proveedor, materiales,
                                cantidad_mecanicos, cantidad_electricos, cantidad_soldadores,
                                cantidad_op_vahos, cantidad_calderistas, fecha_estimada_inicio,
                                duracion_estimada, codigo_ot, codigo_tratar
                            ))
                            conn.commit()
                            st.success(f"✅ **Aviso puesto en tratamiento correctamente**")
                            st.success(f"📋 **Código OT asignado:** {codigo_ot}")
                            st.balloons()
                            
                            # Forzar recarga de la página
                            st.rerun()
                        else:
                            st.error("❌ **Complete todos los campos obligatorios marcados con ***")
                    
                    if rechazar_aviso:
                        conn.execute(
                            'UPDATE registros SET estado="RECHAZADO" WHERE codigo_padre=?',
                            (codigo_tratar,)
                        )
                        conn.commit()
                        st.success("✅ **Aviso rechazado correctamente**")
                        
                        # Forzar recarga de la página
                        st.rerun()
                        
            else:
                st.error("❌ No se pudo encontrar el aviso seleccionado en la base de datos")
        
    else:
        st.info("🎉 No hay avisos pendientes de tratamiento")

# PESTAÑA 4: AVISOS EN PROCESO
with tab4:
    st.header("📊 Avisos en Proceso")
    
    # Obtener avisos en estado EN PROCESO
    df_en_proceso = obtener_avisos_en_proceso()
    
    if not df_en_proceso.empty:
        # Mostrar estadísticas rápidas
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total en Proceso", len(df_en_proceso))
        with col2:
            avisos_alta = len(df_en_proceso[df_en_proceso['prioridad_nueva'] == '1. ALTO'])
            st.metric("Prioridad ALTA", avisos_alta)
        with col3:
            avisos_medio = len(df_en_proceso[df_en_proceso['prioridad_nueva'] == '2. MEDIO'])
            st.metric("Prioridad MEDIA", avisos_medio)
        with col4:
            avisos_baja = len(df_en_proceso[df_en_proceso['prioridad_nueva'] == '3. BAJO'])
            st.metric("Prioridad BAJA", avisos_baja)
        
        # Mostrar tabla con solo las columnas especificadas
        st.subheader("📋 Lista de Avisos en Proceso")
        
        # Seleccionar solo las columnas requeridas
        columnas_mostrar = [
            'codigo_ot',
            'codigo_padre', 
            'descripcion_trabajo',
            'prioridad_nueva',
            'area',
            'equipo',
            'cantidad_mecanicos',
            'cantidad_electricos', 
            'cantidad_soldadores',
            'cantidad_op_vahos',
            'cantidad_calderistas',
            'materiales',
            'fecha_estimada_inicio',
            'duracion_estimada'
        ]
        
        # Renombrar las columnas para mejor presentación
        df_mostrar = df_en_proceso[columnas_mostrar].rename(columns={
            'codigo_ot': 'Código OT',
            'codigo_padre': 'Código Padre',
            'descripcion_trabajo': 'Trabajo a Realizar',
            'prioridad_nueva': 'Prioridad',
            'area': 'Área',
            'equipo': 'Equipo',
            'cantidad_mecanicos': 'Mecánicos',
            'cantidad_electricos': 'Eléctricos',
            'cantidad_soldadores': 'Soldadores',
            'cantidad_op_vahos': 'Op. Vahos',
            'cantidad_calderistas': 'Calderistas',
            'materiales': 'Materiales',
            'fecha_estimada_inicio': 'Fecha Estimada Inicio',
            'duracion_estimada': 'Duración Estimada'
        })
        
        # Mostrar la tabla
        st.dataframe(df_mostrar, use_container_width=True)
        
        # Opcional: Agregar filtros si lo deseas
        st.subheader("🔍 Filtros")
        col_f1, col_f2, col_f3 = st.columns(3)
        
        with col_f1:
            filtro_prioridad = st.selectbox(
                "Filtrar por Prioridad:",
                ["TODAS", "1. ALTO", "2. MEDIO", "3. BAJO"]
            )
        
        with col_f2:
            areas_unicas = ["TODAS"] + sorted(df_en_proceso['area'].unique().tolist())
            filtro_area = st.selectbox(
                "Filtrar por Área:",
                areas_unicas
            )
        
        with col_f3:
            # Aplicar filtros
            df_filtrado = df_en_proceso.copy()
            
            if filtro_prioridad != "TODAS":
                df_filtrado = df_filtrado[df_filtrado['prioridad_nueva'] == filtro_prioridad]
            
            if filtro_area != "TODAS":
                df_filtrado = df_filtrado[df_filtrado['area'] == filtro_area]
            
            st.metric("Avisos Filtrados", len(df_filtrado))
        
        # Mostrar tabla filtrada si se aplicaron filtros
        if filtro_prioridad != "TODAS" or filtro_area != "TODAS":
            st.subheader("📋 Avisos Filtrados")
            df_filtrado_mostrar = df_filtrado[columnas_mostrar].rename(columns={
                'codigo_ot': 'Código OT',
                'codigo_padre': 'Código Padre',
                'descripcion_trabajo': 'Trabajo a Realizar',
                'prioridad_nueva': 'Prioridad',
                'area': 'Área',
                'equipo': 'Equipo',
                'cantidad_mecanicos': 'Mecánicos',
                'cantidad_electricos': 'Eléctricos',
                'cantidad_soldadores': 'Soldadores',
                'cantidad_op_vahos': 'Op. Vahos',
                'cantidad_calderistas': 'Calderistas',
                'materiales': 'Materiales',
                'fecha_estimada_inicio': 'Fecha Estimada Inicio',
                'duracion_estimada': 'Duración Estimada'
            })
            st.dataframe(df_filtrado_mostrar, use_container_width=True)
        
    else:
        st.info("📭 No hay avisos en proceso actualmente")
        st.write("Los avisos aparecerán aquí una vez que sean puestos en tratamiento desde la pestaña 'Avisos en Tratamiento'")

# PESTAÑA 5: OT CULMINADAS
with tab5:
    st.header("🏁 Órdenes de Trabajo Culminadas")
    
    df_culminadas = obtener_ot_culminadas()
    
    if not df_culminadas.empty:
        # Mostrar estadísticas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Culminadas", len(df_culminadas))
        with col2:
            st.metric("Este Mes", len(df_culminadas[df_culminadas['ingresado_el'] >= date.today().replace(day=1).strftime('%Y-%m-%d')]))
        with col3:
            avg_antiguedad = df_culminadas['antiguedad'].mean()
            st.metric("Antigüedad Promedio", f"{avg_antiguedad:.1f} días")
        
        # Mostrar tabla (manejar caso donde no existe codigo_ot)
        columnas_base = ['codigo_padre', 'codigo_mantto', 'estado', 'antiguedad', 'area', 
                       'equipo', 'responsable', 'ingresado_por', 'ingresado_el']
        
        # Agregar codigo_ot solo si existe
        if 'codigo_ot' in df_culminadas.columns:
            columnas_base.insert(0, 'codigo_ot')
        
        columnas_mostrar = [col for col in columnas_base if col in df_culminadas.columns]
        st.dataframe(df_culminadas[columnas_mostrar], use_container_width=True)
        
    else:
        st.info("🆕 No hay órdenes de trabajo culminadas aún.")

# Funciones de mantenimiento en sidebar
st.sidebar.markdown("---")
st.sidebar.header("🧹 Mantenimiento")

if st.sidebar.button("🔄 Actualizar Antigüedades"):
    actualizar_antiguedades()
    st.sidebar.success("✅ Antigüedades actualizadas")
    st.sidebar.info("💡 Presiona F5 para ver los cambios")

# Estadísticas generales en sidebar
st.sidebar.markdown("---")
st.sidebar.header("📊 Resumen General")

df_todas = obtener_todos_registros()
if not df_todas.empty:
    st.sidebar.metric("Total Registros", len(df_todas))
    st.sidebar.metric("Avisos Ingresados", len(obtener_avisos_ingresados()))
    st.sidebar.metric("OT en Proceso", len(obtener_avisos_en_proceso()))
    st.sidebar.metric("OT Culminadas", len(obtener_ot_culminadas()))
else:
    st.sidebar.info("No hay datos aún")

st.sidebar.markdown("---")
st.sidebar.header("🔄 Actualizar")
st.sidebar.info("Presiona **F5** para recargar la página y ver los últimos cambios")

st.sidebar.markdown("---")
st.sidebar.header("🌐 Acceso MultiUsuario")
st.sidebar.code("http://10.0.1.118:8501")

# Cerrar conexión
conn.close()