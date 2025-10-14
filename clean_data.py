import pandas as pd
import numpy as np
from collections import OrderedDict
from collections import defaultdict
import re
from datetime import datetime
import random
from geopy.geocoders import Nominatim
from ast import literal_eval
import time
import csv


# Función para normalizar texto cambiando solo vocales con tilde, manteniendo la "ñ"
def normalizar_CampoTXT(campo):
    if isinstance(campo, str):
        # Convertir a minúsculas
        campo = campo.lower()

        if ',' in campo:
            campo = campo.replace(", ", ",")  # Eliminar espacio posterior a una coma
            campo = campo.replace(" ,", ",")  # Eliminar espacio anterior a una coma
        campo = re.sub(r'[:·]$', '', campo)
        if '-' in campo:
            campo = campo.replace(" - ", "-")  # Eliminar espacios si contiene guión
        if ':' in campo:
            campo = campo.replace(": ", ":")
        if '/' in campo:
            campo = campo.replace("/ ", "/")
        campo = campo.replace(" ", "_")
        #campo = campo.replace("·", "-")
        campo = re.sub(r'\s*(\d+)\s*', r'\1', campo)   
            
        # Diccionario de reemplazo para vocales con tilde
        reemplazos = {
            'á': 'a',
            'à': 'a',
            'é': 'e',
            'è': 'e',
            'í': 'i',
            'ì': 'i',
            'ó': 'o',
            'ò': 'o',
            'ú': 'u',
            'ù': 'u',
            'Á': 'a',
            'É': 'e',
            'Í': 'i',
            'Ó': 'o',
            'Ú': 'u'
        }

        # Reemplazar las vocales acentuadas por sus versiones sin acento
        campo = ''.join(reemplazos.get(c, c) for c in campo)

        if campo.startswith('_'):
            campo = campo[1:]

        campo = campo.replace("__", '_')
        campo = campo.replace("000", '')
        campo = campo.replace("_·_", '_')

        # Poner como mayúscula la primera letra
        campo = campo.capitalize()
    else:
        campo = ""  # Devolver cadena vacía si no es texto

    return campo


def normalizar_direccion(direccion):
    if isinstance(direccion, str):  # Verificar si el valor es string antes de intentar el reemplazo
        direccion= re.sub(r'(Av ·|Avda ·|C ·|Pza ·|Pq ·|Parque ·|Pº ·|V · |Jar ·|Ctra ·|Trv ·|Cm ·|Cmno ·|Ba ·|Calle ·)', '', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^(AVENIDA|AVDA|AV|AV.)', 'AVENIDA ', direccion)
        direccion = re.sub(r'^(CALLE|Cmno_|CMNO|Cv_|C_-|C\.|c\.|Cl\.|C/|c/|CL.|CV)', 'CALLE ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^(PZ)', 'PLAZA ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^(AUTOV)', 'AUTOVIA ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'(^VÍA)', 'VIA ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^BVAR', 'BULEVAR ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^(CTRA)', 'CARRETERA ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^PJE', 'PASAJE ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^RDA', 'RONDA ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^CV', 'CON VUELTA ', direccion, flags=re.IGNORECASE)
        direccion = re.sub(r'^TSÍA', 'TRAVESIA ', direccion, flags=re.IGNORECASE)
        return direccion


def estandarizar_tlf(tlf):
    # Verificar si el valor es string antes de intentar el parseo
    if isinstance(tlf, str):
        # Eliminar todos los espacios y caracteres no numéricos
        tlf = re.sub(r'\D', '', tlf)
        # Asegurarse de que el teléfono tiene 9 dígitos después del prefijo
        if tlf.startswith('34'):
            tlf = tlf[2:]
        elif tlf.startswith('0034'):
            tlf = tlf[4:]
        elif tlf.startswith('+34'):
            tlf = tlf[3:]
        if len(tlf) == 9:
            tlf = '+34 ' + ' '.join([tlf[i:i+3] for i in range(0, len(tlf), 3)])
            return tlf
    # Rellenar el valor original si el valor no es un string o no tiene el formato correcto
    return tlf


def quitar_comillas_simples(campo):
    return campo.replace("'", "")


# Función para tratar valores faltantes en los campos DISTRITO y COD_DISTRITO
def faltantes_distritos(dic_distritos, dic_barrios, archivo_areas, archivo_juegos):
    # Concatenar ambos archivos para calcular las modas de manera conjunta
    archivo_combinado = pd.concat([archivo_areas, archivo_juegos], ignore_index=True)

    # Llenar valores faltantes en COD_DISTRITO usando la moda por BARRIO
    for barrio in dic_barrios.values():  # Usar los valores del diccionario de barrios
        # Obtener la moda para COD_DISTRITO de ese BARRIO
        moda_cod_distrito = archivo_combinado[archivo_combinado['BARRIO'] == barrio]['COD_DISTRITO'].mode()
        
        if not moda_cod_distrito.empty:
            # Reemplazar valores faltantes en COD_DISTRITO en archivo_areas
            mask_areas = (archivo_areas['BARRIO'] == barrio) & (archivo_areas['COD_DISTRITO'].isna())
            archivo_areas.loc[mask_areas, 'COD_DISTRITO'] = moda_cod_distrito[0]

            # Reemplazar valores faltantes en COD_DISTRITO en archivo_juegos
            mask_juegos = (archivo_juegos['BARRIO'] == barrio) & (archivo_juegos['COD_DISTRITO'].isna())
            archivo_juegos.loc[mask_juegos, 'COD_DISTRITO'] = moda_cod_distrito[0]

    # Actualizar DISTRITO en base al COD_DISTRITO
    archivo_areas['DISTRITO'] = archivo_areas['COD_DISTRITO'].map(dic_distritos).fillna("NA")
    archivo_juegos['DISTRITO'] = archivo_juegos['COD_DISTRITO'].map(dic_distritos).fillna("NA")


# Función para estandarizar barrios y distritos
def estandarizar_BarriosDistritos(archivo_areas, archivo_juegos):
    print("estandarizar_BarriosDistritos")
    # Combinar ambos DataFrames y eliminar duplicados en claves
    all_barrios = pd.concat([archivo_areas, archivo_juegos], ignore_index=True).drop_duplicates(subset=["COD_BARRIO", "BARRIO"]).dropna(subset=["COD_BARRIO"])
    all_distritos = pd.concat([archivo_areas, archivo_juegos], ignore_index=True).drop_duplicates(subset=["COD_DISTRITO", "DISTRITO"]).dropna(subset=["COD_DISTRITO"])

    # Crear diccionarios de mapeo asegurando valores únicos
    diccionario_barrios = OrderedDict(
        sorted(
            {
                row["COD_BARRIO"]: normalizar_CampoTXT(row["BARRIO"])
                for _, row in all_barrios[["COD_BARRIO", "BARRIO"]].drop_duplicates(subset=["COD_BARRIO"]).iterrows()
            }.items()
        )
    )
    diccionario_distritos = OrderedDict(
        sorted(
            {
                row["COD_DISTRITO"]: normalizar_CampoTXT(row["DISTRITO"])
                for _, row in all_distritos[["COD_DISTRITO", "DISTRITO"]].drop_duplicates(subset=["COD_DISTRITO"]).iterrows()
            }.items()
        )
    )

    # Normalizar y reemplazar valores en el campo BARRIO usando el diccionario
    archivo_areas["BARRIO"] = archivo_areas["COD_BARRIO"].map(diccionario_barrios).fillna("NA")
    archivo_juegos["BARRIO"] = archivo_juegos["COD_BARRIO"].map(diccionario_barrios).fillna("NA")

    # Completar los valores faltantes de COD_DISTRITO y DISTRITO en ambos archivos
    faltantes_distritos(diccionario_distritos, diccionario_barrios, archivo_areas, archivo_juegos)


# Función para estandarizar los datos de los usuarios
def estandarizar_Usuarios(archivo_usuarios):
    print("estandarizar_Usuarios")
    # Normalizar nombres acorde a nuestro estándar para datos de texto
    if "NOMBRE" in archivo_usuarios.columns:
        archivo_usuarios["NOMBRE"] = archivo_usuarios["NOMBRE"].apply(normalizar_CampoTXT)

    # Eliminar solo espacios en el campo "TELEFONO"
    if "TELEFONO" in archivo_usuarios.columns:
        archivo_usuarios["TELEFONO"] = archivo_usuarios["TELEFONO"].apply(estandarizar_tlf)

    # Eliminar la columna "Email" si existe
    if "Email" in archivo_usuarios.columns:
        archivo_usuarios.drop(columns=["Email"], inplace=True)  # Asegura la eliminación en el DataFrame original

    # Retornar el DataFrame modificado
    return archivo_usuarios


def texto_areas(archivo_areas):
    if "DESC_CLASIFICACION" in archivo_areas.columns:
        archivo_areas["DESC_CLASIFICACION"] = archivo_areas["DESC_CLASIFICACION"].apply(normalizar_CampoTXT)

    if "ESTADO" in archivo_areas.columns:
        archivo_areas["ESTADO"] = archivo_areas["ESTADO"].apply(normalizar_CampoTXT)

    if "TIPO_VIA" in archivo_areas.columns:
        archivo_areas["TIPO_VIA"] = archivo_areas["TIPO_VIA"].apply(normalizar_CampoTXT)
    
    if "NOM_VIA" in archivo_areas.columns:
        archivo_areas["NOM_VIA"] = archivo_areas["NOM_VIA"].apply(normalizar_CampoTXT)

    if "DIRECCION_AUX" in archivo_areas.columns:
        archivo_areas["DIRECCION_AUX"] = archivo_areas["DIRECCION_AUX"].apply(normalizar_direccion)
        archivo_areas["DIRECCION_AUX"] = archivo_areas["DIRECCION_AUX"].apply(normalizar_CampoTXT)

    if "tipo" in archivo_areas.columns:
        archivo_areas["tipo"] = archivo_areas["tipo"].apply(normalizar_CampoTXT)


def texto_juegos(archivo_juegos):
    if "DESC_CLASIFICACION" in archivo_juegos.columns:
        archivo_juegos["DESC_CLASIFICACION"] = archivo_juegos["DESC_CLASIFICACION"].apply(normalizar_CampoTXT)
    
    if "ESTADO" in archivo_juegos.columns:
        archivo_juegos["ESTADO"] = archivo_juegos["ESTADO"].apply(normalizar_CampoTXT)

    if "TIPO_VIA" in archivo_juegos.columns:
        archivo_juegos["TIPO_VIA"] = archivo_juegos["TIPO_VIA"].apply(normalizar_CampoTXT)
    
    if "NOM_VIA" in archivo_juegos.columns:
        archivo_juegos["NOM_VIA"] = archivo_juegos["NOM_VIA"].apply(normalizar_CampoTXT)

    if "DIRECCION_AUX" in archivo_juegos.columns:
        archivo_juegos["DIRECCION_AUX"] = archivo_juegos["DIRECCION_AUX"].apply(normalizar_direccion)
        archivo_juegos["DIRECCION_AUX"] = archivo_juegos["DIRECCION_AUX"].apply(normalizar_CampoTXT)
    
    if "tipo_juego" in archivo_juegos.columns:
        archivo_juegos["tipo_juego"] = archivo_juegos["tipo_juego"].apply(normalizar_CampoTXT)


def texto_incidencias(archivo_incidencias):
    if "TIPO_INCIDENCIA" in archivo_incidencias.columns:
        archivo_incidencias["TIPO_INCIDENCIA"] = archivo_incidencias["TIPO_INCIDENCIA"].apply(normalizar_CampoTXT)
    if "UsuarioID" in archivo_incidencias.columns:
        archivo_incidencias["UsuarioID"] = archivo_incidencias["UsuarioID"].apply(quitar_comillas_simples)


def texto_incidentes(archivo_incidentes):
    if "TIPO_INCIDENTE" in archivo_incidentes.columns:
        archivo_incidentes["TIPO_INCIDENTE"] = archivo_incidentes["TIPO_INCIDENTE"].apply(normalizar_CampoTXT)
    
    if "GRAVEDAD" in archivo_incidentes.columns:
        archivo_incidentes["GRAVEDAD"] = archivo_incidentes["GRAVEDAD"].apply(normalizar_CampoTXT)


def texto_mantenimiento(archivo_mantenimiento):
    if "Tipo" in archivo_mantenimiento.columns:
        archivo_mantenimiento["Tipo"] = archivo_mantenimiento["Tipo"].apply(normalizar_CampoTXT)

    if "Comentarios" in archivo_mantenimiento.columns:
        archivo_mantenimiento["Comentarios"] = archivo_mantenimiento["Comentarios"].apply(normalizar_CampoTXT)


def texto_estaciones(archivo_estaciones):
    if "DIRECCION" in archivo_estaciones.columns:
        archivo_estaciones["DIRECCION"] = archivo_estaciones["DIRECCION"].apply(normalizar_direccion)
        archivo_estaciones["DIRECCION"] = archivo_estaciones["DIRECCION"].apply(normalizar_CampoTXT)


def texto_encuestas(archivo_encuestas):
    if "COMENTARIOS" in archivo_encuestas.columns:
        archivo_encuestas["COMENTARIOS"] = archivo_encuestas["COMENTARIOS"].apply(normalizar_CampoTXT)


# Función para estandarizar campos de texto en los archivos
def estandarizar_Textos(archivo_areas, archivo_juegos, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_estaciones, archivo_encuestas):
    print("estandarizar_Textos")
    texto_areas(archivo_areas)
    texto_juegos(archivo_juegos)
    texto_incidencias(archivo_incidencias)
    texto_incidentes(archivo_incidentes)
    texto_mantenimiento(archivo_mantenimiento)
    texto_estaciones(archivo_estaciones)
    texto_encuestas(archivo_encuestas)


def convert_to_iso(date_str):
    formatos_posibles = [
        "%d/%m/%Y",
        "%d/%m/%y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S"
    ]
    for formato in formatos_posibles:
        try:
            return datetime.strptime(date_str, formato).isoformat() + "Z"
        except ValueError:
            continue
    return None


def fecha_incorrecta_o_vacia(archivo, row, fecha, direccion, df_juegos):
    date_str = row[fecha]
    if date_str == "fecha_incorrecta" or pd.isna(date_str):
        #No hay manera de calcular la fecha de instalación de un juego
        if "CONTRATO_COD" in archivo:
            if pd.isna(row['FECHA_INSTALACION']):
                return f"{row.ID}-FECHA_INSTALACION-ausente"
            else:
                return f"{row.ID}-FECHA_INSTALACION-incorrecta"
        if direccion and df_juegos is not None:
            direccion_aux = row[direccion]
            matching_rows = df_juegos[(df_juegos['DIRECCION_AUX'] == direccion_aux)]
            if matching_rows.empty:
                return f"{row.ID}-FECHA_INSTALACION-desconocida"
            iso_dates = matching_rows['FECHA_INSTALACION'].dropna()
            iso_dates = iso_dates[~iso_dates.str.contains(r'\bFECHA_INSTALACION-incorrecta\b', regex=True)]
            if iso_dates.empty:
                return f"{row.ID}-FECHA_INSTALACION-desconocida"
            oldest_date = iso_dates.min()
            return oldest_date
        return None
    return convert_to_iso(date_str)


def rellenar_fechas(archivo_areas, archivo_juegos, archivo_encuestas, archivo_incidencias, archivo_incidentes, archivo_mantenimiento):
    print("rellenar_fechas")
    archivos = [
        (archivo_areas, "FECHA_INSTALACION", "DIRECCION_AUX"),
        (archivo_juegos, "FECHA_INSTALACION", None),
        (archivo_encuestas, "FECHA", None),
        (archivo_incidencias, "FECHA_REPORTE", None),
        (archivo_incidentes, "FECHA_REPORTE", None),
        (archivo_mantenimiento, "FECHA_INTERVENCION", None)
    ]
    for archivo, fecha, direccion in archivos:
        if "Tipo_juego" in archivo:
            df_juegos = pd.read_csv('Datasets/JuegosLimpio.csv')
            archivo[fecha] = archivo.apply(lambda row: fecha_incorrecta_o_vacia(archivo, row, fecha, direccion, df_juegos), axis=1)
        else:
            archivo[fecha] = archivo.apply(lambda row: fecha_incorrecta_o_vacia(archivo, row, fecha, direccion, None), axis=1)
    
    #return archivo_areas, archivo_juegos, archivo_encuestas, archivo_incidencias, archivo_incidentes, archivo_mantenimiento


def extraer_mantenimiento_id_incidencias(array):
    numeros = [int(re.search(r'\d+', item).group()) for item in array if re.search(r'\d+', item)]
    return numeros if numeros else None


def extraer_id_mantenimiento(cadena):
    cadena = re.sub(r'[^\d\-,]', '', cadena).replace(',', '.')
    try:
        numero = abs(int(float(cadena)))
        return numero
    except ValueError:
        return None


def estandarizar_mantenimiento_id_incidencias(df, columna):
    for i in range(len(df)):
        array = df.at[i, columna]        
        if isinstance(array, str):
            array = eval(array)        
        df.at[i, columna] = extraer_mantenimiento_id_incidencias(array)
    return df


def estandarizar_id_mantenimiento(df, columna):
    for i in range(len(df)):
        valor = df.at[i, columna]
        if isinstance(valor, str):
            df.at[i, columna] = extraer_id_mantenimiento(valor)
    return df


def estandarizar_unidades(archivo_incidencias, archivo_mantenimiento):
    print("estandarizar_unidades")
    incidencias_nuevo = archivo_incidencias
    mantenimiento_nuevo = archivo_mantenimiento
    estandarizar_mantenimiento_id_incidencias(incidencias_nuevo, 'MantenimeintoID')
    estandarizar_id_mantenimiento(mantenimiento_nuevo, 'ID')
    archivo_incidencias = incidencias_nuevo
    archivo_mantenimiento = mantenimiento_nuevo


def asignar_exposicion(archivo_juegos):
    print("asignar_exposicion")
    archivo_juegos['INDICADOR_EXPOSICION'] = archivo_juegos.apply(lambda row: random.choice(['Alta', 'Media', 'Baja']), axis=1)


def obtener_codigo_postal(coordenadas, id_value, geolocator):
    try:
        location = geolocator.reverse((coordenadas[1], coordenadas[0]), exactly_one=True)
        address = location.raw.get('address', {})
        
        time.sleep(1)
        
        codigo_postal = address.get('postcode', None)
        
        if codigo_postal is None:
            return f"{id_value}_COD_POSTAL_no_disponible"
        
        return codigo_postal
    
    except Exception as e:
        return f"{id_value}_COD_POSTAL_no_calculable"


def calcular_postal(path_areas, path_juegos):
    archivo_areas = pd.read_csv(path_areas)
    archivo_juegos = pd.read_csv(path_juegos)

    geolocator = Nominatim(user_agent="geoapi")

    archivo_areas['COD_POSTAL'] = archivo_areas['COD_POSTAL'].astype('object')
    archivo_juegos['COD_POSTAL'] = archivo_juegos['COD_POSTAL'].astype('object')

    for index, row in archivo_areas.iterrows():
        if pd.isna(row['COD_POSTAL']) or row['COD_POSTAL'] == '0':
            coordenadas = literal_eval(row['COORDENADAS-WGS84'])
            nuevo_cod_postal = obtener_codigo_postal(coordenadas, row['ID'], geolocator)
            archivo_areas.at[index, 'COD_POSTAL'] = nuevo_cod_postal
    
    for index, row in archivo_juegos.iterrows():
        if pd.isna(row['COD_POSTAL']) or row['COD_POSTAL'] == '0':
            coordenadas = literal_eval(row['COORDENADAS-WGS84'])
            nuevo_cod_postal = obtener_codigo_postal(coordenadas, row['ID'], geolocator)
            archivo_juegos.at[index, 'COD_POSTAL'] = nuevo_cod_postal
    
    archivo_areas.to_csv("Datasets/AreasLimpio.csv", index=False)
    archivo_juegos.to_csv("Datasets/JuegosLimpio.csv", index=False)


def adaptar_coordenadas(archivo_areas, archivo_juegos):
    archivo_areas = archivo_areas.copy()
    archivo_juegos = archivo_juegos.copy()

    #Creamos la columna COORDENADAS-ETRS89 en base a los atributos COORD_GIS_X y COORD_GIS_Y en Areas y Juegos
    archivo_areas['COORDENADAS-ETRS89'] = archivo_areas.apply(lambda row: f"{row['ID']}_COORDENADAS-ETRS89_desconocidas" if pd.isna(row['COORD_GIS_X']) or pd.isna(row['COORD_GIS_Y']) else [row['COORD_GIS_X'], row['COORD_GIS_Y']],axis=1)
    archivo_juegos['COORDENADAS-ETRS89'] = archivo_juegos.apply(lambda row: f"{row['ID']}_COORDENADAS-ETRS89_desconocidas" if pd.isna(row['COORD_GIS_X']) or pd.isna(row['COORD_GIS_Y']) else [row['COORD_GIS_X'], row['COORD_GIS_Y']],axis=1)

    #Areas
    columnas_areas = archivo_areas.columns.tolist()
    indice_longitud = columnas_areas.index('COORD_GIS_Y')
    columnas_areas.insert(indice_longitud + 1, columnas_areas.pop(columnas_areas.index('COORDENADAS-ETRS89')))
    archivo_areas = archivo_areas[columnas_areas]

    #Juegos
    columnas_juegos = archivo_juegos.columns.tolist()
    indice_longitud = columnas_juegos.index('COORD_GIS_Y')
    columnas_juegos.insert(indice_longitud + 1, columnas_juegos.pop(columnas_juegos.index('COORDENADAS-ETRS89')))
    archivo_juegos = archivo_juegos[columnas_juegos]

    #Eliminamos las columnas COORD_GIS_X, COORD_GIS_Y y SISTEMA_COORD
    if ("COORD_GIS_X" and "COORD_GIS_Y" and "SISTEMA_COORD") in archivo_areas:
        archivo_areas.drop(columns=['COORD_GIS_X', 'COORD_GIS_Y', 'SISTEMA_COORD'], inplace=True)

    if ("COORD_GIS_X" and "COORD_GIS_Y" and "SISTEMA_COORD") in archivo_juegos:  
        archivo_juegos.drop(columns=['COORD_GIS_X', 'COORD_GIS_Y', 'SISTEMA_COORD'], inplace=True)
    
    #Creamos la columna COORDENADAS-WGS84 en base a los atributos LATITUD y LONGITUD
    archivo_areas['COORDENADAS-WGS84'] = 0
    archivo_areas['COORDENADAS-WGS84'] = archivo_areas.apply(lambda row: f"{row['ID']}_COORDENADAS-WGS84_desconocidas" if (row['LATITUD'] == 0) or (row['LONGITUD'] == 0) else [row['LONGITUD'], row['LATITUD']],axis=1)
    archivo_juegos['COORDENADAS-WGS84'] = 0
    archivo_juegos['COORDENADAS-WGS84'] = archivo_juegos.apply(lambda row: f"{row['ID']}_COORDENADAS-WGS84_desconocidas" if (row['LATITUD'] == 0) or (row['LONGITUD'] == 0) else [row['LONGITUD'], row['LATITUD']],axis=1)

    #Areas
    columnas_areas = archivo_areas.columns.tolist()
    indice_longitud = columnas_areas.index('LONGITUD')
    columnas_areas.insert(indice_longitud + 1, columnas_areas.pop(columnas_areas.index('COORDENADAS-WGS84')))
    archivo_areas = archivo_areas[columnas_areas]

    #Juegos
    columnas_juegos = archivo_juegos.columns.tolist()
    indice_longitud = columnas_juegos.index('LONGITUD')
    columnas_juegos.insert(indice_longitud + 1, columnas_juegos.pop(columnas_juegos.index('COORDENADAS-WGS84')))
    archivo_juegos = archivo_juegos[columnas_juegos]

    #Eliminamos las columnas LATITUD y LONGITUD en Areas y Juegos
    archivo_areas.drop(['LATITUD', 'LONGITUD'], axis=1, inplace=True)
    archivo_juegos.drop(['LATITUD', 'LONGITUD'], axis=1, inplace=True)

    archivo_areas.to_csv("Datasets/AreasLimpio.csv", index=False)
    archivo_juegos.to_csv("Datasets/JuegosLimpio.csv", index=False)


def campos_relacionados_con_direccion_auxiliar_areas(row):
    #Si TIPO_VIA, NOM_VIA, NUM_VIA y DIRECCION_AUX están vacíos
    if pd.isna(row['TIPO_VIA']) and pd.isna(row['NOM_VIA']) and pd.isna(row['NUM_VIA']) and pd.isna(row['DIRECCION_AUX']):
        row['DIRECCION_AUX'] = f"{row['ID']}_DIRECCION-AUX_desconocida"
        row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
        row['NOM_VIA'] = f"{row['ID']}_NOM-VIA_desconocido"
        row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
    #Si TIPO_VIA, NOM_VIA, NUM_VIA o DIRECCION_AUX no están vacíos
    else:
        #Si TIPO_VIA no está vacío
        if pd.notna(row['TIPO_VIA']):
            #Si NOM_VIA no está vacío
            if pd.notna(row['NOM_VIA']):
                #Si NUM_VIA no está vacío
                if pd.notna(row['NUM_VIA']):
                    #Formato NUM_VIA
                    num_via_str = str(row['NUM_VIA']).strip()
                    if re.match(r'^\d+$', num_via_str):
                        row['NUM_VIA'] = num_via_str.zfill(3)
                    else:
                        row['NUM_VIA'] = re.sub(r'\s+', '', num_via_str)
                    #!!!Condición Avenida!!!
                    if (row['TIPO_VIA'] == "Avenida" and re.search(r'Avenida[^_]*_', row['NOM_VIA'])):
                        row['DIRECCION_AUX'] = f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}"
                    else:
                        row['DIRECCION_AUX'] = f"{row['TIPO_VIA']} {row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}"
                #Si NUM_VIA está vacío
                else:
                    #Si DIRECCION_AUX está vacío
                    row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
                    row['DIRECCION_AUX'] = f"{row['TIPO_VIA']} {row['NOM_VIA']}"
        
            #Si NOM_VIA está vacío
            else:
                #Si NUM_VIA no está vacío
                if pd.notna(row['NUM_VIA']):
                    #Formato NUM_VIA
                    num_via_str = str(row['NUM_VIA']).strip()
                    if re.match(r'^\d+$', num_via_str):
                        row['NUM_VIA'] = num_via_str.zfill(3)
                    else:
                        row['NUM_VIA'] = re.sub(r'\s+', '', num_via_str)
                    #Si DIRECCION_AUX no está vacío
                    if pd.notna(row['DIRECCION_AUX']):
                        partes = row['DIRECCION_AUX'].split(',')
                        if len(partes) > 1:
                            partes[0] = re.sub(r'^(Avenida_|Calle_|Parque_|Plaza_)\s*', '', partes[0].strip())
                            partes[0] = re.sub(r'(_zona_\d+|_\d+)$', '', partes[0])
                            row['NOM_VIA'] = partes[0]
                        else:
                            row['NOM_VIA'] = f"{row['ID']}_NOM-VIA_desconocido"
        #Si TIPO_VIA está vacío
        else:
            #Si NOM_VIA no está vacío
            if pd.notna(row['NOM_VIA']):
                #Si NUM_VIA no está vacío
                if pd.notna(row['NUM_VIA']):
                    #Formato NUM_VIA
                    num_via_str = str(row['NUM_VIA']).strip()
                    if re.match(r'^\d+$', num_via_str):
                        row['NUM_VIA'] = num_via_str.zfill(3)
                    else:
                        row['NUM_VIA'] = re.sub(r'\s+', '', num_via_str)
                    #Si DIRECCION_AUX no está vacío
                    if pd.notna(row['DIRECCION_AUX']):
                        partes = row['DIRECCION_AUX'].split(',')
                        if len(partes) > 1:
                            tipo_via = {"Avenida_": "Avenida",
                                        "Calle_": "Calle",
                                        "Parque_": "Parque",
                                        "Pasaje_": "Pasaje",
                                        "Plaza_": "Plaza"}
                            row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
                            for prefix, tipo in tipo_via.items():
                                if partes[0].startswith(prefix):
                                    row['TIPO_VIA'] = tipo
                                    partes[0] = re.sub(r'^' + prefix, '', partes[0].strip())
                                    break
                            partes[0] = re.sub(r'(_\d+)$', '', partes[0])
                            row['NOM_VIA'] = partes[0]
                        else:
                            row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
                    #Si DIRECCION_AUX está vacío
                    else:
                        row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
                        row['DIRECCION_AUX'] = f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}" 
            #Si NOM_VIA está vacío
            else: 
                #Si NUM_VIA está vacío
                if pd.isna(row['NUM_VIA']):
                    #Si DIRECCION_AUX no está vacío
                    if pd.notna(row['DIRECCION_AUX']):
                        if ',' in row['DIRECCION_AUX']:
                            partes = row['DIRECCION_AUX'].split(',')
                        else:
                            partes = [row['DIRECCION_AUX']]
                        tipo_via = {"Avenida_": "Avenida",
                                    "Calle_": "Calle",
                                    "Parque_parque_/": "Parque",
                                    "Parque_": "Parque",
                                    "Paseo_": "Paseo",
                                    "Plaza_": "Plaza",
                                    "Via_": "Via"}
                        row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
                        for prefix, tipo in tipo_via.items():
                            if partes[0].startswith(prefix):
                                row['TIPO_VIA'] = tipo
                                partes[0] = re.sub(r'^' + prefix, '', partes[0].strip())
                                break
                        parte_original = partes[0]
                        partes[0] = re.sub(r'(_nº?\d+|_\d+)$', '', partes[0]).strip()
                        if parte_original != partes[0]:
                            numero_encontrado = re.search(r'(\d+)$', parte_original)
                            row['NUM_VIA'] = numero_encontrado.group()
                        else:
                            row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
                        row['NOM_VIA'] = partes[0]   
                    #Si DIRECCION_AUX está vacío
                    else:
                        row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
                        row['NOM_VIA'] = f"{row['ID']}_NOM-VIA_desconocido"
                        row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
                        row['DIRECCION_AUX'] = f"{row['ID']}_DIRECCION-AUX_desconocida"
    return row


def campo_ndp_areas(row, archivo_juegos):
    barrio = row['BARRIO']
    if pd.isna(row['NDP']):
        juegos_filtrados = archivo_juegos[(archivo_juegos['BARRIO'] == barrio) & pd.notna(archivo_juegos['NDP'])]
        juegos_filtrados = juegos_filtrados.sort_values(by='NDP', ascending=True)
        if not juegos_filtrados.empty:
            ndp_mas_bajo = juegos_filtrados.iloc[0]['NDP']
            row['NDP'] = ndp_mas_bajo
        else:
            row['NDP'] = f"{row['ID']}_NDP_desconocido"
    return row


def campos_relacionados_con_direccion_auxiliar_juegos(row):
    #Si TIPO_VIA, NOM_VIA, NUM_VIA y DIRECCION_AUX están vacíos
    if pd.isna(row['TIPO_VIA']) and pd.isna(row['NOM_VIA']) and pd.isna(row['NUM_VIA']) and pd.isna(row['DIRECCION_AUX']):
        row['DIRECCION_AUX'] = f"{row['ID']}_DIRECCION-AUX_desconocida"
        row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
        row['NOM_VIA'] = f"{row['ID']}_NOM-VIA_desconocido"
        row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
    #Si TIPO_VIA, NOM_VIA, NUM_VIA o DIRECCION_AUX no están vacíos
    else:
        #Si TIPO_VIA no está vacío
        if pd.notna(row['TIPO_VIA']):
            #Si NOM_VIA no está vacío
            if pd.notna(row['NOM_VIA']):
                #Si NUM_VIA no está vacío
                if pd.notna(row['NUM_VIA']):
                    #Formato NUM_VIA
                    num_via_str = str(row['NUM_VIA']).strip()
                    if re.match(r'^\d+$', num_via_str):
                        row['NUM_VIA'] = num_via_str.zfill(3)
                    else:
                        row['NUM_VIA'] = re.sub(r'\s+', '', num_via_str)
                    #!!!Condición Avenida!!!
                    if (row['TIPO_VIA'] == "Avenida" and re.search(r'Avenida[^_]*_', row['NOM_VIA'])):
                        row['DIRECCION_AUX'] = f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}"
                    #!!!Condición Paseo!!!
                    elif (row['TIPO_VIA'] == "Paseo" and re.search(r'Paseo[^_]*_', row['NOM_VIA'])):
                        row['DIRECCION_AUX'] = f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}"
                    else:
                        row['DIRECCION_AUX'] = f"{row['TIPO_VIA']} {row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}"
            #Si NOM_VIA está vacío        
            else:
                #Si NUM_VIA no está vacío
                if pd.notna(row['NUM_VIA']):
                    #Formato NUM_VIA
                    num_via_str = str(row['NUM_VIA']).strip()
                    if re.match(r'^\d+$', num_via_str):
                        row['NUM_VIA'] = num_via_str.zfill(3)
                    else:
                        row['NUM_VIA'] = re.sub(r'\s+', '', num_via_str)
                    #Si DIRECCION_AUX está vacía
                    if pd.isna(row['DIRECCION_AUX']):
                        row['NOM_VIA'] = f"{row['ID']}_NOM-VIA_desconocido"
                        row['DIRECCION_AUX'] = f"{row['ID']}_DIRECCION-AUX_desconocida"
        #Si TIPO_VIA está vacío
        else:
            #Si NOM_VIA no está vacío
            if pd.notna(row['NOM_VIA']):
                #Si NUM_VIA no está vacío
                if pd.notna(row['NUM_VIA']):
                    #Formato NUM_VIA
                    num_via_str = str(row['NUM_VIA']).strip()
                    if re.match(r'^\d+$', num_via_str):
                        row['NUM_VIA'] = num_via_str.zfill(3)
                    else:
                        row['NUM_VIA'] = re.sub(r'\s+', '', num_via_str)
                    #Si DIRECCION_AUX está vacío
                    if pd.isna(row['DIRECCION_AUX']):
                        row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
                        row['DIRECCION_AUX'] = f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}" 
            #Si NOM_VIA está vacío
            else: 
                #Si NUM_VIA está vacío
                if pd.isna(row['NUM_VIA']):
                    #Si DIRECCION_AUX no está vacío
                    if pd.notna(row['DIRECCION_AUX']):
                        #Si DIRECCION_AUX contiene ,
                        if ',' in row['DIRECCION_AUX']:
                            partes = row['DIRECCION_AUX'].split(',')
                            tipo_via = {"Autovia_ia_": "Autovia",
                                        "Autovia_": "Autovia",
                                        "Avenida_._de_": "Avenida",
                                        "Avenida_._": "Avenida",
                                        "Avenida__": "Avenida",
                                        "Avenida_": "Avenida",
                                        "Bulevar_": "Bulevar",
                                        "Calle_del_": "Calle",
                                        "Calle_de_": "Calle",
                                        "Calle__": "Calle",
                                        "Calle_": "Calle",
                                        "Camino_de_": "Camino",
                                        "Camino_": "Camino",
                                        "Cañada_del_": "Cañada",
                                        "Cañada_de_": "Cañada",
                                        "Carretera_": "Carretera",
                                        "Cuesta_del_": "Cuesta",
                                        "Gta_glorieta_": "Glorieta",
                                        "Parque_parque_": "Parque",
                                        "Parque_del_": "Parque",
                                        "Parque_de_": "Parque",
                                        "Parque_": "Parque",
                                        "Parque_forestal/": "Parque",
                                        "Pasaje_":"Pasaje",
                                        "Paseo_paseo_": "Paseo",
                                        "Paseo_del_":"Paseo",
                                        "Paseo_de_":"Paseo",
                                        "Paseo_":"Paseo",
                                        "Plaza_": "Plaza",
                                        "Ronda_": "Ronda",
                                        "Travesia_": "Travesia",
                                        "Via_": "Via"}
                            row['TIPO_VIA'] = "Calle"
                            for prefix, tipo in tipo_via.items():
                                if partes[0].startswith(prefix):
                                    row['TIPO_VIA'] = tipo
                                    partes[0] = re.sub(r'^' + prefix, '', partes[0].strip())
                                    break
                            match = re.match(r'(.+?)\s*_(?:nº)?(\d+)(_[^,]*)?', partes[0].strip())
                            if match:
                                row['NOM_VIA'] = match.group(1).strip()
                                row['NUM_VIA'] = match.group(2).zfill(3)
                            else:
                                row['NOM_VIA'] = partes[0].strip()
                                if re.search(r'ascendiente', partes[1], re.IGNORECASE):
                                    row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
                                else:
                                    row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
                                    num_match = re.search(r'_?(\d+_?+[a-zA-Z]?)', partes[1])
                                    if num_match:
                                        num_via = num_match.group(1).replace('_', '').lstrip('0').upper()
                                        row['NUM_VIA'] = num_via
                        #Si DIRECCION_AUX no contiene ,
                        else:
                            partes = row['DIRECCION_AUX']
                            tipo_via = {"Autovia_ia_": "Autovia",
                                        "Autoviaia_": "Autovia",
                                        "Av._": "Avenida",
                                        "Av.": "Avenida",
                                        "Avd_de_": "Avenida",
                                        "Avd._": "Avenida",
                                        "Avda._de_": "Avenida",
                                        "Avenida_._de_": "Avenida",
                                        "Avenida_._": "Avenida",
                                        "Avenida_del_": "Avenida",
                                        "Avenida_": "Avenida",
                                        "-c_": "Calle",
                                        "C_/de_": "Calle",
                                        "C_": "Calle",
                                        "C-calle_": "Calle",
                                        "C·_calle_": "Calle",
                                        "Calle__c/": "Calle",
                                        "Calle_c/": "Calle",
                                        "Calle_	_": "Calle",
                                        "Calle__": "Calle",
                                        "Calle_del_": "Calle",
                                        "Calle_de_": "Calle",
                                        "Calle_": "Calle",
                                        "Camino_de_": "Camino",
                                        "Camino_": "Camino",
                                        "Cañada_de_": "Cañada",
                                        "Carretera_de_": "Carretera",
                                        "Carretera_": "Carretera",
                                        "Cuesta_del_": "Cuesta",
                                        "P-_": "Parque",
                                        "P._": "Parque",
                                        "Pq-parque_": "Parque",
                                        "Parque_del_": "Parque",
                                        "Parque_de_": "Parque",
                                        "Parque-parque_parque_/": "Parque",
                                        "Parque-parque_parque_": "Parque",
                                        "Parque-parque_": "Parque",
                                        "Parque_": "Parque",
                                        "Pso/": "Paseo",
                                        "Paseo_de_": "Paseo",
                                        "Paseo_":"Paseo",
                                        "Plaza_a._del_": "Plaza",
                                        "Plaza_a._de_": "Plaza",
                                        "Pza._del_": "Plaza",
                                        "Pza._de_": "Plaza",
                                        "Plaza_": "Plaza",
                                        "Ronda_de_": "Ronda",
                                        "Ronda_": "Ronda",
                                        "Travesia_": "Travesia"}
                            row['TIPO_VIA'] = "Calle"
                            for prefix, tipo in tipo_via.items():
                                if partes.startswith(prefix):
                                    row['TIPO_VIA'] = tipo
                                    partes = re.sub(r'^' + prefix, '', partes.strip())
                                    break
                            match = re.match(r'(.+?)\s*_(?:nº)?(\d+)(_[^,]*)?', partes.strip())
                            if match:
                                row['NOM_VIA'] = match.group(1).strip()
                                row['NUM_VIA'] = match.group(2).zfill(3)
                            else:
                                row['NOM_VIA'] = partes.strip()
                                row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
                    #Si DIRECCION_AUX está vacío
                    else:
                        row['TIPO_VIA'] = f"{row['ID']}_TIPO-VIA_desconocida"
                        row['NOM_VIA'] = f"{row['ID']}_NOM-VIA_desconocido"
                        row['NUM_VIA'] = f"{row['ID']}_NUM-VIA_desconocido"
                        row['DIRECCION_AUX'] = f"{row['ID']}_DIRECCION-AUX_desconocida"
    return row


def campo_ndp_juegos(archivo_juegos):
    barrios_con_ndp_nulo = archivo_juegos[archivo_juegos['NDP'].isna()]['BARRIO'].unique()

    for barrio in barrios_con_ndp_nulo:
        juegos_filtrados = archivo_juegos[(archivo_juegos['BARRIO'] == barrio) & pd.notna(archivo_juegos['NDP'])]
        juegos_filtrados = juegos_filtrados.sort_values(by='NDP', ascending=True)
        if not juegos_filtrados.empty:
            ndp_mas_bajo = juegos_filtrados.iloc[0]['NDP']
            archivo_juegos.loc[(archivo_juegos['BARRIO'] == barrio) & pd.isna(archivo_juegos['NDP']), 'NDP'] = ndp_mas_bajo

    return archivo_juegos


def formato_mayuscula_NOM_VIA(df):
    df['NOM_VIA'] = df['NOM_VIA'].apply(lambda x: x.capitalize())
    return df


def asegurar_formato_direccion_aux_areas(df):
    df = formato_mayuscula_NOM_VIA(df)
    nom_via_desconocido = re.compile(r'^\d+_NOM-VIA_desconocido$', re.IGNORECASE)
    tipo_via_desconocida = re.compile(r'^\d+_TIPO-VIA_desconocida$', re.IGNORECASE)
    numero_via_desconocido = re.compile(r'^\d+_NUM-VIA_desconocido$', re.IGNORECASE)
    df['DIRECCION_AUX'] = df.apply(lambda row: (
        #nom_via_desconocido
        f"{row['ID']}_DIRECCION-AUX_desconocida"if nom_via_desconocido.match(row['NOM_VIA'])
        #tipo_via_desconocida
        else f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}" if tipo_via_desconocida.match(row['TIPO_VIA'])
        #avenida
        else f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}" if row['TIPO_VIA']=="Avenida" and row['NOM_VIA'].startswith("Avenida")
        #paseo
        else f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}" if row['TIPO_VIA']=="Paseo" and row['NOM_VIA'].startswith("Paseo")
        #numero_via_desconocido
        else f"{row['TIPO_VIA']}_{row['NOM_VIA'].lower()}" if numero_via_desconocido.match(row['NUM_VIA'])
        #caso general
        else f"{row['TIPO_VIA']}_{row['NOM_VIA'].lower()}, {str(row['NUM_VIA']).zfill(3)}"
    ), axis=1)
    return df


def asegurar_formato_direccion_aux_juegos(df):
    df = formato_mayuscula_NOM_VIA(df)
    nom_via_desconocido = re.compile(r'^\d+_NOM-VIA_desconocido$', re.IGNORECASE)
    tipo_via_desconocida = re.compile(r'^\d+_TIPO-VIA_desconocida$', re.IGNORECASE)
    numero_via_desconocido = re.compile(r'^\d+_NUM-VIA_desconocido$', re.IGNORECASE)
    df['DIRECCION_AUX'] = df.apply(lambda row: (
        #nom_via_desconocido
        f"{row['ID']}_DIRECCION-AUX_desconocida"if nom_via_desconocido.match(row['NOM_VIA'])
        #tipo_via_desconocida && numero_via_desconocido
        else f"{row['NOM_VIA']}" if tipo_via_desconocida.match(row['TIPO_VIA']) and numero_via_desconocido.match(row['NUM_VIA'])
        #tipo_via_desconocida
        else f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}" if tipo_via_desconocida.match(row['TIPO_VIA'])
        #avenida
        else f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}" if row['TIPO_VIA']=="Avenida" and row['NOM_VIA'].startswith("Avenida")
        #paseo
        else f"{row['NOM_VIA']}, {str(row['NUM_VIA']).zfill(3)}" if row['TIPO_VIA']=="Paseo" and row['NOM_VIA'].startswith("Paseo")
        #numero_via_desconocido
        else f"{row['TIPO_VIA']}_{row['NOM_VIA'].lower()}" if numero_via_desconocido.match(row['NUM_VIA'])
        #caso general
        else f"{row['TIPO_VIA']}_{row['NOM_VIA'].lower()}, {str(row['NUM_VIA']).zfill(3)}"
    ), axis=1)
    return df


def rellenar_direccion_areas(archivo_areas):
    print("rellenar_direccion_areas")
    df_areas = archivo_areas.copy()
    df_areas = df_areas.apply(campos_relacionados_con_direccion_auxiliar_areas, axis=1)
    df_areas = asegurar_formato_direccion_aux_areas(df_areas)
    return df_areas


def rellenar_ndp_areas(archivo_areas, archivo_juegos):
    print("rellenar_ndp_areas")
    df_areas = archivo_areas.copy()
    df_areas = df_areas.apply(campo_ndp_areas, axis=1, args=(archivo_juegos,))
    return df_areas


def rellenar_direccion_juegos(archivo_juegos):
    print("rellenar_direccion_juegos")
    df_juegos = archivo_juegos.copy()
    df_juegos = df_juegos.apply(campos_relacionados_con_direccion_auxiliar_juegos, axis=1)
    df_juegos = asegurar_formato_direccion_aux_juegos(df_juegos)
    return df_juegos


def rellenar_ndp_juegos(archivo_juegos):
    print("rellenar_ndp_juegos")
    df_juegos = archivo_juegos.copy()
    df_juegos = campo_ndp_juegos(df_juegos)
    return df_juegos


def contar_juegos(archivo_areas, archivo_juegos):
    print("contar_juegos")  
    # Obtener una lista única de todos los tipos de juegos
    tipos_juego = archivo_juegos['tipo_juego'].unique()

    # Crear un diccionario para almacenar los conteos de tipos de juego por cada valor de NDP
    conteo_por_ndp = defaultdict(lambda: {tipo: 0 for tipo in tipos_juego})
    
    # Contar las ocurrencias por cada combinación de NDP y tipo_juego en archivo_juegos
    for _, row in archivo_juegos.iterrows():
        ndp = row['NDP']
        tipo_juego = row['tipo_juego']
        conteo_por_ndp[ndp][tipo_juego] += 1
    
    # Crear la columna NUM_JUEGOS en df_areas usando el conteo agrupado por NDP
    archivo_areas['CANTIDAD_JUEGOS_POR_TIPO'] = archivo_areas['NDP'].apply(lambda ndp: conteo_por_ndp[ndp])


def calcular_capacidadMAX(archivo_areas, archivo_juegos):
    print("calcular_capacidadMAX")
    # Contar la cantidad de juegos para cada valor de "NDP" en archivo_juegos
    conteo_juegos = archivo_juegos['NDP'].value_counts()

    def get_random(ndp):
        # Obtenemos el conteo de juegos para el NDP, con un valor mínimo de 0 si no existe
        count = conteo_juegos.get(ndp, 1)
        # Generar un número aleatorio entre 'count' y 40
        return random.randint(count, 40)

    archivo_areas['CAPACIDAD_MAX'] = archivo_areas['NDP'].map(get_random).fillna(1).astype(int)


def calcular_desgaste(archivo_juegos, archivo_mantenimiento):
    print("calcular_desgaste")
    num_mantenimientos = archivo_mantenimiento['JuegoID'].value_counts()
    tiempo_uso = random.randint(1, 15)

    # Mapa de valores para INDICADOR_EXPOSICION
    exposicion_map = {'Baja': 10, 'Media': 50, 'Alta': 100}

    def calcular_desgaste_por_id(juego_id, exposicion):
        # Obtiene el conteo de mantenimientos (0 si no existe)
        numero_mantenimientos = num_mantenimientos.get(juego_id, 0)
        desgaste_acumulado = (tiempo_uso * exposicion_map[exposicion]) - (numero_mantenimientos * 100)
        return desgaste_acumulado
    
    archivo_juegos['DESGASTE_ACUMULADO'] = archivo_juegos.apply(lambda row: calcular_desgaste_por_id(row['ID'], row['INDICADOR_EXPOSICION']), axis=1)


def calcular_ultimoMantenimiento(archivo_juegos, archivo_mantenimiento):
    print("calcular_ultimoMantenimiento")
    # Agrupar archivo_mantenimiento por 'JuegoID' y obtener la fecha más reciente de 'FECHA_INTERVENCION'
    ultimo_mantenimiento = archivo_mantenimiento.groupby('JuegoID')['FECHA_INTERVENCION'].max().reset_index()
    
    # Realizar un merge para añadir la fecha más reciente de mantenimiento al archivo_juegos
    archivo_juegos = archivo_juegos.merge(ultimo_mantenimiento, how='left', left_on='ID', right_on='JuegoID')
    
    # Renombrar la columna resultante para que sea 'ULTIMA_FECHA_MANTENIMIENTO'
    archivo_juegos = archivo_juegos.rename(columns={'FECHA_INTERVENCION': 'ULTIMA_FECHA_MANTENIMIENTO'})
    
    # Eliminar la columna 'JuegoID' resultante del merge para no modificar las columnas originales
    archivo_juegos = archivo_juegos.drop(columns=['JuegoID'])
    
    # Rellenar los valores faltantes en 'ULTIMA_FECHA_MANTENIMIENTO' con el formato especificado
    archivo_juegos['ULTIMA_FECHA_MANTENIMIENTO'] = archivo_juegos.apply(
        lambda row: f"{row.ID}-ULTIMA_FECHA_MANTENIMIENTO-ausente" if pd.isna(row['ULTIMA_FECHA_MANTENIMIENTO']) else row['ULTIMA_FECHA_MANTENIMIENTO'],
        axis=1
    )
    
    return archivo_juegos


def dividir_incidencias(archivo_incidencias):
    print("dividir_incidencias")
    # Crear una lista para almacenar los nuevos registros
    nuevas_filas = []

    # Recorrer el DataFrame para procesar cada fila
    for _, fila in archivo_incidencias.iterrows():
        # Obtener el ID de la incidencia y el UsuarioID (puede tener múltiples valores)
        id_incidencia = fila['ID']
        usuarios = fila['UsuarioID']
        
        # Convertir el string de UsuarioID a una lista, quitando los corchetes y los espacios
        usuarios = usuarios.strip("[]").replace(" ", "").split(',')
        
        # Crear una nueva fila para cada usuario en UsuarioID
        for usuario in usuarios:
            nueva_fila = fila.copy()  # Copiar la fila original
            nueva_fila['UsuarioID'] = usuario  # Reemplazar UsuarioID por el usuario actual
            nuevas_filas.append(nueva_fila)  # Agregar la nueva fila a la lista

    # Crear un nuevo DataFrame con las filas expandidas
    archivo_incidencias = pd.DataFrame(nuevas_filas)

    return archivo_incidencias


def dividir_mantenimientoID(archivo_incidencias):
    print("dividir_mantenimientoID")
    # Crear una lista para almacenar los nuevos registros
    nuevas_filas = []

    # Recorrer el DataFrame para procesar cada fila
    for _, fila in archivo_incidencias.iterrows():
        # Obtener el ID de la incidencia y el MantenimientoID (puede tener múltiples valores)
        id_incidencia = fila['ID']
        usuarios = fila['MantenimeintoID']

        # Si 'usuarios' no es una lista, lo convertimos
        if isinstance(usuarios, str):
            # Convertir el string de MantenimeintoID a una lista
            usuarios = usuarios.strip("[]").replace(" ", "").split(',')
        
        # Crear una nueva fila para cada usuario en MantenimeintoID
        for usuario in usuarios:
            nueva_fila = fila.copy()  # Copiar la fila original
            nueva_fila['MantenimeintoID'] = usuario  # Reemplazar MantenimeintoID por el usuario actual
            nuevas_filas.append(nueva_fila)  # Agregar la nueva fila a la lista

    # Crear un nuevo DataFrame con las filas expandidas
    archivo_incidencias = pd.DataFrame(nuevas_filas)

    return archivo_incidencias


def transformar_meteo(archivo_meteo24, archivo_estaciones):
    print("transformar_meteo")
    # Cargar los datos del archivo CSV original
    df_original = archivo_meteo24

    # Crear la columna 'ID'
    df_original['ID'] = df_original['PUNTO_MUESTREO'].str[:8] + df_original['ANO'].astype(str) + df_original['MES'].astype(str).str.zfill(2)

    # Crear la columna 'Fecha' (primer día del mes y año)
    df_original['Fecha'] = pd.to_datetime(df_original['ANO'].astype(str) + '-' + df_original['MES'].astype(str).str.zfill(2) + '-01')

    # Mapeo de MAGNITUD a las columnas correspondientes
    mapeo_magnitud = {
        81: 'VIENTO',
        83: 'TEMPERATURA',
        89: 'PRECIPITACION'
    }

    # Inicializar el DataFrame transformado con las columnas necesarias
    archivo_meteo24 = df_original[['ID', 'Fecha']].drop_duplicates().copy()
    for columna in mapeo_magnitud.values():
        archivo_meteo24[columna] = -1

    # Procesar cada MAGNITUD y calcular la media de las mediciones para cada mes
    for magnitud, columna in mapeo_magnitud.items():
        # Filtrar por MAGNITUD y calcular la media de las columnas D (mediciones)
        df_magnitud = df_original[df_original['MAGNITUD'] == magnitud].copy()
        mediciones = df_magnitud.filter(regex='^D[0-9]+$').replace('V', np.nan).astype(float).mean(axis=1)
        df_magnitud[columna] = mediciones.fillna(-1).round(2)

        # Asegurarnos de que la columna esté presente al unir
        if columna not in archivo_meteo24.columns:
            archivo_meteo24[columna] = -1

        # Combinar los datos de cada magnitud al DataFrame transformado
        archivo_meteo24 = pd.merge(
            archivo_meteo24,
            df_magnitud[['ID', columna]],
            on='ID',
            how='left',
            suffixes=('', '_new')
        )

        # Actualizar la columna con los nuevos datos y llenar los valores faltantes
        archivo_meteo24[columna] = archivo_meteo24[columna + '_new'].combine_first(archivo_meteo24[columna])
        archivo_meteo24.drop(columns=[columna + '_new'], inplace=True)

    # Renombrar las columnas para facilitar el merge
    archivo_estaciones.rename(columns={'CÓDIGO': 'PUNTO_MUESTREO', 'Codigo Postal': 'COD_POSTAL'}, inplace=True)

    # Procesar los códigos postales para quedarse con el primero solo en el caso específico
    archivo_estaciones['COD_POSTAL'] = archivo_estaciones['COD_POSTAL'].apply(lambda x: int(x.split(',')[0]) if ',' in x else int(x))

    # Extraer los primeros 8 dígitos del ID para el mapeo
    archivo_meteo24['PUNTO_MUESTREO'] = archivo_meteo24['ID'].str[:8]
    archivo_estaciones['PUNTO_MUESTREO'] = archivo_estaciones['PUNTO_MUESTREO'].astype(str)

    # Unir el DataFrame transformado con los códigos postales
    archivo_meteo24 = pd.merge(archivo_meteo24, archivo_estaciones[['PUNTO_MUESTREO', 'COD_POSTAL']], on='PUNTO_MUESTREO', how='left')

    # Ajustar el formato de la columna 'ID' para que coincida con 'MeteoLimpio.csv'
    archivo_meteo24['ID'] = archivo_meteo24['ID'].str[:8] + '-' + archivo_meteo24['ID'].str[8:]

    # Reordenar las columnas para que coincidan con el archivo MeteoLimpio.csv
    archivo_meteo24 = archivo_meteo24[['ID', 'COD_POSTAL', 'Fecha', 'VIENTO', 'TEMPERATURA', 'PRECIPITACION']]

    # Cambiar formato a las fechas para cumplir con nuestro estandar
    if "Fecha" in archivo_meteo24.columns:
        archivo_meteo24["Fecha"] = archivo_meteo24["Fecha"].astype(str).apply(convert_to_iso)

    return archivo_meteo24


def añadir_tiempo_resolucion(archivo_incidencias):
    print("añadir_tiempo_resolucion")
    #Generar un diccionario con los tiempos de resolución de cada incidencia
    tiempo_res_dict ={}

    #Generar el tiempo de resolucion para cada ID
    for id_incidencia in archivo_incidencias['ID'].unique():
        tiempo_res_dict[id_incidencia] = random.randint(1, 60)
    
    #Añadir la columna de tiempo de resolución al archivo de incidencias
    archivo_incidencias['TIEMPO_RESOLUCION'] = archivo_incidencias['ID'].map(tiempo_res_dict).fillna(0).astype(int)

    return archivo_incidencias


def añadir_tiempo_resolucion(archivo_incidencias):
    #Generar un diccionario con los tiempos de resolución de cada incidencia
    tiempo_res_dict ={}
    #Generar el tiempo de resolucion para cada ID
    for id_incidencia in archivo_incidencias['ID'].unique():
        tiempo_res_dict[id_incidencia] = random.randint(1, 365)
    
    #Añadir la columna de tiempo de resolución al archivo de incidencias
    archivo_incidencias['TIEMPO_RESOLUCION'] = archivo_incidencias['ID'].map(tiempo_res_dict).fillna(1).astype(int)
    return archivo_incidencias


def convertir_csv_comas(path_in, path_out):
    print("convertir_csv_comas")
    with open(path_in, 'r', newline='', encoding='utf-8') as csv_entrada:
        lector = csv.reader(csv_entrada, delimiter=';')
        filas = list(lector)  # Guarda todas las filas en una lista

    # Abrimos el archivo de salida y escribimos los datos con coma como separador
    with open(path_out, 'w', newline='', encoding='utf-8') as csv_salida:
        escritor = csv.writer(csv_salida, delimiter=',')
        for fila in filas:
            escritor.writerow(fila)


def leer_archivos(tipo):
    print("leer_archivos")
    if tipo == "sucio":
        archivo_areas = pd.read_csv("Datasets/AreasSucio.csv")
        archivo_juegos = pd.read_csv("Datasets/JuegosSucio.csv")
        archivo_usuarios = pd.read_csv("Datasets/UsuariosSucio.csv")
        archivo_incidencias = pd.read_csv("Datasets/IncidenciasUsuariosSucio.csv")
        archivo_incidentes = pd.read_csv("Datasets/IncidentesSeguridadSucio.csv")
        archivo_mantenimiento = pd.read_csv("Datasets/MantenimientoSucio.csv")
        archivo_encuestas = pd.read_csv("Datasets/EncuestasSatisfaccionSucio.csv")
        archivo_meteo24 = pd.read_csv("Datasets/meteo24Limpio.csv")
        archivo_estaciones = pd.read_csv("Datasets/estaciones_meteo_CodigoPostalLimpio.csv")
    else:
        archivo_areas = pd.read_csv("Datasets/AreasLimpio.csv")
        archivo_juegos = pd.read_csv("Datasets/JuegosLimpio.csv")
        archivo_usuarios = pd.read_csv("Datasets/UsuariosLimpio.csv")
        archivo_incidencias = pd.read_csv("Datasets/IncidenciasUsuariosLimpio.csv")
        archivo_incidentes = pd.read_csv("Datasets/IncidentesSeguridadLimpio.csv")
        archivo_mantenimiento = pd.read_csv("Datasets/MantenimientoLimpio.csv")
        archivo_encuestas = pd.read_csv("Datasets/EncuestasSatisfaccionLimpio.csv")
        archivo_meteo24 = pd.read_csv("Datasets/meteo24Limpio.csv")
        archivo_estaciones = pd.read_csv("Datasets/estaciones_meteo_CodigoPostalLimpio.csv")

    return archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_encuestas, archivo_meteo24, archivo_estaciones


def escribir_archivos(archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_encuestas, archivo_meteo24, archivo_estaciones):
    print("escribir_archivos")
    archivo_areas.to_csv("Datasets/AreasLimpio.csv", index=False)
    archivo_juegos.to_csv("Datasets/JuegosLimpio.csv", index=False)
    archivo_usuarios.to_csv("Datasets/UsuariosLimpio.csv", index=False)
    archivo_incidencias.to_csv("Datasets/IncidenciasUsuariosLimpio.csv", index=False)
    archivo_incidentes.to_csv("Datasets/IncidentesSeguridadLimpio.csv", index=False)
    archivo_mantenimiento.to_csv("Datasets/MantenimientoLimpio.csv", index=False)
    archivo_encuestas.to_csv("Datasets/EncuestasSatisfaccionLimpio.csv", index=False)
    archivo_meteo24.to_csv("Datasets/meteo24Limpio.csv", index=False)
    archivo_estaciones.to_csv("Datasets/estaciones_meteo_CodigoPostalLimpio.csv", index=False)


# Función principal para consolidar los cambios y guardar los resultados finales
def guardar_cambios():
    convertir_csv_comas("Datasets/estaciones_meteo_CodigoPostal.csv", "Datasets/estaciones_meteo_CodigoPostalLimpio.csv")
    convertir_csv_comas("Datasets/meteo24.csv", "Datasets/meteo24Limpio.csv")

    # leer archivos
    archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_encuestas, archivo_meteo24, archivo_estaciones = leer_archivos("sucio")
     
    # Ejecucion funciones
    estandarizar_BarriosDistritos(archivo_areas, archivo_juegos)
    estandarizar_Usuarios(archivo_usuarios)
    estandarizar_Textos(archivo_areas, archivo_juegos, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_estaciones, archivo_encuestas)
    archivo_meteo24 = transformar_meteo(archivo_meteo24, archivo_estaciones)
    estandarizar_unidades(archivo_incidencias, archivo_mantenimiento)
    asignar_exposicion(archivo_juegos)
    archivo_incidencias = añadir_tiempo_resolucion(archivo_incidencias)
    archivo_incidencias = dividir_incidencias(archivo_incidencias)
    archivo_incidencias = dividir_mantenimientoID(archivo_incidencias)

    # escribir resultados
    escribir_archivos(archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_encuestas, archivo_meteo24, archivo_estaciones)

    # estas dos funciones actualizan por su cuenta los archivos limpios
    adaptar_coordenadas(archivo_areas, archivo_juegos)
    calcular_postal("Datasets/AreasLimpio.csv", "Datasets/JuegosLimpio.csv") # usa archivos limpios de entrada

    # leer archivos
    archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_encuestas, archivo_meteo24, archivo_estaciones = leer_archivos("limpio")

    archivo_areas = rellenar_direccion_areas(archivo_areas)
    archivo_juegos = rellenar_direccion_juegos(archivo_juegos)

    escribir_archivos(archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_encuestas, archivo_meteo24, archivo_estaciones)

    rellenar_fechas(archivo_areas, archivo_juegos, archivo_encuestas, archivo_incidencias, archivo_incidentes, archivo_mantenimiento)
    contar_juegos(archivo_areas, archivo_juegos)
    calcular_capacidadMAX(archivo_areas, archivo_juegos)
    calcular_desgaste(archivo_juegos, archivo_mantenimiento)
    archivo_juegos = calcular_ultimoMantenimiento(archivo_juegos, archivo_mantenimiento)

    escribir_archivos(archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento,archivo_encuestas, archivo_meteo24, archivo_estaciones)

    # leer archivos
    archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_encuestas, archivo_meteo24, archivo_estaciones = leer_archivos("limpio")

    archivo_areas = rellenar_ndp_areas(archivo_areas, archivo_juegos)
    archivo_juegos = rellenar_ndp_juegos(archivo_juegos)

    # escribir resultados finales
    escribir_archivos(archivo_areas, archivo_juegos, archivo_usuarios, archivo_incidencias, archivo_incidentes, archivo_mantenimiento, archivo_encuestas, archivo_meteo24, archivo_estaciones)

# Ejecutar la función principal
if __name__ == "__main__":
    guardar_cambios()
