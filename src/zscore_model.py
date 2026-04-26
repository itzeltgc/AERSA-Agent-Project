# zscore_model.py
# Modelo Z-score: Objetivo 1 TALOS Copilot
# Contiene las funciones del modelo de detección de anomalías de inventario basado en Z-score por producto + almacén.
# Este módulo es importado por la interfaz Streamlit.

## Importar librerías
import pandas as pd
import numpy as np

## 5.1 Función para clasificar la alerta de cada registro según los umbrales definidos para el z-score
def clasificar_severidad(z): 
    """
    - Clasifica la severidad de una anomalía de inventario basándose en el Z-score.
    - Los umbrables se basan en las propiedades de distribución normal.
    
    Args:
        z (float): Z-score calculado para un producto en un cierre específico.
                   Puede ser NaN si no hay historial previo o si std_historica = 0.

    Returns:
        string: Etiqueta de severidad:
                - 'sin_historial': Z-score es NaN
                - 'normal': |z| < 2 (normal)
                - 'alerta': 2 <= |z| < 3 (alerta)
                - 'critico': |z| >= 3 
    """
    if pd.isna(z):
        return 'sin_historial' # z es nulo, std es 0 o no hay historial
    elif abs(z) < 2:
        return 'normal' 
    elif abs(z) < 3:
        return 'alerta' 
    else:
        return 'critico'
    
    
## 5.2 Función para calcular Z-score
def calcular_zscore(df):
    """
    - Calcula el Z-score para cada registro del dataframe.
    - Encapsula todo el proceso de cálculo del Z-score:
        1. Ordena cronológicamente por producto + almacén + fecha
        2. Calcula media y std históricas con expanding window + shift(1) para evitar data leakage
            - Cada fila usa las estadisticas de los cierres anteriores, sin incluir el cierre actual
        3. Calcula el Z-score para cada registro
        4. Clasifica la severidad de una anomalía de inventario basándose en el Z-score

    Args:
        df (pd.DataFrame): Dataframe con los datos filtrados. 
                           Debe contener las columnas idproducto, idalmacen, inventariomes_fecha e inventariomesdetalle_diferencia
    Returns:
        pd.DataFrame: El mismo dataframe ordenado cronológicamente y con 5 columnas nuevas:
                      - media_historica: promedio histórico de diferencias
                      - std_historica: desviación estándar histórica
                      - n_cierres: número de cierres anteriores
                      - zscore: Z-score calculado para cada registro
                      - severidad: clasificación de la anomalía en base al umbral definido para el Z-score
    """
    
    # Ordenar cronológicamente por producto + almacén + fecha
    df = df.sort_values(['idproducto', 'idalmacen', 'inventariomes_fecha'])
    
    # Expanding window por producto + almacen
    # Para cada registro, usa todos los registros anteriores del mismo grupo
    expanding = df.groupby(['idproducto', 'idalmacen'])['inventariomesdetalle_diferencia'].expanding()
    
    # shift(1) desplaza los resultados un lugar hacia adelante, excluyendo el cierre ctual del historial
    df['media_historica'] = expanding.mean().shift(1).values # Media histórica desplazada un paso adelante 
    df['std_historica'] = expanding.std().shift(1).values    # Std histórica desplazada un paso adelante
    df['n_cierres'] = expanding.count().shift(1).values      # Número de registros desplazados un paso adelante
    
    # Calcular Z-score: z = (diferencia_actual - media_historica) / std_historica
    df['zscore'] = (df['inventariomesdetalle_diferencia'] - df['media_historica']) / df['std_historica']
    
    # Clasificar severidad usando la función clasificar_severidad
    df['severidad'] = df['zscore'].apply(clasificar_severidad)
    
    # Devolvemos dataframe actualizado
    return df
    

## 5.3 Función para generar hallazgos
def generar_hallazgos(df, id_cierre, top_n = 20):
    """
    Genera la tabla de hallazgos priorizados para un cierre específico.
        - Dado cualquier cierre de semana, filtra automáticamente los productos anómalos según el umbral definido para el Z-score.
        - Los ordena por impacto económico y devuelve una tabla lista para el auditor

    Args:
        df (pd.DataFrame): Dataframe con Z-scores y severidades ya calculados por calcular_zscore()
        id_cierre (int): ID del cierre a analizar (idinventariomes)
        top_n (int): Número máximo de hallazgos a mostrar. Es opcional con default = 20
    
    Returns:
        pd.DataFrame: Tabla ordenada por impacto económico con los hallazgos más relevantes del cierre. 
                      Devuelve un dataframe vacío si no hay anomalías.
    """
    # Verificar que el cierre existe
    if id_cierre not in df['idinventariomes'].values:
        print(f'El cierre {id_cierre} no existe en los datos.')
        return pd.DataFrame()
    
    # Obtener fecha del cierre
    fecha_cierre = df[df['idinventariomes'] == id_cierre]['inventariomes_fecha'].iloc[0]
    
    # Filtrar solo outliers de ese cierre (alerta o critico)
    outliers = df[(df['idinventariomes'] == id_cierre) & (df['severidad'].isin(['alerta', 'critico']))]
    
    # Si no hay outliers el cierre está limpio
    if len(outliers) == 0:
        print(f'Cierre {id_cierre} ({fecha_cierre}) sin outliers.')
        return pd.DataFrame()
    
    # Métricas resumen de cierre
    n_criticos = (outliers['severidad'] == 'critico').sum()  # Número de productos con diferencia 'critica'
    n_alertas = (outliers['severidad'] == 'alerta').sum()    # Número de productos con diferencia 'alerta'
    impacto_total = outliers['inventariomesdetalle_difimporte'].abs().sum() # Impacto económico total 
    
    print(f"Cierre: {id_cierre}-{fecha_cierre}")
    print(f"Críticos: {n_criticos}")
    print(f"Alertas: {n_alertas}")
    print(f"Impacto total: ${impacto_total:,.2f}")
    
    # Construir tabla de hallazgos
    return(
        # Priorizamos por impacto económico en valor absoluto
        outliers.sort_values('inventariomesdetalle_difimporte', key = abs, ascending = False
                             )
        # Seleccionar solo columnas relevantes para el auditor
        [['severidad',
          'producto_nombre',
          'almacen_nombre',
          'inventariomesdetalle_diferencia',
          'inventariomesdetalle_difimporte',
          'zscore'
          ]]
        # Renombramos columnas de la tabla para que sean mas legibles
        .rename(columns = {
            'severidad': 'Severidad',
            'producto_nombre': 'Producto',
            'almacen_nombre': 'Almacén',
            'inventariomesdetalle_diferencia': 'Diferencia',
            'inventariomesdetalle_difimporte': 'Impacto ($)',
            'zscore': 'Z-score'
            }).head(top_n).reset_index(drop = True).round(2) # Limitamos resultados y reiniciamos indice del dataframe    
    )
        
    
    