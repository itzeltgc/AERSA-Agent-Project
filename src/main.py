from data_engineering import get_inventory_data
from zscore_model import calcular_zscore



df = get_inventory_data()
df_zscore = calcular_zscore(df)