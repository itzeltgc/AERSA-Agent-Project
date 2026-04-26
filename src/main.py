from data_engineering import get_inventory_data
from zscore_model import calcular_zscore
#from kpis import calculate_kpis


df = get_inventory_data()
df_zscore = calcular_zscore(df)
#kpis = calculate_kpis(df_zscore)


print(df.zscore)