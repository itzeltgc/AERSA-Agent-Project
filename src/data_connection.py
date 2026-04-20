from sqlalchemy import create_engine
import pandas as pd
import numpy as np 

engine = create_engine("mysql+pymysql://root:@localhost/talos_tecmty")

with engine.connect() as connection:
    print('conection sucessfull')

df_inventariomes = pd.read_sql("SELECT * FROM inventariomes", engine)
df_inventariomesdetalle = pd.read_sql('SELECT * FROM inventariomesdetalle', engine)


print(df_inventariomes.head(5))