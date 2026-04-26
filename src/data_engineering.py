from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine
import pandas as pd
import os 


load_dotenv(Path(__file__).parent.parent / ".env")


def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_NAME")
    return create_engine(f"mysql+mysqlconnector://{user}@{host}/{database}")


query = '''
SELECT 
    im.idinventariomes,
    im.idempresa,
    im.idsucursal,
    im.idalmacen,
    imd.idproducto,
    im.inventariomes_fecha,
    im.inventariomes_revisada,
    imd.inventariomesdetalle_diferencia,
    imd.inventariomesdetalle_difimporte,
    imd.inventariomesdetalle_costopromedio,
    a.almacen_nombre,
    p.producto_nombre,
    p.idcategoria
FROM inventariomes im 
JOIN inventariomesdetalle imd ON im.idinventariomes = imd.idinventariomes
JOIN almacen a ON a.idalmacen = im.idalmacen
JOIN producto p ON imd.idproducto = p.idproducto
inner join (
    select im.idempresa, im.idsucursal, im.idalmacen, imd.idproducto
    from inventariomes im 
    join inventariomesdetalle imd on im.idinventariomes = imd.idinventariomes 
    join almacen a on im.idalmacen = a.idalmacen 
    WHERE im.inventariomes_estatus in ('terminado','finalizado','aplicado') 
    and a.almacen_estatus = 1 
    GROUP BY im.idempresa, im.idsucursal, im.idalmacen, imd.idproducto 
    HAVING COUNT(im.idinventariomes) >= 5
) cte1 ON imd.idproducto = cte1.idproducto AND im.idalmacen = cte1.idalmacen
WHERE imd.inventariomesdetalle_diferencia IS NOT NULL 
    AND im.inventariomes_estatus IN ('terminado', 'finalizado', 'aplicado')
    AND imd.inventariomesdetalle_costopromedio > 0
    AND a.almacen_estatus = 1
    AND p.producto_baja = 0;
'''


def get_inventory_data():
    engine = get_engine()
    df = pd.read_sql(query, engine)
    return df


if __name__ == "__main__":
    df = get_inventory_data()
    print(df.shape)
    print(df.head())