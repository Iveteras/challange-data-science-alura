import pandas as pd


path = '../week_one/data/anuncios_vendas.parquet'
df_anuncios_vendas = pd.read_parquet(path)

print(df_anuncios_vendas.head())
