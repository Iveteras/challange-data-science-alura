import pandas as pd

# Reading the data base
df = pd.read_json('../data/dataset_bruto.json')

# Firs task
number_of_rows = df.shape[0] # 89083 rows
number_of_columns = df.shape[1] # 3 columns

type_os_columns = df.dtypes # all of them are objects

# creating a new DF with the columns of object in "anuncio"
anuncios_to_index = df['anuncio'][0]
indexs_columns = []

for index in anuncios_to_index:
    indexs_columns.append(index)

# inserting the "anuncio" objects into the df_anuncio


id, caracteristicas, area_util, tipo_anuncio, tipo_unidade, andar, vaga, suites, banheiros, tipo_uso, area_total, quartos, valores, endereco = [],[],[],[],[],[],[],[],[],[],[],[],[],[]

all_lists = [id, caracteristicas, area_util, tipo_anuncio, tipo_unidade, andar, vaga, suites, banheiros, tipo_uso, area_total, quartos, valores, endereco]

for object in df['anuncio']:
    all_lists[0].append(object['id'])
    all_lists[1].append(object['caracteristicas'])
    all_lists[2].append(object['area_util'])
    all_lists[3].append(object['tipo_anuncio'])
    all_lists[4].append(object['tipo_unidade'])
    all_lists[5].append(object['andar'])
    all_lists[6].append(object['vaga'])
    all_lists[7].append(object['suites'])
    all_lists[8].append(object['banheiros'])
    all_lists[9].append(object['tipo_uso'])
    all_lists[10].append(object['area_total'])
    all_lists[11].append(object['quartos'])
    all_lists[12].append(object['valores'])
    all_lists[13].append(object['endereco'])

df_anuncio = pd.DataFrame(columns=indexs_columns, index=range(89083))
df_anuncio.id = id
df_anuncio.caracteristicas = caracteristicas
df_anuncio.area_util = area_util 
df_anuncio.tipo_anuncio = tipo_anuncio
df_anuncio.tipo_unidade = tipo_unidade
df_anuncio.andar = andar
df_anuncio.vaga = vaga
df_anuncio.suites = suites
df_anuncio.banheiros = banheiros
df_anuncio.tipo_uso = tipo_uso
df_anuncio.area_total = area_total
df_anuncio.quartos = quartos
df_anuncio.valores = valores
df_anuncio.endereco = endereco

# exporting as csv, so i can create a new file .py
df_anuncio.to_csv('../data/anuncios.csv', sep=';', index=False)


