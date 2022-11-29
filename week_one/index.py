import pandas as pd

###################### Firsts steps ######################

# importing the data base
df_anuncios = pd.read_csv('data/anuncios.csv', sep=';')

quartos = list(df_anuncios['quartos'])
suites = list(df_anuncios['suites'])
banheiros = list(df_anuncios['banheiros'])
vagas = list(df_anuncios['vaga'])
area_total = list(df_anuncios['area_total'])
area_util = list(df_anuncios['area_util'])

# filtering the df to see how many residencial, apartamento and usado

residencial = df_anuncios['tipo_uso'] == 'Residencial'
freq_residencial = df_anuncios[residencial].shape[0] # return 84541

apartamento = df_anuncios['tipo_unidade'] == 'Apartamento'
freq_apartamento = df_anuncios[apartamento].shape[0] # return 66801

usado = df_anuncios['tipo_anuncio'] == 'Usado'
freq_usado = df_anuncios[usado].shape[0] # return 88827

###################### Make pandas object into int ######################

# Function that convert pandas object into int

def inputNumber(df_column, new_list):
    
    for item in df_column:
        
        item_to_list = eval(item)
        
        if (item_to_list == []):
            new_list.append(0)
        else:
            item_to_int = int(item_to_list[0])
            new_list.append(item_to_int)
    
# All the new lists
new_area_total, new_quartos, new_suites, new_banheiros, new_vagas, new_area_util = [],[],[],[],[],[]  

# Converting all the pandas object into int
inputNumber(area_total, new_area_total) 
inputNumber(quartos, new_quartos)    
inputNumber(suites, new_suites) 
inputNumber(banheiros, new_banheiros) 
inputNumber(vagas, new_vagas) 
inputNumber(area_util, new_area_util)                  

# Replacing in main df
df_anuncios['quartos'] = pd.Series(data=new_quartos)
df_anuncios['area_total'] = pd.Series(data=new_area_total)
df_anuncios['suites'] = pd.Series(data=new_suites)
df_anuncios['banheiros'] = pd.Series(data=new_banheiros)
df_anuncios['vaga'] = pd.Series(data=new_vagas)
df_anuncios['area_util'] = pd.Series(data=new_area_util)


###################### Removing bairro and zona from df_anuncios['endereco'] ######################

# Converting str into dict

new_endereco = []

for string in df_anuncios['endereco']:
    new_endereco.append(eval(string))

# Removing bairro and zona

for element in new_endereco:
    del element['bairro']
    del element['zona']

# Replacing endereco for new_endereco

df_anuncios['endereco'] = pd.Series(data=new_endereco)

###################### Splitting the column valores ######################

# valores type is str. i'll make: str -> list -> dict

valores_to_list, valores_to_dict = [],[]

for string in df_anuncios['valores']:
    valores_to_list.append(eval(string))

for _list in valores_to_list:
    valores_to_dict.append(_list[0])

# Create 4 list with its data: iptu, condominio, valor, tipo

iptu, condominio, valor, tipo = [],[],[],[]

def createListFromDict(valores_to_dict, created_list, value):
    
    for item in valores_to_dict:     
        created_list.append(item[value])
            
createListFromDict(valores_to_dict, iptu, 'iptu')
createListFromDict(valores_to_dict, condominio, 'condominio')      
createListFromDict(valores_to_dict, valor, 'valor')
createListFromDict(valores_to_dict, tipo, 'tipo') 
        
# Adding lists to df_anuncios

df_anuncios['iptu'] = iptu
df_anuncios['condominio'] = condominio
df_anuncios['valor'] = valor
df_anuncios['tipo'] = tipo

# Exclude valores from main df

df_anuncios.drop('valores', inplace=True, axis=1)

###################### removing all elements with tipo == aluguel ######################

all_tipos = df_anuncios['tipo'].drop_duplicates() # return Venda and Aluguel

list_of_index_to_exclude = []

for index in df_anuncios.index:
    
    row_tipo = df_anuncios.iloc[index]['tipo']
    
    if (row_tipo == 'Aluguel'):
        list_of_index_to_exclude.append(index)


df_anuncios.drop(list_of_index_to_exclude, axis=0, inplace=True)
df_anuncios.index = range(df_anuncios.shape[0])

###################### saving as parquet and csv ######################

df_anuncios.to_parquet('data/anuncios_vendas.csv', engine='fastparquet', compression='snappy', index=None)
df_anuncios.to_csv('data/anuncios_vendas.csv', sep=';', index=False)





