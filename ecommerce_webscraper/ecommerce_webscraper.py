#################################################################################################
####################################### ENTRADA DE DADOS ########################################
#################################################################################################

# Quantidade máxima de produtos para pegar de cada página
limit = 30

# Dicionário com as URLs de busca de cada site (basta comentar a linha para remover um site da pesquisa)
urls = {
        'https://www.pichau.com.br/search?q=':'Pichau',
        'https://www.kabum.com.br/busca/':'Kabum',
        'https://www.terabyteshop.com.br/busca?str=':'TerabyteShop',
       }

# Dicionário com o nome e tipo de cada produto (basta comentar a linha para remover um produto)
search_list = {
            #    'Ar condicionado 9000 BTUs':'Ar condicionado',
            #    'Ar condicionado 18000 BTUs':'Ar condicionado',
            #    'RTX 3060':'Placa de vídeo',
            #    'RTX 4060':'Placa de vídeo',
               'RTX 4060 Ti':'Placa de vídeo',
               'RTX 4070':'Placa de vídeo',
               'RTX 5070':'Placa de vídeo',
            #    'RX 6800 XT':'Placa de vídeo',
            #    'RX 6900 XT':'Placa de vídeo',
            #    'RX 7600':'Placa de vídeo',
               'RX 7700 XT':'Placa de vídeo',
               'RX 7800 XT':'Placa de vídeo',
               'RX 9070':'Placa de vídeo',
               'Arc B570':'Placa de vídeo',
               'Arc B580':'Placa de vídeo',
            #    'i5 13400':'Processador',
            #    'i5 13600':'Processador',
            #    'i9 13900':'Processador',
            #    'i5 14600':'Processador',
            #    'i7 13700':'Processador',
               'i7 14700':'Processador',
               'i9 14900':'Processador',
            #    'Core Ultra 5 235K':'Processador',
            #    'Core Ultra 5 245K':'Processador',
            #    'Core Ultra 7 255K':'Processador',
            #    'Core Ultra 7 265K':'Processador',
            #    'Core Ultra 9 275K':'Processador',
            #    'Core Ultra 9 285K':'Processador',
               'Ryzen 7 5700X3D':'Processador',
               'Ryzen 5 7600':'Processador',
               'Ryzen 7 7700':'Processador',
               'Ryzen 7 7800X3D':'Processador',
               'Ryzen 9 7900X':'Processador',
               'Ryzen 9 7950X':'Processador',
            #    'Ryzen 5 8600G':'Processador',
            #    'Ryzen 7 8700G':'Processador',
               'Ryzen 5 9600' : 'Processador',
               'Ryzen 7 9700' : 'Processador',
               'Ryzen 7 9800X3D':'Processador',
               'Ryzen 9 9900X' : 'Processador',
               'Ryzen 9 9950X' : 'Processador',
            #    'A620':'Placa mãe',
               'B650':'Placa mãe',
               'X670':'Placa mãe',
               'X870':'Placa mãe',
            #    'B760':'Placa mãe',
            #    'Z690':'Placa mãe',
            #    'Z790':'Placa mãe',
            #    'B860':'Placa mãe',
            #    'Z890':'Placa mãe',
               'DDR5':'Memória RAM',
               'SSD 1TB':'SSD',
              }

# Ignorar produtos que possuam essas palavras-chave em seus nomes
keywords_to_ignore = [
                      'notebook',
                      'laptop',
                      'all in one',
                      'all-in-one',
                      'cabo',
                      'lenovo',
                      'thinkcentre',
                      'desktop hp',
                      'gabinete',
                      'transparente',
                      'radiador',
                      'ventilador',
                      'ventoinha',
                      'servidor',
                      'pc case',
                      'english edition',
                      'estação',
                      'station'
                     ]

#################################################################################################
#################################### FIM DA ENTRADA DE DADOS ####################################
#################################################################################################

import numpy as np
import pandas as pd
import re
import warnings
import os
import sys
import concurrent.futures
import threading
from random import uniform
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from time import sleep
from urllib.parse import urljoin, urlparse
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import scoped_session, sessionmaker
warnings.filterwarnings('ignore')

# Listas para armazenar os resultados
product_search_list = []
product_name_list = []
product_type_list = []
price_list = []
site_list = []
url_list = []

# Obtém a data e hora atuais
now = pd.Timestamp.now().floor('s')

# Procura por padrões como 'c32', 'c30', etc., no nome do produto
def extract_cas_latency(product_name):
    match = re.search(r'c(\d+)', product_name, re.IGNORECASE)
    if match:
        # Retorna o valor numérico da latência CAS
        return int(match.group(1))
    return None

# Opções do navegador
options = Options()
options.add_argument('--headless') # Executa o navegador em segundo plano, sem abrir uma janela # --headless=old
options.add_argument('window-size=400,800')
options.add_argument('--incognito')
options.add_argument('--disable-gpu')
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
options.add_argument('--disable-web-security')
options.add_argument('--log-level=3')
options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
options.add_experimental_option('useAutomationExtension', False)

# A lógica do scraping foi encapsulada para permitir múltiplas execuções em paralelo
# Esta função recebe o item pesquisado e a url de busca, e utiliza as variáveis globais definidas anteriormente
def scrape_data(search, url, timeout=45):
    
    def target(search, url):
        # Tenta executar o bloco de código a seguir. Caso aconteça algum erro, ele é mostrado e a execução continua
        try:
            
            # Salva o nome original do item pesquisado
            search_original = search

            # Altera a busca para memória
            if search_original == 'DDR5':
                search = 'Memoria DDR5'
            # Altera a busca para SSD
            elif 'SSD' in search_original:
                search = 'SSD 1TB'

            # Obtém o nome do site do dicionário, através da URL de busca
            site = urls[url]
            
            # Abre o navegador e aguarda a página carregar + um intervalo aleatório de 2 a 4 segundos
            browser = webdriver.Chrome(options=options)
            browser.get(url + search)
            sleep(uniform(2, 4))
            
            # Obtém o HTML da página
            page_content = browser.page_source
            page = BeautifulSoup(page_content, 'html.parser')
            
            # Fecha o navegador
            browser.quit()
            
            # Encontra os nomes dos 'limit' primeiros produtos, os preços e os links deles
            # A lógica é quase a mesma para todos os sites
            if site == 'Pichau':
                product_names = page.find_all('h2', class_=re.compile(r'MuiTypography-root.*MuiTypography-h6'), limit=limit)
                card_contents = page.find_all('div', class_=re.compile(r'MuiCardContent-root'), limit=limit)
                prices = [content.find('div', text=re.compile(r'R\$')) for content in card_contents]
                product_urls = page.find_all('a', {'data-cy': 'list-product'}, limit=limit)
            
            elif site == 'Kabum':
                product_names = page.find_all('span', class_=re.compile('nameCard'), limit=limit)
                prices = page.find_all('span', class_=re.compile('priceCard'), limit=limit)
                product_urls = page.find_all('a', class_=re.compile('productLink'), limit=limit)
            
            elif site == 'TerabyteShop':
                product_names = page.find_all('a', class_=re.compile('product-item__name'), limit=limit)
                prices = page.find_all('div', class_=re.compile('product-item__new-price'), limit=limit)
                product_urls = product_names
            
            # Se for encontrado algum produto, executa a extração de dados dos objetos definidos anteriormente
            if product_names and prices and product_urls:
                
                # Trata a busca, removendo espaços e pontuação
                search = re.sub(r'[\s\W]', '', search).lower()

                for product, price, product_url in zip(product_names, prices, product_urls):
                    
                    # Na TerabyteShop, a URL completa do produto (domínio + endereço específico) já está disponível
                    if site == 'TerabyteShop':

                        # Obtém e trata o nome do produto, removendo espaços e pontuação
                        product_name_original = product.get_text(strip=True)
                        product_name = re.sub(r'[\s\W]', '', product_name_original).lower()
                        full_url = product.get('href')
                        
                        # Verifica se o item é uma placa mãe e, se for, seleciona apenas as que suportam DDR5,
                        # e ignora itens que possuam as palavras-chave proibidas no nome
                        if search_list[search_original] == 'Placa mãe':
                            if search not in product_name \
                            or any(keyword.replace(' ', '') in product_name for keyword in keywords_to_ignore):
                                continue
                        
                        # Verifica se a busca é por memória RAM e procura por DDR5 e C32 ou inferior no nome
                        elif search_list[search_original] == 'Memória RAM':
                            product_name_adjusted = product_name.replace('dd5', 'ddr5')
                            product_name_adjusted = re.sub(r'cl(?=\d+)', 'c', product_name_adjusted)
                            cas_latency = extract_cas_latency(product_name_adjusted)
                            if 'ddr5' not in product_name_adjusted or cas_latency is None or cas_latency > 32 \
                            or any(keyword.replace(' ', '') in product_name for keyword in keywords_to_ignore):
                                continue
                        
                        # Verifica se a busca é por SSD
                        elif search_list[search_original] == 'SSD':
                            if 'ssd' not in product_name or '1tb' not in product_name \
                            or any(keyword.replace(' ', '') in product_name for keyword in keywords_to_ignore):
                                continue
                        
                        # Verifica se a busca é por ar condicionado
                        elif search_list[search_original] == 'Ar condicionado':
                            if 'arcondicionado' not in product_name or re.search(r'\d+', search).group() + 'btu' not in product_name:
                                continue

                        # Ignora itens que possuam as palavras-chave proibidas no nome
                        else:
                            if search not in product_name \
                            or any(keyword.replace(' ', '') in product_name for keyword in keywords_to_ignore):
                                continue
                        
                        # Tenta converter o valor do preço para um número. Caso não seja possível, atribui NaN ao preço
                        try:
                            price_number = re.sub(r'[^\d,]', '', price.get_text(strip=True)).replace(',', '.')
                            price_number = float(price_number)
                        except Exception as e:
                            # print(e)
                            price_number = np.nan
                    
                    else:
                        
                        # Obtém e trata o nome do produto, removendo espaços e pontuação
                        product_name_original = product.get_text(strip=True)
                        product_name = re.sub(r'[\s\W]', '', product_name_original).lower()
                        
                        # Verifica se o item é uma placa mãe e, se for, seleciona apenas as que suportam DDR5
                        # e ignora itens que possuam as palavras-chave proibidas no nome
                        if search_list[search_original] == 'Placa mãe':
                            if search not in product_name \
                            or 'ddr5' not in product_name.replace('dd5', 'ddr5') \
                            or any(keyword.replace(' ', '') in product_name for keyword in keywords_to_ignore):
                                continue
                        
                        # Verifica se a busca é por memória RAM e procura por DDR5 e C32 ou inferior no nome
                        elif search_list[search_original] == 'Memória RAM':
                            product_name_adjusted = product_name.replace('dd5', 'ddr5')
                            product_name_adjusted = re.sub(r'cl(?=\d+)', 'c', product_name_adjusted)
                            cas_latency = extract_cas_latency(product_name_adjusted)
                            if 'ddr5' not in product_name_adjusted or cas_latency is None or cas_latency > 32 \
                            or any(keyword.replace(' ', '') in product_name for keyword in keywords_to_ignore):
                                continue
                        
                        # Verifica se a busca é por SSD
                        elif search_list[search_original] == 'SSD':
                            if 'ssd' not in product_name or '1tb' not in product_name \
                            or any(keyword.replace(' ', '') in product_name for keyword in keywords_to_ignore):
                                continue
                        
                        # Verifica se a busca é por ar condicionado
                        elif search_list[search_original] == 'Ar condicionado':
                            if 'arcondicionado' not in product_name or re.search(r'\d+', search).group() + 'btu' not in product_name:
                                continue

                        # Ignora itens que possuam as palavras-chave proibidas no nome
                        else:
                            if search not in product_name \
                            or any(keyword.replace(' ', '') in product_name for keyword in keywords_to_ignore):
                                continue
                        
                        # Tenta converter o valor do preço para um número. Caso não seja possível, atribui NaN ao preço
                        try:
                            price_number = re.sub(r'[^\d,]', '', price.get_text(strip=True)).replace(',', '.')
                            price_number = float(price_number)
                        except Exception as e:
                            # print(e, site, search_original)
                            price_number = np.nan
                        
                        # Junta a URL base do site com a URL do produto
                        parsed_url = urlparse(url)
                        base_url = f'{parsed_url.scheme}://{parsed_url.netloc}/'
                        full_url = urljoin(base_url, product_url.get('href'))
                    
                    # Adiciona os resultados às listas
                    product_search_list.append(search_original)
                    product_name_list.append(product_name_original)
                    product_type_list.append(search_list[search_original])
                    price_list.append(price_number)
                    site_list.append(site)
                    url_list.append(full_url)
            
            # Exibe uma mensagem de erro se nenhum produto for encontrado, e fecha o navegador
            else:
                pass
        
        # Exibe uma mensagem de erro com a linha do erro se ocorrer alguma exceção
        except Exception as e:
            # Tenta fechar o navegador, se ele ainda estiver aberto
            try:
                browser.quit()
            except:
                pass
    
    # Inicia a thread para executar a função
    exception = [None]

    thread = threading.Thread(target=target, args=(search, url))
    thread.start()
    thread.join(timeout) # Aguarda a thread com o limite de tempo definido

    if thread.is_alive():
        raise TimeoutError(f'Timeout de {timeout} segundos atingido durante o scraping no site {urls[url]} para o item {search}.')
    
    if exception[0]:
        raise exception[0]

# Paralelização do scraping: serão executados, em paralelo, um número de tarefas proporcional ao número de threads da CPU
with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
    
    # Prepara as tarefas
    tasks = {executor.submit(scrape_data, search, url): (search, url) for search in search_list for url in urls}

    # Processa as tarefas com uma barra de progresso
    for future in tqdm(concurrent.futures.as_completed(tasks), total=len(tasks), desc='Progresso'):
        try:
            result = future.result()
        
        # Exibe uma mensagem de erro com a linha do erro se ocorrer alguma exceção
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # print(f'Mensagem de erro: {e}')
            # print(f'Número da linha do erro: {exc_traceback.tb_lineno}')

# Processa os resultados, adicionando-os a um dataframe
df = pd.DataFrame(
                  {
                   'nm_produto':product_search_list,
                   'ds_produto':product_name_list,
                   'nu_preco':price_list,
                   'nm_site':site_list,
                   'ds_url':url_list,
                   'nm_tipo_produto':product_type_list,
                   'dh_acesso':np.full(len(product_name_list), now)
                  }
                 )

# Altera alguns produtos para sua versão alternativa, se existir
def corrigir_produtos(df):
    # GPUs - encontra o termo na descrição
    gpus = {
        '4060ti': ('4060 ti', 'RTX 4060 Ti'),
        '4070ti': ('4070 ti', 'RTX 4070 Ti'),
        '4070super': ('4070 super', 'RTX 4070 Super'),
        '4070ti super': ('4070 ti super', 'RTX 4070 Ti Super'),
        '5070ti': ('5070 ti', 'RTX 5070 Ti'),
        '9070xt': ('9070 xt', 'RX 9070 XT')
    }
    for compacto, (expandido, nome_final) in gpus.items():
        mask = (df['ds_produto']
                  .str.lower()
                  .str.replace(compacto, expandido, regex=False)
                  .str.contains(expandido))
        df.loc[mask, 'nm_produto'] = nome_final

    # CPUs Ryzen - exige descrição + nome base já preenchido
    ryzen = {
        '7600x': ('Ryzen 5 7600',  'Ryzen 5 7600X'),
        '7700x': ('Ryzen 7 7700',  'Ryzen 7 7700X'),
        '7900x3d': ('Ryzen 9 7900', 'Ryzen 9 7900X3D'),
        '7950x3d': ('Ryzen 9 7950', 'Ryzen 9 7950X3D'),
        '9600x': ('Ryzen 5 9600',  'Ryzen 5 9600X'),
        '9700x': ('Ryzen 7 9700',  'Ryzen 7 9700X'),
        '9900x3d': ('Ryzen 9 9900', 'Ryzen 9 9900X3D'),
        '9950x3d': ('Ryzen 9 9950', 'Ryzen 9 9950X3D')
    }
    for termo, (nome_base, nome_final) in ryzen.items():
        mask = (df['ds_produto'].str.lower().str.contains(termo)) & \
               (df['nm_produto'].str.contains(nome_base))
        df.loc[mask, 'nm_produto'] = nome_final

    return df

df = corrigir_produtos(df)

# Altera o tipo do produto se existirem as palavras 'pc gamer' ou 'computador' no nome
mask_pc = df['ds_produto'].str.lower().str.contains(r'\bpc\b|computador|workstation', regex=True)
df.loc[mask_pc, 'nm_tipo_produto'] = 'PC'

# Altera o tipo do produto se existir 'kit upgrade' no nome
mask_upgrade = df['ds_produto'].str.lower().str.contains(r'\bkit\b', regex=True)
df.loc[mask_upgrade, 'nm_tipo_produto'] = 'Kit upgrade'

# Altera o nome do produto se for um SSD SATA
mask_sata = (df['ds_produto'].str.lower().str.contains('sata')) & (df['nm_produto'].str.contains('SSD 1TB'))
df.loc[mask_sata, 'nm_produto'] = 'SSD 1TB (SATA)'

# Adiciona uma coluna de data
df.insert(6, 'dt_acesso', now.date())
df_now = df.copy()

# Verifica se o arquivo já existe no caminho de saída, e cria um backup antes de atualizá-lo
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
path = os.path.join(base_dir, 'data', 'processed', 'price_history.xlsx')
if os.path.isfile(path):
    backup_path = path.replace('.xlsx', '_bak.xlsx').replace('processed', 'backup')
    
    if os.path.isfile(backup_path):
        os.remove(backup_path)
    os.rename(path, backup_path)
    
    # Concatena o dataframe criado com o dataframe já existente
    try:
        df_old = pd.read_excel(backup_path)
    except:
        df_old = pd.DataFrame()
    df = pd.concat([df_old, df], ignore_index=True)
    df['dt_acesso'] = pd.to_datetime(df['dt_acesso']).dt.date

# Organiza os registros e salva o arquivo
df.drop_duplicates(subset=df.drop(['ds_url', 'dh_acesso'], axis=1).columns, inplace=True)
df.sort_values(by=['nm_produto', 'dt_acesso', 'nu_preco'], inplace=True, ignore_index=True)
df.to_excel(path, index=False)
print('Excel salvo!')

# Credenciais do banco
user = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
conn_string = f'mysql+pymysql://{user}:{password}@localhost:3307/mba_dsa'

# Conecta no SQL e obtém os dados do dia
engine = create_engine(conn_string)
db = scoped_session(sessionmaker(bind=engine))
select_query = f"SELECT * FROM mba_dsa.hardware_price_history WHERE dt_acesso = '{now.date()}'"
print(select_query)
df_sql = pd.read_sql(select_query, engine)

# Concatena e organiza os registros, eliminando duplicatas
df_updated = pd.concat([df_now, df_sql], axis=0, ignore_index=True)
df_updated['dt_acesso'] = pd.to_datetime(df_updated['dt_acesso']).dt.date
df_updated.drop_duplicates(subset=df_updated.drop(['ds_url', 'dh_acesso'], axis=1).columns, inplace=True)
df_updated.sort_values(by=['nm_produto', 'dt_acesso', 'nu_preco'], inplace=True, ignore_index=True)

# Remove os registros do dia para evitar inserir dados que já existem
delete_query = f"DELETE FROM mba_dsa.hardware_price_history WHERE dt_acesso = '{now.date()}'"
print(delete_query)
db.execute(text(delete_query))
db.commit()

# Salva os novos dados no SQL e fecha a conexão
df_updated.to_sql('hardware_price_history', engine, index=False, if_exists='append')
db.close()
engine.dispose()

# Exibe a duração da execução do script
runtime = (pd.Timestamp.now() - now) / pd.Timedelta(minutes=1)
print(f'Concluído! Runtime: {round(runtime, 2)} minutos.')

# Exibe os resultados da pesquisa
print()
print(df[df['dh_acesso'] == now][['nm_site', 'nm_produto', 'ds_produto', 'nu_preco', 'nm_tipo_produto']])

sys.exit()

##### TODO Colocar o input em um arquivo separado
##### Ideia: Previsão de preços usando a série temporal
##### Ideia: Analisar diferença do preço antes e depois do lançamento da nova linha