import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from tqdm import tqdm

url = 'https://loft.com.br/venda/imoveis/sp/sao-paulo/jardim-america_sao-paulo_sp'

print (f'\nURL to scrape: {url}\n')
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

info = []
payload = {}
print ('\nScraping page 1')
for i in range(2, 50):    
    for link in tqdm(soup.find_all("a", class_="MuiButtonBase-root MuiCardActionArea-root jss260")):
        new_link = link.get('href')
        new_link = f'https://loft.com.br{new_link}' 
        # all_text só trata sempre a mesma frase, apesar de estarmos a correr vários url's. 
        #ver qual é o problema
        for info_text in soup.find_all("div", attrs={'class': "MuiGrid-root jss121 MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-sm-6 MuiGrid-grid-md-3 MuiGrid-grid-lg-3"}):
            info_text = info_text.text
            regex = r"(.*?)R\$ ([\d.,]+)(.*?)m²(\d+)m²?(\d+)"
            matches = re.match(regex, info_text)
            result = [matches.group(i).strip() for i in range(1, 6)]
            info.append (dict (house_links = new_link,
                               house_type = result[0],
                               house_price = result[1],
                               address = result[2],
                               area = result[3],
                               bedrooms = result[4][0],
                               parking_space = result[4][1],
                               ))
    payload['pagina'] = i
    print (f'Scraping page {i}')
    response = requests.get(url, params = payload)
    soup = BeautifulSoup(response.text, "html.parser")
    if 'Nenhum imóvel encontrado' in soup.text:
        break

houses_df = pd.DataFrame (info)
print ('Scraped info:\n\n',houses_df)
houses_df.to_csv('houses.csv')
print ('File "houses.csv" saved.')

#for row in houses_df.itertuples():
#    response = requests.get(row.house_links)
#    soup = BeautifulSoup(response.content, "html.parser")
#    if 'Este imóvel está indisponível' in soup.text:
#        pass
#    else:
#        all_text = soup.find("div", class_="MuiGrid-root MuiGrid-container")
#    print (all_text.text)