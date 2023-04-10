import requests
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://loft.com.br/venda/imoveis/sp/sao-paulo/jardim-america_sao-paulo_sp'

print (f'\nURL to scrape: {url}\n')
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

links = []
payload = {}
print ('\nScraping page 1')
for i in range(2, 55):
    for link in soup.find_all("a", class_="MuiButtonBase-root MuiCardActionArea-root jss260"):
        new_link = link.get('href')
        new_link = f'https://loft.com.br/venda{new_link}'
        links.append (dict (house_links = new_link))
    payload['pagina'] = i
    print ('\nScraping page', i)
    response = requests.get(url, params = payload)
    response2 = requests.get(f'https://loft.com.br/venda/imoveis/sp/sao-paulo/jardim-america_sao-paulo_sp?pagina={i}')
    print (response.status_code)
    print (response2.status_code)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
    else:
        break

houses_df = pd.DataFrame (links)
print (houses_df)

for link in links:
    response = requests.get(link)
    soup = BeautifulSoup(response.content, "html.parser")
    house_type = soup.find("a", class_="jss213")
    print (house_type)

#    title = movie.find("h3").find("a").string
#    duration = int(movie.find("span", class_="runtime").string.strip(' min'))
#    movies.append({'title': title, 'duration': duration})
