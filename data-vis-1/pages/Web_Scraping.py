import streamlit as st
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup

st.set_page_config(page_title="Web Scraping Data", 
                   page_icon="📊")

url = 'https://loft.com.br/venda/imoveis/sp/sao-paulo/jardim-america_sao-paulo_sp'

st.title ('Houses for Sale in Jardim América, São Paulo, Brasil')
st.write (f'Web scraping URL: {url}')

if st.button ('Scrape Website'):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    info_to_df = []
    payload = {}
    progress_text = 'Scraping website page 1'
    my_bar = st.progress(0, text = progress_text)
    for i in range(2, 50):
        contents = soup.find_all("div", attrs={'class': "MuiGrid-root jss121 MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-sm-6 MuiGrid-grid-md-3 MuiGrid-grid-lg-3"})
        for j, info in enumerate(contents):
            my_bar.progress (int(100 * j / len(contents)), text = progress_text)
            info_text = info.text
            regex = r"(.*?)R\$ ([\d.,]+)(.*?)m²(\d+)m²?(\d+)"
            matches = re.match(regex, info_text)
            result = [matches.group(i).strip() for i in range(1, 6)]
            new_link = info.find('a', attrs={'class':'MuiButtonBase-root MuiCardActionArea-root jss260'}).get('href')
            new_link = f'https://loft.com.br{new_link}'
            response = requests.get(new_link)
            soup = BeautifulSoup(response.text, "html.parser")
            latitude = [text for text in soup.text.split(',') if 'latitude' in text][0].split('"latitude":')[-1]
            longitude = [text for text in soup.text.split(',') if 'longitude' in text][0].split('"longitude":')[-1]
            pattern = r'-\d+\.\d+'
            match_lat = re.search(pattern, latitude)
            match_lon = re.search(pattern, longitude)
            lat = float(match_lat.group())
            lon = float(match_lon.group())
            info_to_df.append (dict (house_links = new_link,
                                     house_type = result[0],
                                     house_price = int(result[1].replace('.','')),
                                     address = result[2],
                                     area = result[3],
                                     bedrooms = result[4][0],
                                     parking_space = result[4][1],
                                     latitude = lat,
                                     longitude = lon,
                                    ))

        payload['pagina'] = i
        progress_text = f'Scraping website page {i}'
        response = requests.get(url, params = payload)
        soup = BeautifulSoup(response.text, "html.parser")
        if 'Nenhum imóvel encontrado' in soup.text:
            break

    houses_df = pd.DataFrame (info_to_df)
    houses_df.to_csv('houses.csv', index = False)
    st.success ('File "houses.csv" is ready to be saved. Use the button on the sidebar to save file locally.')
    st.header ("Downloaded data")
    st.dataframe (houses_df)

    # Save file
    @st.cache_data
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

    csv = convert_df(houses_df)

    st.sidebar.download_button(
        label = "Download data as CSV",
        data = csv,
        file_name = 'houses_df.csv',
        mime = 'text/csv',
)