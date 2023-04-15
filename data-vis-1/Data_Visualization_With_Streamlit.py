import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
#import requests
#import urllib.parse

st.set_page_config(
    page_title="Data Visualization With Streamlit",
    page_icon="📈",
)

types = {'latitude': np.float32, 'longitude': np.float32} 
url = 'https://loft.com.br/venda/imoveis/sp/sao-paulo/jardim-america_sao-paulo_sp'

st.title ('Houses for Sale in Jardim América, São Paulo, Brasil')
st.info('Download a "houses_df.csv" file using the upload button on the sidebar.')
uploaded_file = st.sidebar.file_uploader("Choose a file 'houses_df.csv'")
if uploaded_file is not None:
    # Can be used wherever a "file-like" object is accepted:
    df = pd.read_csv(uploaded_file)
    for col in df.columns:
        if 'Unnamed' in col:
            df = df.drop (columns = col)
            break

    options = st.multiselect(
        'Select one or more house types:',
        df.house_type.unique(),
        [df.house_type.unique()[0], df.house_type.unique()[1]])

    df_selected = df[df['house_type'].isin(options)]

    df_nums = df_selected.drop(columns = ['house_links', 'address', 'latitude', 'longitude'])
    df_nums = df_nums.groupby('house_type').agg(['mean', 'max', 'min']).reset_index()
    df_nums.columns = ['House Type', 'House Price Average', 'House Price Max', 'House Price Min',
                      'Area Average', 'Area Max', 'Area Min', 
                       'Bedrooms Average', 'Bedrooms Max', 'Bedrooms Min', 
                       'Parking Space Average', 'Parking Space Max', 'Parking Space Min']
    df_nums['Number of Houses'] = df_selected.groupby('house_type').count().reset_index()['house_links']
    df_nums['House Price per m2'] = df_nums['House Price Average'] / df_nums['Area Average']

    # Define a layer to display on a map
    layer = pdk.Layer(
        "ScatterplotLayer",
        df_selected,
        pickable=True,
        opacity=0.8,
        stroked=True,
        filled=True,
        auto_highlight=True,
        extruded=True,
        elevation_scale=0.1,
        elevation_range=[0, 1],
        radius = 1,
        radius_scale=1,
        radius_min_pixels=1,
        radius_max_pixels=100,
        line_width_min_pixels=1,
        get_position=['longitude', 'latitude'],
        get_radius=8,
        get_fill_color=[102, 179, 72],
        get_line_color=[0, 0, 0],
        )

    # Set the viewport location
    view_state = pdk.ViewState(latitude = -23.562799, longitude = -46.663020, zoom = 14, bearing = 0, pitch = 0)

    # Render
    r = pdk.Deck(layers = [layer], 
                 initial_view_state = view_state,
                 map_style = 'road', 
                 tooltip={"text": "{name}\n{address}"})
    st.pydeck_chart(r, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Number of Houses", value = f'{df_nums["Number of Houses"].sum():,}')
        st.metric(label="Max. House Price", value = f'${df_nums["House Price Max"].max():,}')
    with col2:
        st.metric(label="Max. Area", value = f'{df_nums["Area Max"].max():,} m2')
        st.metric(label="Min. House Price", value = f'${df_nums["House Price Min"].min():,}')
    with col3:
        st.metric(label="Min. Area", value = f'{df_nums["Area Min"].min():,} m2')

    tab1, tab2 = st.tabs(["📈 Charts", "🗃 Data"])

    col1, col2 = tab1.columns(2)

    with col1:
        st.bar_chart(df_nums, y = 'House Price Average', x = 'House Type')
        st.bar_chart(df_nums, y = 'House Price per m2', x = 'House Type')

    with col2:
        st.bar_chart(df_nums, y = 'Number of Houses', x = 'House Type')
        st.bar_chart(df_nums, y = 'Area Average', x = 'House Type')

    tab2.dataframe(df_nums.set_index('House Type').T)