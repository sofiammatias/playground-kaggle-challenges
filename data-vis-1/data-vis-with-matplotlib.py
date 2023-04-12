import streamlit as st
import mpld3
import streamlit.components.v1 as components
import matplotlib.pyplot as plt

#create your figure and get the figure object returned
fig, ((ax1, ax2, ax3), (ax4, ax5, ax6)) = plt.subplots(2, 3)
ax1.plot([1, 2, 3, 4, 5]) 
ax2.plot([5, 4, 3, 2, 1]) 

st.pyplot(fig)