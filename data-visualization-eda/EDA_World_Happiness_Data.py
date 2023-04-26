import pandas as pd

#df = pd.read_excel (r'C:\Users\xufia\OneDrive\Ambiente de Trabalho\DataForFigure2_1WHR2021C2.xls')
df = pd.read_excel (r'C:\Users\xufia\OneDrive\Ambiente de Trabalho\DataForFigure2_1WHR2021C2.xls')
print (df.info())
print (df.iloc[-5:,[0,1,5,6, 18]])
