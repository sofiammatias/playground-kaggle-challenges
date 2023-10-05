import pandas as pd

output_dir = r'C:\Users\xufia\OneDrive\Imagens\Videos\_Videos finais - EN\images'
df = pd.read_csv(f'{output_dir}\contents.csv', usecols=['image_name', 'secs', 'content'])
print (df.tail(15))

# clean df
for i in range(len(df) - 1):
    if (i > 1) and (df.iloc[i].content == df.iloc[i-1].content):
        print (i)
        df.drop (labels = i-1, axis = 0, inplace = True)

print (df.head(15))
