import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

from urllib.request import urlopen
from bs4 import BeautifulSoup
url = "http://www.hubertiming.com/results/2017GPTR10K"
html = urlopen(url)
soup = BeautifulSoup(html, 'lxml')

print(soup.title)
print(soup.get_text()[:500])  # Print the first 500 characters of text

rows = soup.find_all('tr')
print(rows[:10])  # Print first 10 rows


# Extract and clean table rows
list_rows = []
for row in rows:
    cells = row.find_all('td')
    str_cells = str(cells)
    clean = re.compile('<.*?>')
    clean2 = re.sub(clean, '', str_cells)
    list_rows.append(clean2)

# Convert to DataFrame and clean columns
df = pd.DataFrame(list_rows)
df1 = df[0].str.split(',', expand=True)
df1[0] = df1[0].str.strip('[')

# Extract column headers
col_labels = soup.find_all('th')
col_str = str(col_labels)
all_header = []
cleantext2 = BeautifulSoup(col_str, "lxml").get_text()
all_header.append(cleantext2)

df2 = pd.DataFrame(all_header)
df3 = df2[0].str.split(',', expand=True)

# Combine headers and data
frames = [df3, df1]
df4 = pd.concat(frames)
df5 = df4.rename(columns=df4.iloc[0])

# Clean up dataframe
df6 = df5.dropna(axis=0, how='any')
df7 = df6.drop(df6.index[0])  # drop duplicate header row
df7.rename(columns={'[Place': 'Place', ' Team]': 'Team'}, inplace=True)
df7['Team'] = df7['Team'].str.strip(']')

# 🔧 Strip whitespace from all column names (important fix)
df7.columns = df7.columns.str.strip()

# Convert 'Chip Time' to minutes
time_list = df7[' Chip Time'].tolist()

time_mins = []
for i in time_list:
    h, m, s = i.split(':')
    math = (int(h) * 3600 + int(m) * 60 + int(s)) / 60
    time_mins.append(math)

df7['Runner_mins'] = time_mins

# Show result
df7.head()
df7.describe(include=[np.number])


from pylab import rcParams
rcParams['figure.figsize'] = 15, 5

df7.boxplot(column='Runner_mins')
plt.grid(True, axis='y')
plt.ylabel('Chip Time')
plt.xticks([1], ['Runners'])

x = df7['Runner_mins']
sns.histplot(x, kde=True, bins=25, color='m', edgecolor='black')
plt.title("Distribution of Runner Chip Times")
plt.show()

f_times = df7[df7[' Gender'] == ' F']['Runner_mins']
m_times = df7[df7[' Gender'] == ' M']['Runner_mins']

sns.histplot(f_times, kde=True, color='hotpink', label='Female', edgecolor='black')
sns.histplot(m_times, kde=True, color='blue', label='Male', edgecolor='black')
plt.legend()
plt.title("Gender Comparison of Runner Times")
plt.show()


g_stats = df7.groupby(" Gender", as_index=True).describe()
print(g_stats)

df7.boxplot(column='Runner_mins', by=' Gender')
plt.ylabel('Chip Time')
plt.title("")
plt.suptitle("")
