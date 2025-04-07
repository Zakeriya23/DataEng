import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline

from urllib.request import urlopen
from bs4 import BeautifulSoup
url = "http://www.hubertiming.com/results/2017GPTR10K"
html = urlopen(url)
soup = BeautifulSoup(html, 'lxml')

rows = soup.find_all('tr')

list_rows = []
for row in rows:
    cells = row.find_all('td')
    row_text = [cell.get_text(strip=True) for cell in cells]
    if row_text:
        list_rows.append(row_text)

df = pd.DataFrame(list_rows)
df.head()

df.columns = ['Place', 'Bib', 'Name', 'Gender', 'City', 'State', 'Time', 'Gun Time', 'Team']
def convert_time_to_minutes(t):
    try:
        m, s = map(int, t.split(":"))
        return m + s / 60
    except:
        return None

df['Finish_Minutes'] = df['Time'].apply(convert_time_to_minutes)
df.info()
df.describe()

print(f"Average Finish Time: {df['Finish_Minutes'].mean():.2f} minutes")

sns.boxplot(data=df, y='Finish_Minutes')
plt.title("10K Runner Finish Times")
plt.ylabel("Time (minutes)")
plt.grid(True, axis='y')
plt.show()

sns.histplot(data=df, x='Finish_Minutes', hue='Gender', kde=True, element='step')
plt.title("Finish Time Distribution by Gender")
plt.xlabel("Time (minutes)")
plt.ylabel("Runner Count")
plt.show()