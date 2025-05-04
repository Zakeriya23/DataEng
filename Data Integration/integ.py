import pandas as pd
from us_state_abbrev import abbrev_to_us_state
import seaborn as sns
import matplotlib.pyplot as plt

#1. Read the CSVs
cases_df   = pd.read_csv('covid_confirmed_usafacts.csv')
deaths_df  = pd.read_csv('covid_deaths_usafacts.csv')
census_df  = pd.read_csv('acs2017_county_data.csv')

#Trim to only the needed columns
cases_df   = cases_df[['County Name', 'State', '2023-07-23']]
deaths_df  = deaths_df[['County Name', 'State', '2023-07-23']]
census_df  = census_df[['County', 'State', 'TotalPop', 'IncomePerCap', 'Poverty', 'Unemployment']]

#2. Show column headers
print('Cases columns:  ', cases_df.columns.tolist())
print('Deaths_df columns: ', deaths_df.columns.tolist())
print('Census_df columns: ', census_df.columns.tolist())


# 3. Remove trailing spaces from county names
cases_df.loc[:, 'County Name']  = cases_df['County Name'].str.rstrip()
deaths_df.loc[:, 'County Name'] = deaths_df['County Name'].str.rstrip()

print('Any trailing spaces in cases_df? ', cases_df['County Name'].str.endswith(' ').any())
print('Any trailing spaces in deaths_df?', deaths_df['County Name'].str.endswith(' ').any())

wash_cases  = cases_df[cases_df['County Name'] == 'Washington County']
wash_deaths = deaths_df[deaths_df['County Name'] == 'Washington County']

print('Washington counties in cases_df: ', len(wash_cases))
print('Washington counties in deaths_df:', len(wash_deaths))

before_case= len(cases_df)
before_death= len(deaths_df)


# 4. Remove statewide unallocated records
cases_df   = cases_df[cases_df['County Name'] != 'Statewide Unallocated']
deaths_df  = deaths_df[deaths_df['County Name'] != 'Statewide Unallocated']

print('Before/after cases_df: ', before_case, "->",len(cases_df))
print('Before/after deaths_df:',before_death, "->", len(deaths_df))

# 5. map state abbreviations
cases_df['State']  = cases_df['State'].map(abbrev_to_us_state)
deaths_df['State'] = deaths_df['State'].map(abbrev_to_us_state)
 
print("\n", cases_df.head())

# 6. dict
cases_df['key']   = cases_df['County Name'] + ', ' + cases_df['State']
deaths_df['key']  = deaths_df['County Name'] + ', ' + deaths_df['State']
census_df['key']  = census_df['County']      + ', ' + census_df['State']

cases_df.set_index('key', inplace=True)
deaths_df.set_index('key', inplace=True)
census_df.set_index('key', inplace=True)

print("\n", census_df.head())

# 7. rename
cases_df.rename(columns={'2023-07-23': 'Cases'}, inplace=True)
deaths_df.rename(columns={'2023-07-23': 'Deaths'}, inplace=True)

print('\n\ncases_df columns:', cases_df.columns.values.tolist())
print('deaths_df columns:', deaths_df.columns.values.tolist())


# 8. Integration
join_df = (
    cases_df[['Cases']]
    .join(deaths_df[['Deaths']], how='inner')
    .join(census_df[['TotalPop','IncomePerCap','Poverty','Unemployment']], how='inner')
)


join_df['CasesPerCap']  = join_df['Cases']  / join_df['TotalPop']
join_df['DeathsPerCap'] = join_df['Deaths'] / join_df['TotalPop']

print('\n\nRows in join_df:', len(join_df))

# 9. Correlation matrix 
corr_matrix = join_df.corr()
print("\nCorrelation Matrix:\n",corr_matrix)

# 10. Heatmap
correlation_matrix = join_df.corr()

plt.figure(figsize=(10, 8))
sns.heatmap(
    correlation_matrix,
    annot=True,
    cmap='coolwarm',
    fmt='.2f',
    linewidths=0.5
)
plt.title('\n\n\nCorrelation Matrix Heatmap')
plt.show()