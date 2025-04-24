import scipy.stats as stats
import pandas as pd
import matplotlib.pyplot as plt

# Load your data 
df = pd.read_csv('employees.csv')

# 1. Existence Assertion
original_count = len(df)
mask = (
    df['name'].notna() &
    (df['name'].str.strip() != '') &
    (df['name'].str.strip().str.split().str.len() >= 2)
)
df_valid_existence = df[mask].copy()
dropped_name = original_count - len(df_valid_existence)
print(f"Dropped {dropped_name} rows because they had no name, or didn't have first and last name.")

# 2. Limit Assertion
df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')
today = pd.to_datetime('today')
df['tenure_years'] = (today - df['hire_date']).dt.days / 365.25

before_tenure = len(df)
tenure_mask = df['tenure_years'].between(0, 10)
df_valid_tenure = df[tenure_mask].copy()
dropped_tenure = before_tenure - len(df_valid_tenure)
print(f"Dropped {dropped_tenure} rows for tenure outside 0–10 years.")

# 3. Intra-record Assertion
df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
before_birth_hire = len(df)
birth_hire_mask = df['birth_date'] < df['hire_date']
df_valid_birth_hire = df[birth_hire_mask].copy()
dropped_birth_hire = before_birth_hire - len(df_valid_birth_hire)
print(f"Dropped {dropped_birth_hire} rows where birth_date is not before hire_date.")

# 4. Inter-record Assertion
before_manager = len(df)
manager_mask = (
    df['reports_to'].notna() &
    df['reports_to'].isin(df['eid'])
)
df_valid_manager = df[manager_mask].copy()
dropped_manager = before_manager - len(df_valid_manager)
print(f"Dropped {dropped_manager} rows whose reports_to isn’t a known employee.")

# 5.  Summary Assertion
before_city = len(df)
# count employees per city
city_counts = df['city'].value_counts()

# keep only cities with count ≥2
good_cities = city_counts[city_counts >= 2].index
df_valid_city = df[df['city'].isin(good_cities)].copy()
dropped_city = before_city - len(df_valid_city)
print(f"Dropped {dropped_city} rows because their city had fewer than 2 employees.")

# 6. Statistical Assertion:
salaries = df['salary'].dropna()

# Shapiro–Wilk normality test
stat, pval = stats.shapiro(salaries)
print(f"Shapiro–Wilk test p-value: {pval:.3f}")
if pval < 0.05:
    print("→ Reject normality (p < 0.05).")
else:
    print("→ Cannot reject normality (p ≥ 0.05).")

# save to new csv
df_valid_city.to_csv('validated.csv', index=False)

# Plot Statistical Assertion
df = pd.read_csv('validated.csv')
plt.figure()
plt.hist(df['salary'], bins=20)
plt.title('Salary Distribution')
plt.xlabel('Salary')
plt.ylabel('Frequency')
plt.show()
