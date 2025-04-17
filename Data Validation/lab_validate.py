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
df_valid = df[mask].copy()
dropped_name = original_count - len(df_valid)
print(f"Dropped {dropped_name} rows because they had no name, or didn't have first and last name.")

# 2. Limit Assertion
before_tenure = len(df_valid)
df_valid['hire_date'] = pd.to_datetime(df_valid['hire_date'], errors='coerce')
today = pd.to_datetime('today')
df_valid['tenure_years'] = (today - df_valid['hire_date']).dt.days / 365.25

tenure_mask = df_valid['tenure_years'].between(0, 10)
df_valid = df_valid[tenure_mask].copy()
dropped_tenure = before_tenure - len(df_valid)
print(f"Dropped {dropped_tenure} rows for tenure outside 0–10 years.")

# 3. Intra-record Assertion
before_birth_hire = len(df_valid)
df_valid['birth_date'] = pd.to_datetime(df_valid['birth_date'], errors='coerce')
birth_hire_mask = df_valid['birth_date'] < df_valid['hire_date']
df_valid = df_valid[birth_hire_mask].copy()
dropped_birth_hire = before_birth_hire - len(df_valid)
print(f"Dropped {dropped_birth_hire} rows where birth_date is not before hire_date.")

# 4. Inter-record Assertion
before_manager = len(df_valid)
manager_mask = (
    df_valid['reports_to'].notna() &
    df_valid['reports_to'].isin(df_valid['eid'])
)
df_valid = df_valid[manager_mask].copy()
dropped_manager = before_manager - len(df_valid)
print(f"Dropped {dropped_manager} rows whose reports_to isn’t a known employee.")

# 5.  Summary Assertion
before_city = len(df_valid)
# count employees per city
city_counts = df_valid['city'].value_counts()

# keep only cities with count ≥2
good_cities = city_counts[city_counts >= 2].index
df_valid = df_valid[df_valid['city'].isin(good_cities)].copy()
dropped_city = before_city - len(df_valid)
print(f"Dropped {dropped_city} rows because their city had fewer than 2 employees.")

# 6. Statistical Assertion:
salaries = df_valid['salary'].dropna()

# Shapiro–Wilk normality test
stat, pval = shapiro(salaries)
print(f"Shapiro–Wilk test p-value: {pval:.3f}")
if pval < 0.05:
    print("→ Reject normality (p < 0.05).")
else:
    print("→ Cannot reject normality (p ≥ 0.05).")

# save to new csv
df_valid.to_csv('validated.csv', index=False)

# Plot Statistical Assertion
df = pd.read_csv('validated.csv')
plt.figure()
plt.hist(df['salary'], bins=20)
plt.title('Salary Distribution')
plt.xlabel('Salary')
plt.ylabel('Frequency')
plt.show()