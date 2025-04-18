import pandas as pd

usecols = [
    'EVENT_NO_TRIP',
    'OPD_DATE',
    'VEHICLE_ID',
    'METERS',
    'ACT_TIME',
    'GPS_LONGITUDE',
    'GPS_LATITUDE'
]

df = pd.read_csv('bc_trip259172515_230215.csv', usecols=usecols)

print('breadcrumb records:', len(df))

df['TIMESTAMP'] = df.apply(
    lambda r: pd.to_datetime(r['OPD_DATE'], format="%d%b%Y:%H:%M:%S")
              + pd.to_timedelta(r['ACT_TIME'], unit='s'),
    axis=1
)

df = df.drop(columns=['OPD_DATE', 'ACT_TIME'])

df['dMETERS'] = df['METERS'].diff()
df['dTIME'] = df['TIMESTAMP'].diff().dt.total_seconds()

df['SPEED'] = df['dMETERS'] / df['dTIME']

df = df.drop(columns=['dMETERS', 'dTIME'])

print('min speed:', df['SPEED'].min())
print('max speed:', df['SPEED'].max())
print('avg speed:', df['SPEED'].mean())

print('\ncolumns:', df.columns.tolist())
print("\n",df.head())
