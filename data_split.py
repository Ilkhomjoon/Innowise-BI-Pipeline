import pandas as pd

file_path = 'Dataset\Superstore.csv'

print("Loading Data...")
df = pd.read_csv(file_path, encoding='latin1')

# 1. Split the data: 80% (Initial Load) and 20% (Secondary Load)
split_index = int(len(df) * 0.8)
initial_load = df.iloc[:split_index].copy()
secondary_load = df.iloc[split_index:].copy()

# SIMULATION for secondary loa

# 2. Adding duplicates: 
# The first 3 rows from the intial load will be added to the secondary load as they are
duplicates = initial_load.head(3).copy()
secondary_load = pd.concat([secondary_load, duplicates], ignore_index=True)

# 3. SCD Type 1 Simulation (changing name):
# Changing the name of the user in the 10th row
scd1_customer_id = secondary_load['Customer ID'].iloc[10]
secondary_load.loc[secondary_load['Customer ID'] == scd1_customer_id, 'Customer Name'] = "Name Changed"

# 4. SCD Type 2 Simulation (Updating address):
# Adding a new row based on an existing row from the initial load
scd2_customer_id = initial_load['Customer ID'].iloc[20]
new_transaction = initial_load[initial_load['Customer ID'] == scd2_customer_id].iloc[[0]].copy()

# Assuming the previous user moved to a new location, a new transaction is being added for him.
new_transaction['Row ID'] = 9995 
new_transaction['Order ID'] = 'TASH-2026-99999'
new_transaction['Region'] = 'Central'
new_transaction['State'] = 'Chilanzar'

secondary_load = pd.concat([secondary_load, new_transaction], ignore_index=True)

initial_load.to_csv('Dataset\initial_load.csv', index=False)
secondary_load.to_csv('Dataset\secondary_load.csv', index=False)

print("Ready!")