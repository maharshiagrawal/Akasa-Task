import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sqlalchemy import create_engine # type: ignore

df_flight = pd.read_csv('C:/Users/palak/OneDrive/Desktop/aviation_data.csv')

# 1) For cleaning the data
# Checking the original format
print(df_flight[['DepartureDate', 'ArrivalDate']].head())

# 2) Converting departure date and arrival date into proper date format
df_flight['DepartureDate'] = pd.to_datetime(df_flight['DepartureDate'], format='%m-%d-%Y', errors='coerce')
df_flight['ArrivalDate'] = pd.to_datetime(df_flight['ArrivalDate'], format='%m-%d-%Y', errors='coerce')

# 3) Converting Departure time and Arrival time into 24-hour format
def parse_time(time_str):
    try:
        return pd.to_datetime(time_str, format='%I:%M %p').time()  # 12-hour format with AM/PM
    except ValueError:
        return pd.to_datetime(time_str, format='%H:%M:%S').time()  # 24-hour format

df_flight['DepartureTime'] = df_flight['DepartureTime'].apply(parse_time)
df_flight['ArrivalTime'] = df_flight['ArrivalTime'].apply(parse_time)

# 4) Handling NaN values in the DelayMinutes column
df_flight['DelayMinutes'] = df_flight['DelayMinutes'].fillna(df_flight['DelayMinutes'].median())

# 5) Dropping duplicates based on FlightNumber and DepartureDate
df_unique_flights = df_flight.drop_duplicates(subset=['FlightNumber', 'DepartureDate'])
print(f'Total number of unique entries: {len(df_unique_flights)}')
print(df_unique_flights)

# 6) Check for NaT values before combining date and time
if df_flight['DepartureDate'].isna().any() or df_flight['DepartureTime'].isna().any():
    print("Warning: There are NaT values in DepartureDate or DepartureTime.")
    df_flight = df_flight.dropna(subset=['DepartureDate', 'DepartureTime'])

# Combining Departure/Arrival date and time
df_flight['DepartureDateTime'] = pd.to_datetime(df_flight['DepartureDate'].astype(str) + ' ' + df_flight['DepartureTime'].astype(str))
df_flight['ArrivalDateTime'] = pd.to_datetime(df_flight['ArrivalDate'].astype(str) + ' ' + df_flight['ArrivalTime'].astype(str))

# Adjusting ArrivalDateTime if it is less than DepartureDateTime
bool_mask = df_flight['ArrivalDateTime'] < df_flight['DepartureDateTime']
df_flight.loc[bool_mask, 'ArrivalDateTime'] += pd.Timedelta(days=1)

# 7) Creating a new column for FlightDuration
df_flight['FlightDuration'] = df_flight['ArrivalDateTime'] - df_flight['DepartureDateTime']

# 8) Analyzing the distribution of delays
print(df_flight['DelayMinutes'].describe())

plt.figure(figsize=(10, 6))
sns.boxplot(x='Airline', y='DelayMinutes', data=df_flight)
plt.title('Flight Delays by Airline')
plt.xlabel('Airline')
plt.ylabel('Delay (minutes)')
plt.show()

# Calculate the average delay for each airline
average_delay = df_flight.groupby('Airline')['DelayMinutes'].mean().reset_index()

# Bar chart for average delay by airline
plt.figure(figsize=(10, 6))
sns.barplot(x='Airline', y='DelayMinutes', data=average_delay, palette='viridis')
plt.title('Average Delay by Airline')
plt.xlabel('Airline')
plt.ylabel('Average Delay (Minutes)')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.show()

# Save the cleaned DataFrame to a new CSV file
df_flight.to_csv('C:/Users/palak/OneDrive/Desktop/cleaned_aviation_dataaa.csv', index=False)
print("Cleaned data saved as 'cleaned_aviation_dataaa.csv'.")


# 9) Storing data in a database
# Create a database connection
engine = create_engine('sqlite:///aviation_data.db')  # Create SQLite database file

# Store the cleaned data into the database
df_flight.to_sql('flights', con=engine, if_exists='replace', index=False)
print("Cleaned data saved to the database 'aviation_data.db' in the 'flights' table.")