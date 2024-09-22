import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine # type: ignore

# Load the dataset
df_flight = pd.read_csv('C:/Users/palak/OneDrive/Desktop/aviation_data.csv')

# Display basic info about the dataset
df_flight.info()
print(df_flight.head())

# 1) For cleaning the data
# Checking the original format
from datetime import datetime
print(df_flight[['DepartureDate', 'ArrivalDate']].head())

# 2) Converting DepartureDate and ArrivalDate columns to datetime
df_flight['DepartureDate'] = pd.to_datetime(df_flight['DepartureDate'], format='%m-%d-%Y', errors='coerce')
df_flight['ArrivalDate'] = pd.to_datetime(df_flight['ArrivalDate'], format='%m-%d-%Y', errors='coerce')

# Display the transformed columns
print(df_flight[['DepartureDate', 'ArrivalDate']].head())

# 3) Converting DepartureTime and ArrivalTime into 24-hour format
def parse_time(time_str):
    try:
        return pd.to_datetime(time_str, format='%I:%M %p').time()  # 12-hour format
    except ValueError:
        return pd.to_datetime(time_str, format='%H:%M:%S').time()  # 24-hour format

df_flight['DepartureTime'] = df_flight['DepartureTime'].apply(parse_time)
df_flight['ArrivalTime'] = df_flight['ArrivalTime'].apply(parse_time)

print(df_flight[['DepartureTime', 'ArrivalTime']].head())

# 4) Handling NaN values in the DelayMinutes column
df_flight['DelayMinutes'].fillna(df_flight['DelayMinutes'].median(), inplace=True)

# Checking unique flights
unique_flight_dates = df_flight[['FlightNumber', 'DepartureDate']].drop_duplicates()
print(f'Total number of unique: {unique_flight_dates}')

# Dropping duplicates
df_unique_flights = df_flight.drop_duplicates(subset=['FlightNumber', 'DepartureDate'])
print(f'Total number of unique entries: {len(df_unique_flights)}')
print(df_unique_flights)

# 6) Combining Departure/Arrival date and time
df_unique_flights['DepartureDateTime'] = pd.to_datetime(
    df_unique_flights['DepartureDate'].astype(str) + ' ' + df_unique_flights['DepartureTime'].astype(str)
)
df_unique_flights['ArrivalDateTime'] = pd.to_datetime(
    df_unique_flights['ArrivalDate'].astype(str) + ' ' + df_unique_flights['ArrivalTime'].astype(str)
)

# Display the updated DataFrame
print(df_unique_flights[['FlightNumber', 'DepartureDateTime', 'ArrivalDateTime']])

# Dropping unnecessary columns
new_df_flights = df_unique_flights.drop(['DepartureDate', 'DepartureTime', 'ArrivalDate', 'ArrivalTime'], axis=1)

bool_mask = new_df_flights['ArrivalDateTime'] < new_df_flights['DepartureDateTime']
new_df_flights.loc[bool_mask, 'ArrivalDateTime'] += pd.Timedelta(days=1)
print(new_df_flights[['FlightNumber', 'DepartureDateTime', 'ArrivalDateTime']])

# 7) Creating a new column for FlightDuration
new_df_flights['FlightDuration'] = new_df_flights['ArrivalDateTime'] - new_df_flights['DepartureDateTime']

# 8) Analyze the distribution of delays
print(new_df_flights['DelayMinutes'].describe())

plt.subplot(1, 1, 1)
sns.boxplot(x='Airline', y='DelayMinutes', data=new_df_flights)
plt.title('Flight Delays by Airline')
plt.xlabel('Airline')
plt.ylabel('Delay (minutes)')

# Average delay for each airline
average_delay = new_df_flights.groupby('Airline')['DelayMinutes'].mean().reset_index()

# Creating DepartureHour column
new_df_flights['DepartureHour'] = new_df_flights['DepartureDateTime'].dt.hour

# Grouping by departure hour
delay_analysis = new_df_flights.groupby('DepartureHour')['DelayMinutes'].mean().reset_index()
print(delay_analysis)

# Plotting average delay by airline
plt.figure(figsize=(10, 6))
sns.barplot(x='Airline', y='DelayMinutes', data=average_delay, palette='viridis')
plt.title('Average Delay by Airline')
plt.xlabel('Airline')
plt.ylabel('Average Delay (Minutes)')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.show()

# Saving the cleaned dataset to a CSV file
new_df_flights.to_csv('C:/Users/palak/OneDrive/Desktop/cleaned_aviation_dataaa.csv', index=False)

# 9) Storing data in a database
# Create a database connection
engine = create_engine('sqlite:///C:/Users/palak/OneDrive/Desktop/aviation_data.db')  # Create SQLite database file

# Store the cleaned data into the database
new_df_flights.to_sql('flights', con=engine, if_exists='replace', index=False)
print("Cleaned data saved to the database 'aviation_data.db' in the 'flights' table.")