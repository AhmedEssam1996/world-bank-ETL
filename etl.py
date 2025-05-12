import pandas as pd
from sqlalchemy import create_engine
import urllib

# 1. Extract
def extract(file_name):
    return pd.read_excel(file_name)

# 2. Transform
def transform(df):
    df = df.rename(columns={
        'Country Name': 'Country_Name',
        'Country Code': 'Country_Code',
        'Region': 'Region',
        'IncomeGroup': 'IncomeGroup',
        'Year': 'Year',
        'Birth rate, crude (per 1,000 people)': 'BirthRate',
        'Death rate, crude (per 1,000 people)': 'DeathRate',
        'Electric power consumption (kWh per capita)': 'ElectricityConsumption',
        'GDP (USD)': 'GDP',
        'GDP per capita (USD)': 'GDP_per_capita',
        'Individuals using the Internet (% of population)': 'InternetUsage',
        'Infant mortality rate (per 1,000 live births)': 'InfantMortality',
        'Life expectancy at birth (years)': 'LifeExpectancy',
        'Population density (people per sq. km of land area)': 'PopulationDensity',
        'Unemployment (% of total labor force) (modeled ILO estimate)': 'UnemploymentRate'
    })

    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    return df

# 3. Load
def load(df, target_table):
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=DESKTOP-QU2UO02;"
        "DATABASE=SalesDB;"
        "Trusted_Connection=yes;"
    )
    
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    df.to_sql(name=target_table, con=engine, if_exists='replace', index=False)
    print("تم تحميل البيانات بنجاح إلى الجدول:", target_table)

file_name = r"C:\Users\Elbostan\Desktop\New folder (8)\New folder (7)\WorldBank.xlsx"
extracted_data = extract(file_name)
transformed_data = transform(extracted_data)
load(transformed_data, 'DevelopmentIndicators')