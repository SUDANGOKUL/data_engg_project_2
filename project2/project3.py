from dagster import asset
import pandas as pd
from pandas import DataFrame, read_excel, read_csv


@asset
def car_stocks() -> DataFrame:
    # read the input data from a excel file
    df = read_excel('./data/car_stocks.xlsx')
    return df

# color_table.csv file is webscrapped using a webscrapping code in project1.ipynb of the project folder
@asset
def color() -> DataFrame:
    # read the input data from a csv file
    df = pd.read_csv('./data/color_table.csv')

    # drop the first column as it is not needed
    df = df.drop(['Unnamed: 0'], axis=1)

    # rename the columns to remove the suffixes
    df.columns = ['Color', 'NA PPG', 'NA DP', 'EU PPG',
                  'EU DP', 'AP PPG', 'AP DP', 'WORLD PPG', 'WORLD DP']
    # droping the first two rows of the dataframe
    df = df.iloc[2:]
    return df


@ asset
def CCI() -> DataFrame:
    # read the input data from a csv file
    df = pd.read_csv('./data/cci.csv')
    # Selecting the Requirered months of january 
    df['TIME'] = pd.to_datetime(df['TIME'])
    df['Prod. year'] = df['TIME'].dt.year
    df = df[df['TIME'].dt.month == 1]
    df = df[df['TIME'].dt.day == 1]
    # drop the columns as they are not needed
    df = df.drop(["LOCATION", "INDICATOR", "SUBJECT", "MEASURE",
                 "FREQUENCY", "TIME", "Flag Codes"], axis=1)
    # rename the columns
    df.columns = ['CCI', 'Prod. year']
    return df

@asset
def car_stocks_hi_lo(car_stocks: DataFrame) -> DataFrame:
    df = car_stocks
    # Convert the 'Date' column of the DataFrame to a string and extract the first four characters as the year of production.
    df['Date'] = df['Date'].astype(str)
    df['Prod. year'] = df['Date'].str[:4]
    # Group the DataFrame by 'Manufacturer' and 'Prod. year' columns, and calculate the first and last closing prices for each group. Then, reset the index of the resulting DataFrame.
    df = df.groupby(['Manufacturer', 'Prod. year'])[
        'Closing Price'].agg(['first', 'last']).reset_index()
    # Convert the 'Manufacturer' column to uppercase and rename the columns of the DataFrame.
    df['Manufacturer'] = df['Manufacturer'].str.upper()
    df.columns = ['Manufacturer', 'Prod. year',
                  'Starting Price', 'Ending Price']
    # Calculate the percentage change in the closing price of each stock using the starting and ending prices, and round the result to the nearest integer.
    df['Stock Price Change'] = round(
        (df['Ending Price'] - df['Starting Price']) * 100 / df['Starting Price'])
    df['Stock Price Change'] = df['Stock Price Change'].astype(
        str).str.rstrip('%').astype(float)
    # Calculate the median percentage change in stock price for each production year and subtract it from the percentage change for each individual stock. Then, round the result to the nearest integer and add a '%' symbol to the end.

    Median = df.groupby('Prod. year')['Stock Price Change'].transform('median')
    df['Percentile'] = round(df['Stock Price Change'] - Median)
    df['Percentile'] = df['Percentile'].astype(int).astype(str) + '%'
    # Convert the 'Stock Price Change' column to a string and add a '%' symbol to the end. 
    df['Stock Price Change'] = df['Stock Price Change'].astype(
        int).astype(str) + '%'
    return df

@asset
def car_prices(color: DataFrame, CCI: DataFrame) -> DataFrame:
    # read the input data from a csv file
    df = read_csv('./data/car_price_prediction.csv')
    # Merge the color DataFrame with the main DataFrame df using a left join on the 'Color' column. This adds a new column 'Color ID' to the main DataFrame df with the corresponding values from the 'Color ID' column of the color DataFrame.
    df = df.merge(color, how='left', on=['Color'])
    # Merge the CCI DataFrame with the main DataFrame df using a left join on the 'Prod. year' column. This adds a new column 'CCI' to the main DataFrame df with the corresponding values from the 'CCI' column of the CCI DataFrame.
    df = df.merge(CCI, how='left', on=['Prod. year'])
    return df





@asset
def analytic_car_data(car_stocks_hi_lo: DataFrame, car_prices: DataFrame) -> DataFrame:
    #Convert the 'Prod. year' column in both input DataFrames car_stocks_hi_lo and car_prices from string to integer using the astype() method.
    car_stocks_hi_lo['Prod. year'] = car_stocks_hi_lo['Prod. year'].astype(int)
    car_prices['Prod. year'] = car_prices['Prod. year'].astype(int)
    # Merge the two input DataFrames car_stocks_hi_lo and car_prices based on the columns 'Prod. year' and 'Manufacturer', using a left join. This creates a new DataFrame df that contains the columns from both DataFrames.
    df = car_prices.merge(car_stocks_hi_lo, how='left', on=[
        'Prod. year', 'Manufacturer'])
    #ilter the resulting DataFrame df to keep only the rows where the 'Prod. year' is between 2011 and 2015, inclusive, using the boolean indexing.
    df = df[(df['Prod. year'] >= 2011) & (df['Prod. year'] <= 2015)]
    # Filter the resulting DataFrame df to keep only the rows where the 'Manufacturer' is one of the following: 'CHEVROLET', 'FORD', 'HYUNDAI', or 'TOYOTA', using the isin() method and boolean indexing.
    df = df[df['Manufacturer'].isin(
        ['CHEVROLET', 'FORD', 'HYUNDAI', 'TOYOTA'])]
    df.to_csv('analytical_car_data.csv', index=False)
    return df
