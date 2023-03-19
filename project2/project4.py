from dagster import asset
import pandas as pd
from pandas import DataFrame, read_excel, read_csv


@asset
def car_stocks() -> DataFrame:
    df = read_excel('./data/car_stocks.xlsx')
    return df


@asset
def color() -> DataFrame:
    # read the input data from a csv file
    df = pd.read_csv('./data/color_table.csv')

    # drop the first column as it is not needed
    df = df.drop(['Unnamed: 0'], axis=1)

    # rename the columns to remove the suffixes
    df.columns = ['Color', 'NA PPG', 'NA DP', 'EU PPG',
                  'EU DP', 'AP PPG', 'AP DP', 'WORLD PPG', 'WORLD DP']
    df = df.iloc[2:]
    return df


@ asset
def CCI() -> DataFrame:
    df = pd.read_csv('./data/cci.csv')
    df['TIME'] = pd.to_datetime(df['TIME'])
    df['Prod. year'] = df['TIME'].dt.year
    df = df[df['TIME'].dt.month == 1]
    df = df[df['TIME'].dt.day == 1]
    df = df.drop(["LOCATION", "INDICATOR", "SUBJECT", "MEASURE",
                 "FREQUENCY", "TIME", "Flag Codes"], axis=1)
    df.columns = ['CCI', 'Prod. year']
    return df

@asset
def car_stocks_hi_lo(car_stocks: DataFrame) -> DataFrame:
    df = car_stocks

    df['Date'] = df['Date'].astype(str)
    df['Prod. year'] = df['Date'].str[:4]
    df = df.groupby(['Manufacturer', 'Prod. year'])[
        'Closing Price'].agg(['first', 'last']).reset_index()
    df['Manufacturer'] = df['Manufacturer'].str.upper()
    df.columns = ['Manufacturer', 'Prod. year',
                  'Starting Price', 'Ending Price']
    df['Stock Price Change'] = round(
        (df['Ending Price'] - df['Starting Price']) * 100 / df['Starting Price'])
    df['Stock Price Change'] = df['Stock Price Change'].astype(
        str).str.rstrip('%').astype(float)

    Median = df.groupby('Prod. year')['Stock Price Change'].transform('median')
    df['Percentile'] = round(df['Stock Price Change'] - Median)
    df['Percentile'] = df['Percentile'].astype(int).astype(str) + '%'
    df['Stock Price Change'] = df['Stock Price Change'].astype(
        int).astype(str) + '%'
    return df

@asset
def car_prices(color: DataFrame, CCI: DataFrame) -> DataFrame:
    df = read_csv('./data/car_price_prediction.csv')
    df = df.merge(color, how='left', on=['Color'])
    df = df.merge(CCI, how='left', on=['Prod. year'])
    return df


@asset
def car_stocks_hi_lo(car_stocks: DataFrame) -> DataFrame:
    df = car_stocks

    df['Date'] = df['Date'].astype(str)
    df['Prod. year'] = df['Date'].str[:4]
    df = df.groupby(['Manufacturer', 'Prod. year'])[
        'Closing Price'].agg(['first', 'last']).reset_index()
    df['Manufacturer'] = df['Manufacturer'].str.upper()
    df.columns = ['Manufacturer', 'Prod. year',
                  'Starting Price', 'Ending Price']
    df['Stock Price Change'] = round(
        (df['Ending Price'] - df['Starting Price']) * 100 / df['Starting Price'])
    df['Stock Price Change'] = df['Stock Price Change'].astype(
        str).str.rstrip('%').astype(float)

    Median = df.groupby('Prod. year')['Stock Price Change'].transform('median')
    df['Percentile'] = round(df['Stock Price Change'] - Median)
    df['Percentile'] = df['Percentile'].astype(int).astype(str) + '%'
    df['Stock Price Change'] = df['Stock Price Change'].astype(
        int).astype(str) + '%'
    print(df)
    return df


@asset
def analytic_car_data(car_stocks_hi_lo: DataFrame, car_prices: DataFrame) -> DataFrame:
    car_stocks_hi_lo['Prod. year'] = car_stocks_hi_lo['Prod. year'].astype(int)
    car_prices['Prod. year'] = car_prices['Prod. year'].astype(int)
    df = car_prices.merge(car_stocks_hi_lo, how='left', on=[
        'Prod. year', 'Manufacturer'])
    df = df[(df['Prod. year'] >= 2011) & (df['Prod. year'] <= 2015)]
    df = df[df['Manufacturer'].isin(
        ['CHEVROLET', 'FORD', 'HYUNDAI', 'TOYOTA'])]
    df.to_csv('analytical_car_data.csv', index=False)
    return df
