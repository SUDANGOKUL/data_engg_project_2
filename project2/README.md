# Project #2 Description

Why do some cars sell for more than others? What are the factors that go into not only the buying decisions of consumers, but also the pricing decisions of auto manufacturers?  What data can we use to determine the best answer to some of these questions?

On the surface, if we're trying to predict car price, we might just look at the characteristics of the car: make, model, year, engine size, features, trim package.  Unfortunately, neither humans nor corporations are simple, rational, mathematical decision makers -- what behavioral economists refer to as [econs](https://www.researchgate.net/publication/344119977_Econs_vs_Humans_An_Introduction_to_Behavioral_Economics_Social_Education_832_p_94-99).

## Directed Acyclic Graphs (DAGs)
For this project, we're going to build a data pipeline using a popular Python package called [dagster](https://dagster.io/).  In computer science, a DAG is a Directed Acyclic Graph.
* Graph - meaning the process can be described as a collection of connected nodes that represent data processing activities
* Directed - meaning that flow of control goes in a specific path from one node to another, but not arbitrarily back-and-forth
* Acyclic - meaning that the flow of control does not circle back on itself

You can picture a DAG as a set of processing instructions in a series of activities that you can read from one side of the page to the other -- typically following a left to right convention or a top-down convention.

![DAG Example](dag_example.png)

## Data about Car Sales and Auto Manufacturers
For this project, we're going to take two separate data sources and combine them into a single analytical data set:
* From a [Car Price Prediction Challenge](https://www.kaggle.com/datasets/deepcontractor/car-price-prediction-challenge) on Kaggle, we have some information some historical car sales includes models from 1939 through 2020. This data set gives us manufacturer, model, production year, category, if the vehicle has leather interior, fuel type, and engine size. It also has the price paid for the sale of that vehicle.
* We also want to know something about the perception of that particular vehicle manufacturer when that car was manufactured. For example, did we think "Chevys are better than Fords" in 2016 when that vehicle was manufactured. I don't know for sure that this information is actually important to the resale of that vehicle today, but economists who study resale of vehicles tell me perception is important. Our proxy for "are Chevys better than Fords" in any particular year is going to be the relative performance of their stock price during that year.  For example, if the stock price for Chevrolet increased by 10% in 2016 and the stock price for Ford increased by 15% in 2016, then we will say that Ford was perceived better than Chevy.

I'll provide some additional instructions in the requirents later on.

# Project Deliverables

## Final Data Set:

### A) Kaggle Car Prices
The final data set will need to have the following columns from the car price data from Kaggle:
* ID 
* Price
* Manufacturer
* Model
* Prod. year
* Category
* Leather interior
* Fuel type
* Engine volume
* Mileage
* Cylinders
* Gear box type
* Drive wheels
* Doors
* Wheel
* Color
* Airbags

### B) Stock Price
And two additional columns from the stock price lookups:
* Starting stock price - Stock price for the vehicle's manufacturer on January 1 of the vehicle's production year
* Ending stock price - Stock price for the vehicle's manufacturer on December 31 of the vehicle's production year
* Stock price change - Percent change in stock price that year
* Distance from median - The distance between the selected percent stock price change and the median stock price change for that same year over all auto manufacturers

### C) Automobile Color Popularity (from 2012)
Add eight additional columns from [this Wikipedia page](https://en.wikipedia.org/wiki/Car_colour_popularity). For color popularity, you don't need to worry about the specific year. Since 2012 occurs in our scope of analysis (2011-2015), we'll just assume the color preferences didn't change much during that period.
* US PPG
* US DP
* AP PPG
* AP DP
* EU PPG
* EU DP
* WORLD PPG
* WORLD DP

### D) Consumer Confidence Index
From [this website](https://data.oecd.org/leadind/consumer-confidence-index-cci.htm), extract the data for CCI by month and add the CCI value for January of the production year of each car.
* January CCI


## Code:
Using dagster as your toolkit, create a DAG that takes your input data sources and produces the required Final Data Set described above. As usual, I'll grade on three criteria:
1. Does your code run?
2. Does your code produce the right output?
3. Did you write code that follows good practices and is easy to understand?

Included in the project directory is a `output.valid` file (a CSV that contains the output I'm expecting your program to create) to help you with end-to-end testing.

## Diagram:
Create a simple visual diagram describing your DAG. Store it as a PNG file in your project repository.


# Constraints
1. Only examine the data for cars with production five years from 2011 and 2015
2. Only examine the data for the following four manufacturers: Hyundai, Toyota, Ford, Chevrolet (GM)


# Stock Price Calculations Explained
The first two values in the stock price lookup are easy to understand: Get the 1/1 and 12/31 stock prices for that car manufacurer and for the year the car was produced.

```math
{Percent Change} = \frac{Ending Price - Starting Price}{Starting Price}
```

```math
{Distance From Median} = {Percent Change} - median([{PercentChangesForYear}])
```

As an example, if we have the following data form 2011:
| Manufacturer | Starting |   Ending | Pct Change |
|--------------|----------|----------|------------|
| Chevrolet    |    28.08 |    15.60 |       -44% |
| Ford         |    11.22 |     6.95 |       -38% |
| Hyundai      |    30.10 |    25.25 |       -16% |
| Toyota       |    63.77 |    51.70 |       -19% |

We can calculate the **Distance from Median** for Chevrolet as:

```math
Distance From Median = {-44}\% - median([PercentChangesForYear])
```

```math
median([PercentChangesForYear]) = {-29}\%
```

```math
Distance From Median = {-16}\%
```

Following through on all calculations for 2011:
| Manufacturer | Starting |   Ending | Pct Change | Distance from Med |
|--------------|----------|----------|------------|-------------------|
| Chevrolet    |    28.08 |    15.60 |       -44% |              -16% |
| Ford         |    11.22 |     6.95 |       -38% |              -10% |
| Hyundai      |    30.10 |    25.25 |       -16% |               12% |
| Toyota       |    63.77 |    51.70 |       -19% |               10% |
