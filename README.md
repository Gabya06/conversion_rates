# conversion_rates
Repository for code to generate conversion rates

## Code Examples

clean.py: 
* input: .xlsx file to clean
* output: clean .csv file (this is the input file for conv_rates.py)

`python ./src/clean.py ./data/input_data/new_report_07052016.xlsx ./data/test/result3.csv`

conv_rates.py:
* input: cleaned .csv file, month_start, month_end 
   * month_start: month (number) to start calculating conversion rates
   * month_end: month (number) to stop calculating conversion rates
* output: conversion rates results in table format 
   * example output:

`python ./src/conv_rates.pyt ./data/test/results3.csv 9 1`

   Conversion Rates Results:

| Stage              | conv_rate | nominator | denominator | start_date | end_date | 
|--------------------|-----------|-----------|-------------|------------|----------| 
| Qualified          | 0.239332  | 129       | 539         | 10/17/15   | 6/13/16  | 
| Buying Process id. | 0.324895  | 77        | 237         | 10/17/15   | 6/13/16  | 
| Short List         | 0.407692  | 53        | 130         | 10/17/15   | 6/13/16  | 
| Chosen Vendor      | 0.62069   | 36        | 58          | 10/17/15   | 6/13/16  | 
| Negotiation/Review | 0.714286  | 35        | 49          | 10/17/15   | 6/13/16  | 
| PO In Progress     | 0.95      | 19        | 20          | 10/17/15   | 6/13/16  | 


