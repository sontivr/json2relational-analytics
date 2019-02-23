# json2relational-analytics

# Introduction
The goal of this repo is to quickly setup a ETL process that could be scheduled to run daily and to enable data analysts start querying the data.

# Approach
Each file to be processed has orders data in nested json format. src/process_json_files.py performs the following steps to process json data files and to load the processed files into postgres database.

1. Extracts line_items from each order to a separate csv file with user details
2. Creates a separate csv file for orders without line_items
3. Outputs csv files into a separate diretory
4. Loads orders csv file into myapp.orders and line_items csv file into myapp.line_items
5. Analyzes both orders and line_items tables

I have populated myapp.user_summary using user_summary.sql after orders and line_items tables were loaded.


# Getting Started
Make sure that postgres database is accessible and tables are created before invoking run.sh

