# UK Data Pull Pipeline
This Jupyter notebook is designed to retrieve XHTML data from Companies House UK.

The main CSV file is very large and therefore has been split into 7 parts to be processed separately.

The companies have been filtered with the aim to exclude those classified as micro entities.

The CSV data column number 20: `Accounts.AccountCategory` has been used to filter out the non-relevant entries.

XHTML data of companies' filings is processed and downloaded from the Companies House API, and interacts with a database to update company profiles and XHTML tables in AWS. It also includes error logging and the ability to find the latest uploaded item in a DynamoDB table.