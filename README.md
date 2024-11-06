# Loan Data Analysis with SQL

## About the Project
The goal of this project is analyze loan payment abilities of customers of Czech Republic Bank. All analysis are done to answer the following analytical question:  
- **How do client demographics and account behaviors influence loan repayment statuses in terms of both frequency and amount of late or missed payments?**

The project incorporates the following procedures:
- Use of Python to process datasets for SQL analysis
- Operational Layer with all necessary tables
- Analytical Layer created with the help of ETL executed on Operational Layer
- Stored Procedures and Triggers
- Data Marts and Materialized View for analysis purposes

## Dataset
Dataset was downloaeded from https://github.com/prasantaman/Credit-Card-Data-Analysis-Using-SQL/tree/main. It has 8 tables with relevant columns. Python was used to process `LOAN`, `CREDIT_CARD`, `CLIENT`, `DEMOGRAPHIC`, `TRANSACTION` tables.
- `LOAN`, `TRANSACTION`, `CREDIT_CARD` have date type of YYMMDD, which were converted to standard date type of YYYY-MM-DD
- `CLIENT` has date of birth encrypted by adding 50 to month. If month > 12 then it is 'Female' and 'Male' otherwise. It was also converted to standard date type of YYYY-MM-DD
- `DEMOGRAPHIC` table has `?` for one observation which was replaced by NULL

NOTE: `ACCOUNT` table has date for account creation which is intentionally encrypted with random dates   

## Prerequisites
Please make sure that the following commands return 'ON' and 'Path To Access Import/Export Data' in MySQL WorkBench:
```
SHOW VARIABLES LIKE "local_infile";
SHOW VARIABLES LIKE "secure_file_priv";
```

## ER Diagram
<div align="center">
  <a>
    <img src="ER Diagram/ER-Diagram.png" alt="ER Diagram" width="600" height="450">
  </a>
</div>

**Analytical Layer, VIEWS, and Materialized VIEW**
<div align="center">
  <a>
    <img src="ER Diagram/Analytical Layer, Views.png" alt="ER Diagram" width="400" height="450">
  </a>
</div>

## Analytical Plan

### 1. Overview

The goal of this project is to build an analytical layer named `financial_analytics` to integrate data across multiple tables in a banking system. The layer aggregates key financial and demographic details for each account, providing a comprehensive view of account activities, loans, client demographics, and transaction data. Additionally, a trigger (`update_financial_analytics_after_insert`) ensures that the loan information is kept up-to-date in `financial_analytics` whenever new loan records are added.

### 2. Components
1. **Stored Procedure**: `create_financial_analytics`
2. **Trigger**: `update_financial_analytics_after_insert`

### 3. Dependencies
This plan assumes the existence of the following tables:

- `ACCOUNT:` Holds basic account information.
- `LOAN:` Stores loan details linked to accounts.
- `DISPOSITION:` Links accounts to clients.
- `CLIENT:` Contains client details (e.g., birth year, gender).
- `DEMOGRAPHIC:` Stores demographic information (e.g., region, average salary) with anonymized columns.
- `TRANSACTION:` Contains records of account transactions.
- `CREDIT_CARD:` Records information on client credit cards.
- `ORDERS:` Holds data about orders associated with accounts.

### 4. Analytical Layer: `financial_analytics`
The `financial_analytics` table consolidates data from the dependent tables. Below is the structure and purpose of each component in the `financial_analytics` table.

**Columns:**
- **Account Information:**
    - `account_id:` Primary identifier for each account.
    - `account_creation_date:` Date when the account was created.
    - `account_frequency:` Frequency of the account (e.g., monthly, yearly).

- **Loan Information:**
    - `loan_amount:` Amount of the loan, set to 0 if no loan exists.
    - `loan_duration:` Duration of the loan, set to 0 if no loan exists.
    - `loan_status:` Categorical status of the loan, translating loan status codes to meaningful descriptions.

- **Client Information:**
    - `client_id:` Primary identifier for each client.
    - `client_birth_year:` Year of birth for the client, derived from the birth_number.
    - `client_gender:` Gender of the client.

- **Demographic Information:**
    - `client_region:` Region of the client, anonymized in the DEMOGRAPHIC table.
    - `urban_inhabitants:` Ratio of urban inhabitants in the client’s region.
    - `average_salary:` Average salary in the client’s region.
    - `unemployment_rate_1995:` Unemployment rate in the region in 1995.
    - `unemployment_rate_1996:` Unemployment rate in the region in 1996.

- **Transaction Information:**
    - `transaction_total:` Total sum of all transactions associated with the account.
    - `transaction_type:` Type of the latest transaction, categorized as either 'Credit' or 'Debit' based on the last transaction type.

- **Credit Card Information:**
    - `credit_card_type:` Type of the client’s credit card, or ‘No Card’ if none exists.
- **Order Information:**
    - `order_total_amount:` Sum of all orders associated with the account.

**SQL Code**
The `create_financial_analytics` stored procedure accomplishes this by creating the `financial_analytics` table through multiple `LEFT JOIN` operations to retain accounts even if they lack loans, transactions, or other associated data.

### 5. **Trigger:** `update_financial_analytics_after_insert`
The `update_financial_analytics_after_insert` trigger keeps the `financial_analytics` table in sync with the `LOAN` table. When a new loan record is inserted, the trigger:
1. Checks if the account already exists in the `financial_analytics` table.
2. If the account exists, it updates the loan details for that account.
3. If the account doesn’t exist, it inserts a new record with the loan details for the new account.
   
**Trigger Logic**
- **Loan Status:** Translates loan status codes (e.g., 'A', 'B', 'C', 'D') to descriptive statuses for readability.
- **Insert or Update:** Ensures that each account in `financial_analytics` has up-to-date loan information immediately after a new loan is created.

### 6. Implement all of the above mentioned procedures.
