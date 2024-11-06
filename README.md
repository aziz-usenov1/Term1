<div align="center">
  <a>
    <img src="loan_logo.jpg" alt="Loan" width="600" height="400">
  </a>
</div>

# Loan Data Analysis with SQL

## About the Project
The objective of this project is to analyze loan repayment capacities among customers of Czech Republic Bank, focusing on the following key question:
- **How do client demographics and account behaviors influence loan repayment statuses in terms of both frequency and amount of late or missed payments?**

This project involves a multi-layered analytical approach, incorporating:
1. **Python-Driven Data Processing:** Preprocessing of datasets to ensure compatibility with SQL analyses.
2. **Operational Layer:** A foundational database layer with essential tables.
3. **Analytical Layer:** Developed using ETL processes on the Operational Layer.
4. **Stored Procedures and Triggers:** Enhancements to streamline database operations.
5. **Data Marts and Materialized Views:** To facilitate in-depth analysis.

## Dataset
The dataset, sourced from [GitHub](https://github.com/prasantaman/Credit-Card-Data-Analysis-Using-SQL/tree/main), comprises eight tables with pertinent columns. Initial processing of the LOAN, CREDIT_CARD, CLIENT, DEMOGRAPHIC, and TRANSACTION tables was done using Python. Key transformations included:

- **Date Formatting:** The `LOAN`, `TRANSACTION`, and `CREDIT_CARD` tables contained dates in YYMMDD format, which were standardized to YYYY-MM-DD.
- **Client Birth Date and Gender Encoding:** `CLIENT` data contained dates of birth encrypted by adding 50 to the month (months over 12 signify females, otherwise males). Dates were converted to the YYYY-MM-DD format.
- **DEMOGRAPHIC Table:** Missing values marked as “?” were replaced with NULL.
- **ACCOUNT Table:** Account creation dates are intentionally encrypted with random dates for privacy.

## Prerequisites
Before proceeding, ensure that MySQL Workbench returns 'ON' and 'Path To Access Import/Export Data' for the following configuration commands:
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

<div align="center">
  <a>
    <img src="SQL Outputs/Analytical Table Snapshot.png" alt="ER Diagram" width="1000" height="100">
  </a>
</div>

## Data Mart Views and Materialized View for Financial Analytics

### **1. Loan Analysis View** (`loan_analysis_view`)
This view provides a summary of loans per account, detailing the count, average amount, average duration, and debt status. It’s essential for monitoring client loan statuses and identifying clients in debt.

- **Fields:**
    - `account_id`: Unique identifier for each account.
    - `loan_status`: The current loan status (e.g., 'Client in debt').
    - `loan_count`: Total number of loans per account.
    - `avg_loan_amount`: Average loan amount per account.
    - `avg_loan_duration`: Average loan duration per account.
    - `clients_in_debt`: Count of loans where the client status is "Client in debt".
- **Use Case:** Provides insight into loan performance and helps track accounts with loans in debt.

### **2. Transaction Summary View** (`transaction_summary_view`)
This view summarizes transactions by account and transaction type, enabling analysis of transaction patterns and average amounts.

- **Fields:**
    - `account_id`: Unique identifier for each account.
    - `transaction_type`: Type of transaction (e.g., 'deposit', 'withdrawal').
    - `transaction_count`: Count of transactions by type for each account.
    - `avg_transaction_amount`: Average amount for each transaction type per account.
- **Use Case:** Supports transaction analysis, allowing financial analysts to identify transaction trends and understand customer spending behaviors.

### **3. Client Demographics View** (`client_demographics_view`)
This view provides demographic insights into clients by region, focusing on metrics like average salary, unemployment rates, and urban inhabitants ratio.

- **Fields:**
    - `client_region`: Region where the client resides.
    - `total_clients`: Total number of clients in each region.
    - `avg_salary`: Average salary of clients in each region.
    - `avg_unemployment_rate_1995`, `avg_unemployment_rate_1996`: Average unemployment rates for 1995 and 1996 by region.
    - `avg_urban_inhabitants_ratio`: Average urban inhabitants ratio in each region.
- **Use Case:** Helps in understanding the demographic distribution of clients, which aids in tailored marketing and policy-making.

### **4. Materialized View: Account Activity Summary** (`account_activity_summary`)
A materialized view that summarizes account activity and status. This view is refreshed daily to provide up-to-date information on transaction totals, average order amounts, and account status.

- **Fields:**
    - `account_id`: Unique identifier for each account.
    - `account_creation_date`: Date when the account was created.
    - `account_frequency`: Frequency of account usage.
    - `total_transactions`: Sum of all transaction totals for the account.
    - `avg_order_amount`: Average order amount for transactions associated with the account.
    - `loan_presence`: Status indicating if the account has an active loan ("Has Loan" or "No Loan").
    - `account_status`: Indicates if the account is "Active" or "Inactive" based on transaction activity.
- **Use Case:** Optimizes performance by storing precomputed summaries, allowing fast retrieval of account activity data. It's suitable for daily reporting and analytics.

- **Event: Daily Refresh** (`refresh_account_activity_summary`)
A scheduled event refreshes the account_activity_summary materialized view daily. This event ensures that the view is updated with any new transactions, account changes, or other relevant data recorded in the financial_analytics table.

    - **Event Description:** Deletes and repopulates the data in the account_activity_summary table.
    - **Frequency:** Every 1 day.
    - **Logic:**
        - Clears existing data from account_activity_summary.
        - Re-inserts aggregated data from the financial_analytics table.
    - **Purpose:** Keeps the view’s data accurate by fully refreshing it every day, which is essential for time-sensitive analysis and reporting.
- **Triggers on** `financial_analytics` **for Real-Time Updates**
  
To ensure real-time updates, three triggers (`INSERT`, `UPDATE`, `DELETE`) are created on the `financial_analytics` table. These triggers automatically adjust values in the `account_activity_summary` table whenever relevant data is modified in financial_analytics.

- **Trigger: After Insert** (`trg_after_insert_financial_analytics`)

This trigger is activated after a new record is inserted into financial_analytics, ensuring that the account_activity_summary table is updated to reflect the new transaction or account data.

    - Trigger Type: `AFTER INSERT`
    - Action: Inserts a new record into `account_activity_summary` or updates an existing one.
    - Logic:
        - Adds `transaction_total` to the `total_transactions`.
        - Adjusts `avg_order_amount` by recalculating with the new `order_total_amount`.
        - Updates `loan_presence` if a loan amount is recorded.
        - Sets `account_status` to "Active" if `transaction_total` is greater than zero.
        
**Trigger: After Update** (`trg_after_update_financial_analytics`)

This trigger is activated after a record in financial_analytics is updated. It adjusts values in account_activity_summary to reflect the updated transaction or account details.

    - Trigger Type: `AFTER UPDATE`
    - Action: Modifies existing data in `account_activity_summary`.
    - Logic:
        - Adjusts `total_transactions` by adding the difference between the new and old `transaction_total`.
        - Recalculates `avg_order_amount` based on the new and old values of `order_total_amount`.
        - Updates `loan_presence` if the updated record includes a loan amount.
        - Adjusts `account_status` based on the updated `transaction_total`.
        
**Trigger: After Delete** (`trg_after_delete_financial_analytics`)

This trigger is activated after a record is deleted from financial_analytics. It adjusts the account_activity_summary view to account for the removed data.

    - Trigger Type: `AFTER DELETE`
    - Action: Updates data in `account_activity_summary` to reflect the deletion.
    - Logic:
        - Reduces `total_transactions` by the deleted `transaction_total`.
        - Adjusts `avg_order_amount` by recalculating after removing the deleted order amount.
        - Updates `loan_presence` to "No Loan" if there are no remaining transactions with loans.
        - Sets `account_status` to "Inactive" if `total_transactions` is zero after the deletion.

## **Conclusion**

This project provides a comprehensive analysis of loan repayment patterns, exploring how client demographics and account behaviors influence loan statuses. By creating an efficient analytical layer, implementing stored procedures and triggers, and using data marts and materialized views, the project enables detailed insights into customer loan behaviors. These insights help in identifying clients at risk and understanding demographic and behavioral trends that may affect repayment.

## **Future Enhancements**

Potential future improvements include refining the triggers for enhanced real-time updates, expanding demographic analysis by adding new variables, and optimizing materialized view refresh frequency based on analytical needs.

#### Thank you for reviewing this project! Feel free to reach out or contribute with suggestions for further development.







