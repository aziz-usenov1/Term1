-- Creating a new schema and ensuring that it is unique

DROP SCHEMA IF EXISTS CREDIT_ANALYSIS;
CREATE SCHEMA CREDIT_ANALYSIS;
USE CREDIT_ANALYSIS;

######################
-- OPERATIONAL LAYER
######################

-- Creating and populating tables in schema

-- 1. DEMOGRAPHIC table
DROP TABLE IF EXISTS DEMOGRAPHIC;
CREATE TABLE DEMOGRAPHIC (
    A1 INT NOT NULL PRIMARY KEY,
    A2 VARCHAR(255),
    A3 VARCHAR(255),
    A4 INT,
    A5 INT,
    A6 INT,
    A7 INT,
    A8 INT,
    A9 INT,
    A10 DECIMAL,
    A11 INT,
    A12 DECIMAL,
    A13 DECIMAL,
    A14 INT,
    A15 INT,
    A16 INT
);
-- Loading data
LOAD DATA INFILE '/Users/azizbek.ussenov/mysql_uploads/DEMOGRAPHIC.csv'
INTO TABLE DEMOGRAPHIC 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, @A12, A13, A14, @A15, A16)
SET
A12 = NULLIF(@A12, ''),
A15 = NULLIF(@A15, '')
;

-- Create a new temporary table which store values from dataset. 
-- After get account_id that pertain to both tables and drop others otherwise no foreign key reference can be achieved.
-- Finally, load data with common account_id into original table called 'ACCOUNT' and drop the temporary one.
-- The logic is the same for other tables throughout the creating and loading procedures.

-- 2. ACCOUNT table
DROP TABLE IF EXISTS ACCOUNT;
CREATE TABLE ACCOUNT (
    account_id INT NOT NULL PRIMARY KEY,
    district_id INT NOT NULL,
    frequency VARCHAR(255) NOT NULL,
    date DATE NOT NULL
);
------
CREATE TEMPORARY TABLE ACCOUNT_STAGING LIKE ACCOUNT;

------
-- SET GLOBAL local_infile = 1;
-- SHOW VARIABLES LIKE "local_infile";
-- SHOW VARIABLES LIKE "secure_file_priv";

-- Loading data
LOAD DATA INFILE '/Users/azizbek.ussenov/mysql_uploads/ACCOUNT.csv'
INTO TABLE ACCOUNT_STAGING
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES;

------
SET SQL_SAFE_UPDATES = 0;
DELETE FROM ACCOUNT_STAGING
WHERE district_id NOT IN (SELECT A1 FROM DEMOGRAPHIC);
SET SQL_SAFE_UPDATES = 1;
------
INSERT INTO ACCOUNT (account_id, district_id, frequency, date)
SELECT account_id, district_id, frequency, date
FROM ACCOUNT_STAGING;

DROP TEMPORARY TABLE ACCOUNT_STAGING;
------


-- 3. CLIENT table
DROP TABLE IF EXISTS CLIENT;
CREATE TABLE CLIENT (
    client_id INT NOT NULL PRIMARY KEY,
    birth_number DATE,
    district_id INT,
    gender VARCHAR(255),
    FOREIGN KEY (district_id) REFERENCES DEMOGRAPHIC(A1)    
);
-- Loading data
LOAD DATA INFILE '/Users/azizbek.ussenov/mysql_uploads/CLIENT.csv'
INTO TABLE CLIENT 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

-- 4. DISPOSITION table
DROP TABLE IF EXISTS DISPOSITION;
CREATE TABLE DISPOSITION (
    disp_id INT NOT NULL PRIMARY KEY,
    client_id INT,
    account_id INT,
    type VARCHAR(255) NOT NULL,
	FOREIGN KEY (client_id) REFERENCES CLIENT(client_id),
    FOREIGN KEY(account_id) REFERENCES ACCOUNT(account_id)
);

--------
CREATE TEMPORARY TABLE DISPOSITION_STAGING LIKE DISPOSITION;
--------
-- Loading data
LOAD DATA INFILE '/Users/azizbek.ussenov/mysql_uploads/DISPOSITION.csv'
INTO TABLE DISPOSITION_STAGING 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES;

-----
SET SQL_SAFE_UPDATES = 0;
DELETE FROM DISPOSITION_STAGING
WHERE client_id NOT IN (SELECT client_id FROM CLIENT)
   OR account_id NOT IN (SELECT account_id FROM ACCOUNT);
SET SQL_SAFE_UPDATES = 1;

INSERT INTO DISPOSITION (disp_id, client_id, account_id, type)
SELECT disp_id, client_id, account_id, type
FROM DISPOSITION_STAGING;

DROP TEMPORARY TABLE DISPOSITION_STAGING;
------

-- 5. LOAN table
DROP TABLE IF EXISTS LOAN;
CREATE TABLE LOAN (
    loan_id INT NOT NULL PRIMARY KEY,
    account_id INT,
    date DATE,
    amount DECIMAL,
    duration INT,
    payments DECIMAL,
    status VARCHAR(40) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES ACCOUNT(account_id)
);
----------------
CREATE TEMPORARY TABLE LOAN_STAGING LIKE LOAN;

----------------
-- Loading data
LOAD DATA INFILE '/Users/azizbek.ussenov/mysql_uploads/LOAN.csv'
INTO TABLE LOAN_STAGING 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;
---------------
SET SQL_SAFE_UPDATES = 0;
DELETE FROM LOAN_STAGING
WHERE account_id NOT IN (SELECT account_id FROM ACCOUNT);
SET SQL_SAFE_UPDATES = 1;

INSERT INTO LOAN (loan_id, account_id, date, amount, duration, payments, status)
SELECT loan_id, account_id, date, amount, duration, payments, status
FROM LOAN_STAGING;

DROP TEMPORARY TABLE LOAN_STAGING;
---------------
-- 6. ORDER table
DROP TABLE IF EXISTS ORDERS;
CREATE TABLE ORDERS (
    order_id INT NOT NULL PRIMARY KEY,
    account_id INT,
    bank_to VARCHAR(40) NOT NULL,
    account_to INT NOT NULL,
    amount DECIMAL,
    K_symbol VARCHAR(255),
	FOREIGN KEY (account_id) REFERENCES ACCOUNT(account_id)
);

----------
CREATE TEMPORARY TABLE ORDERS_STAGING LIKE ORDERS;
----------

-- Loading data
LOAD DATA INFILE '/Users/azizbek.ussenov/mysql_uploads/ORDER.csv'
INTO TABLE ORDERS_STAGING 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES;
---------
SET SQL_SAFE_UPDATES = 0;
DELETE FROM ORDERS_STAGING
WHERE account_id NOT IN (SELECT account_id FROM ACCOUNT);
SET SQL_SAFE_UPDATES = 1;

INSERT INTO ORDERS (order_id, account_id, bank_to, account_to, amount, K_symbol)
SELECT order_id, account_id, bank_to, account_to, amount, K_symbol
FROM ORDERS_STAGING;

DROP TEMPORARY TABLE ORDERS_STAGING;
---------

-- 7. TRANSACTION table
DROP TABLE IF EXISTS TRANSACTION;
CREATE TABLE TRANSACTION (
    trans_id INT NOT NULL PRIMARY KEY,
    account_id INT,
    date DATE,
    type VARCHAR(255),
    operation_desc VARCHAR(255),
    amount DECIMAL,
    balance DECIMAL,
    K_symbol VARCHAR(255),
    bank VARCHAR(255),
    account DECIMAL,
	FOREIGN KEY (account_id) REFERENCES ACCOUNT(account_id)
);
-- Loading data
LOAD DATA INFILE '/Users/azizbek.ussenov/mysql_uploads/TRANSACTION.csv'
INTO TABLE TRANSACTION 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
(trans_id, account_id, date, type, @op_var, amount, balance, @K_symbol_var, @bank_var, @account_var)
SET
`operation_desc` = NULLIF(@op_var, ''),
`K_symbol` = NULLIF(@K_symbol_var, ''),
`bank` = NULLIF(@bank_var, ''),
`account` = NULLIF(@account_var, '')
;


-- 8. CREDIT CARD table
DROP TABLE IF EXISTS CREDIT_CARD;
CREATE TABLE CREDIT_CARD (
    card_id INT NOT NULL PRIMARY KEY,
    disp_id INT,
    type VARCHAR(255),
    issue DATE,
	FOREIGN KEY (disp_id) REFERENCES DISPOSITION(disp_id)
);

------
CREATE TEMPORARY TABLE CREDIT_CARD_STAGING LIKE CREDIT_CARD;
------

-- Loading data
LOAD DATA INFILE '/Users/azizbek.ussenov/mysql_uploads/CREDIT_CARD.csv'
INTO TABLE CREDIT_CARD_STAGING 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

--------
SET SQL_SAFE_UPDATES = 0;
DELETE FROM CREDIT_CARD_STAGING
WHERE disp_id NOT IN (SELECT disp_id FROM DISPOSITION);
SET SQL_SAFE_UPDATES = 1;

INSERT INTO CREDIT_CARD (card_id, disp_id, type, issue)
SELECT card_id, disp_id, type, issue
FROM CREDIT_CARD_STAGING;

DROP TEMPORARY TABLE CREDIT_CARD_STAGING;

######################
-- ETL 1: LOAN SUMMARY
######################

DROP PROCEDURE IF EXISTS loan_analysis_etl;
DELIMITER //

CREATE PROCEDURE loan_analysis_etl()
BEGIN
    DROP TABLE IF EXISTS loan_summary;
    CREATE TABLE loan_summary AS
    SELECT 
        d.client_id,
        l.account_id,
        SUM(l.amount) AS total_loan_amount,
        AVG(l.duration) AS avg_loan_duration,
        COUNT(*) AS loan_count,
        SUM(CASE WHEN l.status = 'D' THEN 1 ELSE 0 END) AS loans_in_debt
    FROM 
        LOAN l
    -- Join ACCOUNT to link loans to accounts
    JOIN ACCOUNT a ON l.account_id = a.account_id
    -- Join DISPOSITION to link accounts to clients
    JOIN DISPOSITION d ON a.account_id = d.account_id
    GROUP BY 
        d.client_id, l.account_id;
END //
DELIMITER ;

CALL loan_analysis_etl();

-- ETL 2: Disposition Analysis

DROP PROCEDURE IF EXISTS disposition_analysis_etl;
DELIMITER //

CREATE PROCEDURE disposition_analysis_etl()
BEGIN
    DROP TABLE IF EXISTS disposition_analysis;

    -- Aggregate disposition data for each client and loan type
    CREATE TABLE disposition_analysis AS
    SELECT client_id, COUNT(*) AS total_dispositions,
           SUM(CASE WHEN type = 'owner' THEN 1 ELSE 0 END) AS owner_dispositions,
           SUM(CASE WHEN type = 'user' THEN 1 ELSE 0 END) AS user_dispositions
    FROM DISPOSITION
    GROUP BY client_id;
END //

DELIMITER ;
CALL disposition_analysis_etl();

######################
-- ANALYTICAL LAYER
######################
---------------
DROP PROCEDURE IF EXISTS create_financial_analytics;
DELIMITER //

CREATE PROCEDURE create_financial_analytics()
BEGIN
    DROP TABLE IF EXISTS financial_analytics;
    CREATE TABLE financial_analytics AS
    SELECT 
        -- Account Information (Main Table)
        a.account_id,
        a.date AS account_creation_date,
        a.frequency AS account_frequency,

        -- Loan Information (LEFT JOIN to retain accounts even without loans)
        IFNULL(l.amount, 0) AS loan_amount,
        IFNULL(l.duration, 0) AS loan_duration,
        CASE 
            WHEN l.status = 'A' THEN 'No problems'
            WHEN l.status = 'B' THEN 'Loan not paid'
            WHEN l.status = 'C' THEN 'Running, OK'
            WHEN l.status = 'D' THEN 'Client in debt'
            ELSE 'No Loan'
        END AS loan_status,

        -- Client Information (via DISPOSITION to link accounts to clients)
        c.client_id,
        c.birth_number AS client_birth_year,
        c.gender AS client_gender,

        -- Location Information from DEMOGRAPHIC (LEFT JOIN with NULL replacement)
        IFNULL(d.A3, 'Unknown') AS client_region,            -- Assuming A3 represents the region
        IFNULL(d.A10, 0) AS urban_inhabitants,               -- Assuming A10 is the ratio of urban inhabitants
        IFNULL(d.A11, 0) AS average_salary,                  -- Assuming A11 is the average salary
        IFNULL(d.A12, 0) AS unemployment_rate_1995,          -- Assuming A12 is the unemployment rate in 1995
        IFNULL(d.A13, 0) AS unemployment_rate_1996, 

        -- Transaction Information (LEFT JOIN with NULL replacement)
        IFNULL(SUM(t.amount), 0) AS transaction_total,
        IFNULL(MAX(CASE 
            WHEN t.type = 'PRIJEM' THEN 'Credit'
            WHEN t.type = 'VYDAJ' THEN 'Debit'
        END), 'No Transactions') AS transaction_type,        -- Summarized to show a representative type

        -- Credit Card Information (LEFT JOIN with NULL replacement)
        IFNULL(cc.type, 'No Card') AS credit_card_type,

        -- Order Information (LEFT JOIN with NULL replacement)
        IFNULL(SUM(o.amount), 0) AS order_total_amount

    FROM 
        ACCOUNT a

    -- Join LOAN to include loan information (optional)
    LEFT JOIN LOAN l ON a.account_id = l.account_id

    -- Join DISPOSITION to link accounts to clients
    LEFT JOIN DISPOSITION disp ON a.account_id = disp.account_id

    -- Join CLIENT to get client details (optional via DISPOSITION)
    LEFT JOIN CLIENT c ON disp.client_id = c.client_id

    -- Join DEMOGRAPHIC to get region and other demographic details based on district_id (optional)
    LEFT JOIN DEMOGRAPHIC d ON c.district_id = d.A1 -- Assuming A1 is the district_id in DEMOGRAPHIC

    -- Join TRANSACTION to get transaction details per account (optional)
    LEFT JOIN `TRANSACTION` t ON a.account_id = t.account_id

    -- Join CREDIT_CARD to get credit card type for each client (optional)
    LEFT JOIN CREDIT_CARD cc ON disp.disp_id = cc.disp_id

    -- Join ORDER to get order amount details for each account (optional)
    LEFT JOIN ORDERS o ON a.account_id = o.account_id

    GROUP BY 
        a.account_id, l.loan_id, c.client_id, cc.card_id,
        a.date, a.frequency, l.amount, l.duration, l.status, 
        c.birth_number, d.A3, d.A10, d.A11, d.A12, d.A13, cc.type;

END //

DELIMITER ;
CALL create_financial_analytics();

####################
-- Trigger for Analytical Table
####################
DROP TRIGGER IF EXISTS update_financial_analytics_after_insert;
DELIMITER //

CREATE TRIGGER update_financial_analytics_after_insert
AFTER INSERT ON LOAN
FOR EACH ROW
BEGIN
    -- Check if the account already exists in financial_analytics table
    IF EXISTS (SELECT 1 FROM financial_analytics WHERE account_id = NEW.account_id) THEN
        -- Update the loan information for the existing account in financial_analytics
        UPDATE financial_analytics
        SET 
            loan_amount = NEW.amount,
            loan_duration = NEW.duration,
            loan_status = CASE 
                            WHEN NEW.status = 'A' THEN 'No problems'
                            WHEN NEW.status = 'B' THEN 'Loan not paid'
                            WHEN NEW.status = 'C' THEN 'Running, OK'
                            WHEN NEW.status = 'D' THEN 'Client in debt'
                            ELSE 'No Loan'
                          END
        WHERE account_id = NEW.account_id;
    ELSE
        -- If the account doesn't exist, insert a new record into financial_analytics
        INSERT INTO financial_analytics (
            account_id, loan_amount, loan_duration, loan_status
        )
        VALUES (
            NEW.account_id,
            NEW.amount,
            NEW.duration,
            CASE 
                WHEN NEW.status = 'A' THEN 'No problems'
                WHEN NEW.status = 'B' THEN 'Loan not paid'
                WHEN NEW.status = 'C' THEN 'Running, OK'
                WHEN NEW.status = 'D' THEN 'Client in debt'
                ELSE 'No Loan'
            END
        );
    END IF;
END //

DELIMITER ;

###################
-- Data Marts
###################

-- Data Mart 1: Loan Analysis
DROP VIEW IF EXISTS loan_analysis_view;
CREATE VIEW loan_analysis_view AS
SELECT 
    account_id,
    loan_status,
    COUNT(*) AS loan_count,
    AVG(loan_amount) AS avg_loan_amount,
    AVG(loan_duration) AS avg_loan_duration,
    SUM(CASE WHEN loan_status = 'Client in debt' THEN 1 ELSE 0 END) AS clients_in_debt
FROM 
    financial_analytics
GROUP BY 
    account_id, loan_status;

-- Data Mart 2: Transaction Summary
DROP VIEW IF EXISTS transaction_summary_view;
CREATE VIEW transaction_summary_view AS
SELECT 
    account_id,
    transaction_type,
    COUNT(transaction_type) AS transaction_count,
    AVG(transaction_total) AS avg_transaction_amount
FROM 
    financial_analytics
GROUP BY 
    account_id, transaction_type;

-- Data Mart 3: Client Demographics
DROP VIEW IF EXISTS client_demographics_view;
CREATE VIEW client_demographics_view AS
SELECT 
    client_region,
    COUNT(client_id) AS total_clients,
    AVG(average_salary) AS avg_salary,
    AVG(unemployment_rate_1995) AS avg_unemployment_rate_1995,
    AVG(unemployment_rate_1996) AS avg_unemployment_rate_1996,
    AVG(urban_inhabitants) AS avg_urban_inhabitants_ratio
FROM 
    financial_analytics
GROUP BY 
    client_region;

#####################
-- Materialized View
#####################
-- Step 1: Creating a Materialized View through creating a table
DROP TABLE IF EXISTS account_activity_summary;
CREATE TABLE account_activity_summary AS
SELECT 
    account_id,
    account_creation_date,
    account_frequency,
    SUM(transaction_total) AS total_transactions,
    AVG(order_total_amount) AS avg_order_amount,
    MAX(CASE WHEN loan_amount > 0 THEN 'Has Loan' ELSE 'No Loan' END) AS loan_presence,
    MAX(CASE WHEN transaction_total > 0 THEN 'Active' ELSE 'Inactive' END) AS account_status
FROM 
    financial_analytics
GROUP BY 
    account_id, account_creation_date, account_frequency;
    
-- Step 2: Setting Up a Refresh Mechanism
DROP EVENT IF EXISTS refresh_account_activity_summary;
DELIMITER //
CREATE EVENT refresh_account_activity_summary
ON SCHEDULE EVERY 1 DAY
DO
BEGIN
    DELETE FROM account_activity_summary;
    INSERT INTO account_activity_summary
    SELECT 
        account_id,
        account_creation_date,
        account_frequency,
        SUM(transaction_total) AS total_transactions,
        AVG(order_total_amount) AS avg_order_amount,
        MAX(CASE WHEN loan_amount > 0 THEN 'Has Loan' ELSE 'No Loan' END) AS loan_presence,
        MAX(CASE WHEN transaction_total > 0 THEN 'Active' ELSE 'Inactive' END) AS account_status
    FROM 
        financial_analytics
    GROUP BY 
        account_id, account_creation_date, account_frequency;
END//

DELIMITER ;

-- Step 3: Creating Triggers for INSERT, UPDATE, DELETE

-- Insert
DROP TRIGGER IF EXISTS trg_after_insert_financial_analytics;
DELIMITER //
CREATE TRIGGER trg_after_insert_financial_analytics
AFTER INSERT ON financial_analytics
FOR EACH ROW
BEGIN
    INSERT INTO account_activity_summary (account_id, account_creation_date, account_frequency, total_transactions, avg_order_amount, loan_presence, account_status)
    VALUES (
        NEW.account_id,
        NEW.account_creation_date,
        NEW.account_frequency,
        NEW.transaction_total,
        NEW.order_total_amount,
        CASE WHEN NEW.loan_amount > 0 THEN 'Has Loan' ELSE 'No Loan' END,
        CASE WHEN NEW.transaction_total > 0 THEN 'Active' ELSE 'Inactive' END
    )
    ON DUPLICATE KEY UPDATE
        total_transactions = total_transactions + NEW.transaction_total,
        avg_order_amount = (avg_order_amount + NEW.order_total_amount) / 2,
        loan_presence = CASE WHEN NEW.loan_amount > 0 THEN 'Has Loan' ELSE loan_presence END,
        account_status = CASE WHEN NEW.transaction_total > 0 THEN 'Active' ELSE account_status END;
END//

DELIMITER ;

-- UPDATE
DROP TRIGGER IF EXISTS trg_after_update_financial_analytics;
DELIMITER //
CREATE TRIGGER trg_after_update_financial_analytics
AFTER UPDATE ON financial_analytics
FOR EACH ROW
BEGIN
    UPDATE account_activity_summary
    SET 
        total_transactions = total_transactions - OLD.transaction_total + NEW.transaction_total,
        avg_order_amount = (avg_order_amount - OLD.order_total_amount + NEW.order_total_amount) / 2,
        loan_presence = CASE WHEN NEW.loan_amount > 0 THEN 'Has Loan' ELSE loan_presence END,
        account_status = CASE WHEN NEW.transaction_total > 0 THEN 'Active' ELSE account_status END
    WHERE account_id = NEW.account_id;
END//

DELIMITER ; 

-- DELETE
DROP TRIGGER IF EXISTS trg_after_delete_financial_analytics;
DELIMITER //
CREATE TRIGGER trg_after_delete_financial_analytics
AFTER DELETE ON financial_analytics
FOR EACH ROW
BEGIN
    UPDATE account_activity_summary
    SET 
        total_transactions = total_transactions - OLD.transaction_total,
        avg_order_amount = (avg_order_amount * 2 - OLD.order_total_amount) / 2,
        loan_presence = CASE WHEN total_transactions = 0 THEN 'No Loan' ELSE loan_presence END,
        account_status = CASE WHEN total_transactions = 0 THEN 'Inactive' ELSE account_status END
    WHERE account_id = OLD.account_id;
END //

DELIMITER ;

####################################################################################################

