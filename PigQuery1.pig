-- DS503 Project 1, Question 4.1: Query 1

-- Load Customers dataset and set fields
customers = LOAD '/user/Project1/data/Customers.txt' USING PigStorage(',') AS (
    ID: int,
    Name: chararray,
    Age: int,
    Gender: chararray,
    CountryCode: int,
    Salary: float
);

-- Load Transactions dataset and set fields
transactions = LOAD '/user/Project1/data/Transactions.txt' USING PigStorage(',') AS (
    TransID: int,
    CustID: int,
    TransTotal: float,
    TransNumItems: int,
    TransDesc: chararray
);

-- Join Customers and Transactions datasets on ID and CustID
joinedCustTrans = JOIN customers BY ID, transactions BY CustID;

-- Group by customer and calculate the number of transactions
groupedByCustName = GROUP joinedCustTrans BY customers::Name;
allTransCount = FOREACH groupedByCustName GENERATE group AS name, COUNT(joinedCustTrans) AS TransCount;

-- Find the minimum transaction count by ordering by transactin count and selecting the minimum
MinTransCount = ORDER allTransCount BY TransCount;
MinTransCount = LIMIT MinTransCount 1;

-- Check for other customers with the minimum transaction count
allMinTransCount = FILTER allTransCount BY TransCount == MinTransCount.TransCount;

-- Print name and transaction count for all customers with the minimum transaction count
result = FOREACH allMinTransCount GENERATE name, TransCount;
STORE result INTO '/user/Project1/FinalOutputQ4a' using PigStorage(',');