-- DS503 Project 1, Question 4.3: Query 3

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


-- Get only necessary fields: id, age, and gender
customerIdAgeGender = FOREACH customers GENERATE ID, Age, Gender;

-- Put age into corresponding group and name this ageGroup
customerIdAgeGroupGender = FOREACH customerIdAgeGender GENERATE ID, Gender,
                CASE
                    WHEN Age >= 10 AND Age < 20 THEN '[10 to 20)'
                    WHEN Age >= 20 AND Age < 30 THEN '[20 to 30)'
                    WHEN Age >= 30 AND Age < 40 THEN '[30 to 40)'
                    WHEN Age >= 40 AND Age < 50 THEN '[40 to 50)'
                    WHEN Age >= 50 AND Age < 60 THEN '[50 to 60)'
                    ELSE '[60 to 70)'
                END AS ageGroup;

-- Get only necessary fields: CustID, TransTotal
transactionCustIDTransTotal = FOREACH transactions GENERATE CustID, TransTotal;

-- Join Customers and Transactions datasets on ID and CustID
joinedCustTrans = JOIN customerIdAgeGroupGender BY ID, transactionCustIDTransTotal BY CustID;

-- Group by ageGroup and gender
groupByAgeGroupGender = GROUP joinedCustTrans BY (ageGroup, Gender);

-- Calculate min trans total, max trans total, and average trans total
result = FOREACH groupByAgeGroupGender GENERATE group.ageGroup AS ageRange, group.Gender AS Gender,
            MIN(joinedCustTrans.TransTotal) AS MinTransTotal,
            MAX(joinedCustTrans.TransTotal) AS MaxTransTotal,
            AVG(joinedCustTrans.TransTotal) AS AvgTransTotal;

-- Print the result
STORE result INTO '/user/Project1/FinalOutputQ4c' using PigStorage(',');