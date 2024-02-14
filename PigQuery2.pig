-- DS503 Project 1, Question 4.2: Query 2

-- Load Customers dataset and set fields
customers = LOAD '/user/Project1/data/Customers.txt' USING PigStorage(',') AS (
    ID: int,
    Name: chararray,
    Age: int,
    Gender: chararray,
    CountryCode: int,
    Salary: float
);

-- Get only necessary fields: ID and CountryCode
customerIdCountry = FOREACH customers GENERATE ID, CountryCode;

-- Group by CountryCode
groupedByCountryCode = GROUP customerIdCountry BY CountryCode;

-- Count the number of unique customer IDs
allNumCust = FOREACH groupedByCountryCode GENERATE group AS CountryCode, COUNT(customerIdCountry.ID) AS numCust;

-- Select country codes that have greater than 5,000 customers or less than 2,000 customers
selectedNumCust = FILTER allNumCust BY numCust > 5000 OR numCust < 2000;

-- Print the country codes and their customer numbers
result = FOREACH selectedNumCust GENERATE CountryCode, numCust;
STORE result INTO '/user/Project1/FinalOutputQ4b' using PigStorage(',');