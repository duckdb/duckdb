#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("SQL Server functions tests", "[sqlserver]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA Sales;"));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE Sales.SalesPerson( BusinessEntityID int NOT NULL, TerritoryID int, SalesQuota decimal(22,4), "
	    "Bonus decimal(22,4) NOT NULL, CommissionPct decimal(10,4) NOT NULL, SalesYTD decimal(22,4) NOT NULL, "
	    "SalesLastYear decimal(22,4) NOT NULL , rowguid string , ModifiedDate datetime NOT NULL );"));
	REQUIRE_NO_FAIL(con.Query("COPY Sales.SalesPerson FROM 'test/sqlserver/data/SalesPerson.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query("	CREATE TABLE Sales.SalesTaxRate( SalesTaxRateID int NOT NULL, StateProvinceID int NOT "
	                          "NULL, TaxType tinyint NOT NULL, TaxRate decimal(10,4) NOT NULL , Name string NOT NULL, "
	                          "rowguid string , ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(
	    con.Query("COPY Sales.SalesTaxRate FROM 'test/sqlserver/data/SalesTaxRate.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE Sales.SalesPersonQuotaHistory( BusinessEntityID int NOT NULL, QuotaDate datetime NOT NULL, "
	    "SalesQuota decimal(22,4) NOT NULL, rowguid string , ModifiedDate datetime NOT NULL);"));
	REQUIRE_NO_FAIL(con.Query("COPY Sales.SalesPersonQuotaHistory FROM "
	                          "'test/sqlserver/data/SalesPersonQuotaHistory.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE Sales.SalesTerritory( TerritoryID int NOT NULL, Name string NOT NULL, "
	              "CountryRegionCode string NOT NULL, Group_ string NOT NULL, SalesYTD decimal(22,4) NOT NULL , "
	              "SalesLastYear decimal(22,4) NOT NULL , CostYTD decimal(22,4) NOT NULL , CostLastYear decimal(22,4) "
	              "NOT NULL , rowguid string , ModifiedDate datetime NOT NULL );"));
	REQUIRE_NO_FAIL(
	    con.Query("COPY Sales.SalesTerritory FROM 'test/sqlserver/data/SalesTerritory.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA HumanResources;"));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE HumanResources.Employee( BusinessEntityID int NOT NULL, NationalIDNumber string NOT NULL, "
	    "LoginID string NOT NULL, OrganizationNode VARCHAR, 	OrganizationLevel integer not null, JobTitle string "
	    "NOT NULL, BirthDate date NOT NULL, MaritalStatus string NOT NULL, Gender string NOT NULL, HireDate date NOT "
	    "NULL, SalariedFlag string NOT NULL , VacationHours smallint NOT NULL , SickLeaveHours smallint NOT NULL, "
	    "CurrentFlag string NOT NULL , rowguid string , ModifiedDate datetime NOT NULL); "));
	REQUIRE_NO_FAIL(
	    con.Query("COPY HumanResources.Employee FROM 'test/sqlserver/data/Employee.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE HumanResources.EmployeePayHistory( BusinessEntityID int NOT NULL, RateChangeDate datetime NOT "
	    "NULL, Rate decimal(22,4) NOT NULL, PayFrequency tinyint NOT NULL, ModifiedDate datetime NOT NULL); "));
	REQUIRE_NO_FAIL(con.Query("COPY HumanResources.EmployeePayHistory FROM "
	                          "'test/sqlserver/data/EmployeePayHistory.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE HumanResources.EmployeeDepartmentHistory( BusinessEntityID int NOT NULL, "
	                          "DepartmentID smallint NOT NULL, ShiftID tinyint NOT NULL, StartDate date NOT NULL, "
	                          "EndDate date, ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(con.Query("COPY HumanResources.EmployeeDepartmentHistory FROM "
	                          "'test/sqlserver/data/EmployeeDepartmentHistory.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE HumanResources.Department( DepartmentID smallint NOT NULL, Name string NOT "
	                          "NULL, GroupName string NOT NULL, ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(
	    con.Query("COPY HumanResources.Department FROM 'test/sqlserver/data/Department.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE HumanResources.Shift( ShiftID tinyint NOT NULL, Name string NOT NULL, StartTime string "
	              "NOT NULL, EndTime string NOT NULL, ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(con.Query("COPY HumanResources.Shift FROM 'test/sqlserver/data/Shift.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA Person;"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE Person.Person( BusinessEntityID int NOT NULL, PersonType string NOT NULL, NameStyle "
	              "string NOT NULL, Title string , FirstName string NOT NULL, MiddleName string, LastName string NOT "
	              "NULL, Suffix string, EmailPromotion int NOT NULL, AdditionalContactInfo string, Demographics "
	              "string, rowguid string, ModifiedDate datetime NOT NULL); "));
	REQUIRE_NO_FAIL(
	    con.Query("COPY Person.Person FROM 'test/sqlserver/data/Person.csv.gz' (DELIMITER '|', QUOTE '*');"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE Person.BusinessEntityAddress( BusinessEntityID int NOT NULL, AddressID int NOT NULL, "
	              "AddressTypeID int NOT NULL, rowguid string, ModifiedDate datetime NOT NULL ) ; "));
	REQUIRE_NO_FAIL(con.Query("COPY Person.BusinessEntityAddress FROM "
	                          "'test/sqlserver/data/BusinessEntityAddress.csv.gz' DELIMITER '|';"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE Person.Address( AddressID int NOT NULL, AddressLine1 string NOT NULL, AddressLine2 "
	              "string, City string NOT NULL, StateProvinceID int NOT NULL, PostalCode string NOT NULL, "
	              "SpatialLocation string, rowguid string, ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(con.Query("COPY Person.Address FROM 'test/sqlserver/data/Address.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE Person.StateProvince( StateProvinceID int NOT NULL, StateProvinceCode string NOT NULL, "
	              "CountryRegionCode string NOT NULL, IsOnlyStateProvinceFlag string NOT NULL , Name string NOT NULL, "
	              "TerritoryID int NOT NULL, rowguid string , ModifiedDate datetime NOT NULL); "));
	REQUIRE_NO_FAIL(
	    con.Query("COPY Person.StateProvince FROM 'test/sqlserver/data/StateProvince.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE Person.CountryRegion( CountryRegionCode string NOT NULL, Name string NOT "
	                          "NULL, ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(
	    con.Query("COPY Person.CountryRegion FROM 'test/sqlserver/data/CountryRegion.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE Person.EmailAddress( BusinessEntityID int NOT NULL, EmailAddressID int NOT NULL, "
	              "EmailAddress string, rowguid string , ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(
	    con.Query("COPY Person.EmailAddress FROM 'test/sqlserver/data/EmailAddress.csv.gz' DELIMITER '|';"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE Person.PersonPhone( BusinessEntityID int NOT NULL, PhoneNumber string NOT "
	                          "NULL, PhoneNumberTypeID int NOT NULL, ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(con.Query("COPY Person.PersonPhone FROM 'test/sqlserver/data/PersonPhone.csv.gz' DELIMITER '|';"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE Person.PhoneNumberType( PhoneNumberTypeID int NOT NULL, Name string NOT "
	                          "NULL, ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(
	    con.Query("COPY Person.PhoneNumberType FROM 'test/sqlserver/data/PhoneNumberType.csv.gz' DELIMITER '|';"));

	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA Production;"));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE Production.Product( ProductID int NOT NULL, Name string NOT NULL, ProductNumber string NOT NULL, "
	    "MakeFlag string NOT NULL , FinishedGoodsFlag string NOT NULL , Color string, SafetyStockLevel smallint NOT "
	    "NULL, ReorderPoint smallint NOT NULL, StandardCost decimal(22,4) NOT NULL, ListPrice decimal(22,4) NOT NULL, "
	    "Size string, SizeUnitMeasureCode string, WeightUnitMeasureCode string, Weight decimal(8, 2), "
	    "DaysToManufacture int NOT NULL, ProductLine string, Class string, Style string, ProductSubcategoryID int, "
	    "ProductModelID int, SellStartDate datetime NOT NULL, SellEndDate datetime, DiscontinuedDate datetime, rowguid "
	    "string  , ModifiedDate datetime NOT NULL );"));
	REQUIRE_NO_FAIL(con.Query("	COPY Production.Product FROM 'test/sqlserver/data/Product.csv.gz' DELIMITER '\t';"));

	REQUIRE_NO_FAIL(con.Query(" CREATE TABLE Production.ProductInventory( ProductID int NOT NULL, LocationID smallint "
	                          "NOT NULL, Shelf string NOT NULL, Bin tinyint NOT NULL, Quantity smallint NOT NULL , "
	                          "rowguid string , ModifiedDate datetime NOT NULL ); "));
	REQUIRE_NO_FAIL(con.Query(
	    "	COPY Production.ProductInventory FROM 'test/sqlserver/data/ProductInventory.csv.gz' DELIMITER '\t';"));

	// views

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW Sales.vSalesPerson AS SELECT s.BusinessEntityID ,p.Title ,p.FirstName ,p.MiddleName ,p.LastName "
	    ",p.Suffix ,e.JobTitle ,pp.PhoneNumber ,pnt.Name AS PhoneNumberType ,ea.EmailAddress ,p.EmailPromotion "
	    ",a.AddressLine1 ,a.AddressLine2 ,a.City ,sp.Name AS StateProvinceName ,a.PostalCode ,cr.Name AS "
	    "CountryRegionName ,st.Name AS TerritoryName ,st.Group_ AS TerritoryGroup ,s.SalesQuota ,s.SalesYTD "
	    ",s.SalesLastYear FROM Sales.SalesPerson s INNER JOIN HumanResources.Employee e ON e.BusinessEntityID = "
	    "s.BusinessEntityID INNER JOIN Person.Person p ON p.BusinessEntityID = s.BusinessEntityID INNER JOIN "
	    "Person.BusinessEntityAddress bea ON bea.BusinessEntityID = s.BusinessEntityID INNER JOIN Person.Address a ON "
	    "a.AddressID = bea.AddressID INNER JOIN Person.StateProvince sp ON sp.StateProvinceID = a.StateProvinceID "
	    "INNER JOIN Person.CountryRegion cr ON cr.CountryRegionCode = sp.CountryRegionCode LEFT OUTER JOIN "
	    "Sales.SalesTerritory st ON st.TerritoryID = s.TerritoryID LEFT OUTER JOIN Person.EmailAddress ea ON "
	    "ea.BusinessEntityID = p.BusinessEntityID LEFT OUTER JOIN Person.PersonPhone pp ON pp.BusinessEntityID = "
	    "p.BusinessEntityID LEFT OUTER JOIN Person.PhoneNumberType pnt ON pnt.PhoneNumberTypeID = "
	    "pp.PhoneNumberTypeID; "));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW HumanResources.vEmployeeDepartmentHistory AS SELECT e.BusinessEntityID ,p.Title "
	              ",p.FirstName ,p.MiddleName ,p.LastName ,p.Suffix ,s.Name AS Shift ,d.Name AS Department "
	              ",d.GroupName ,edh.StartDate ,edh.EndDate FROM HumanResources.Employee e INNER JOIN Person.Person p "
	              "ON p.BusinessEntityID = e.BusinessEntityID INNER JOIN HumanResources.EmployeeDepartmentHistory edh "
	              "ON e.BusinessEntityID = edh.BusinessEntityID INNER JOIN HumanResources.Department d ON "
	              "edh.DepartmentID = d.DepartmentID INNER JOIN HumanResources.Shift s ON s.ShiftID = edh.ShiftID; "));

	// code below generated using scrape.py

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql?view=sql-server-2017

	result = con.Query("SELECT AVG(VacationHours)AS a, SUM(SickLeaveHours) AS b "
	                   " FROM HumanResources.Employee WHERE JobTitle LIKE 'Vice President%';");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {25}));
	REQUIRE(CHECK_COLUMN(result, 1, {97}));

	result = con.Query("SELECT TerritoryID, AVG(Bonus)as a, SUM(SalesYTD) as b FROM "
	                   "Sales.SalesPerson GROUP BY TerritoryID order by TerritoryID; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
	REQUIRE(CHECK_COLUMN(
	    result, 1, {0.00, 4133.3333, 4100.00, 2500.00, 2775.00, 6700.00, 2750.00, 985.00, 75.00, 5650.00, 5150.00}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {1252127.9471, 4502152.2674, 3763178.1787, 3189418.3662, 6709904.1666, 2315185.611,
	                      4058260.1825, 3121616.3202, 1827066.7118, 1421810.9242, 4116871.2277}));

	result = con.Query("SELECT AVG(DISTINCT ListPrice) FROM Production.Product;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 1);
	// FIXME mismatch, probably down to missing DECIMAL type
	// REQUIRE(CHECK_COLUMN(result, 0, {437.4042}));

	result = con.Query("SELECT AVG(ListPrice) FROM Production.Product;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {438.6662}));

	result =
	    con.Query("SELECT BusinessEntityID, TerritoryID ,YEAR(ModifiedDate) AS SalesYear "
	              ",SalesYTD ,AVG(SalesYTD) OVER (PARTITION BY "
	              "TerritoryID ORDER BY YEAR(ModifiedDate) ) AS MovingAvg ,SUM(SalesYTD) "
	              "OVER (PARTITION BY TerritoryID ORDER BY YEAR(ModifiedDate) ) AS CumulativeTotal FROM "
	              "Sales.SalesPerson WHERE TerritoryID IS NULL OR TerritoryID < 5 ORDER BY TerritoryID, SalesYear;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 6);
	// TODO needs double checking, probably using different adventureworks db
	//	REQUIRE(CHECK_COLUMN(result, 0, {274, 287, 285, 283, 280, 284, 275, 277, 276, 281}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), 1, 1, 1, 2, 3, 4, 4}));
	//	REQUIRE(CHECK_COLUMN(result, 2, {2005, 2006, 2007, 2005, 2005, 2006, 2005, 2005, 2005, 2005}));
	//	REQUIRE(CHECK_COLUMN(result, 3,
	//	                     {559697.56, 519905.93, 172524.45, 1573012.94, 1352577.13, 1576562.20, 3763178.18,
	// 3189418.37, 	                      4251368.55, 2458535.62})); 	REQUIRE(CHECK_COLUMN(result, 4, {559697.56,
	// 539801.75, 417375.98, 1462795.04, 1462795.04, 1500717.42, 3763178.18, 3189418.37, 3354952.08, 3354952.08}));
	//	REQUIRE(CHECK_COLUMN(result, 5,
	//	                     {559697.56, 1079603.50, 1252127.95, 2925590.07, 2925590.07, 4502152.27, 3763178.18,
	// 3189418.37, 	                      6709904.17, 6709904.17}));
	//

	result = con.Query("SELECT BusinessEntityID, TerritoryID ,YEAR(ModifiedDate) AS SalesYear ,SalesYTD "
	                   " ,AVG(SalesYTD) OVER (ORDER BY YEAR(ModifiedDate) ) AS MovingAvg "
	                   ",SUM(SalesYTD) OVER (ORDER BY YEAR(ModifiedDate) ) AS CumulativeTotal FROM "
	                   "Sales.SalesPerson WHERE TerritoryID IS NULL OR TerritoryID < 5 ORDER BY SalesYear;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 6);
	// TODO needs double checking, probably using different adventureworks db
	//
	//	REQUIRE(CHECK_COLUMN(result, 0, {274, 275, 276, 277, 280, 281, 283, 284, 287, 285}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 4, 3, 1, 4, 1, 1, Value(), Value()}));
	//	REQUIRE(CHECK_COLUMN(result, 2, {2005, 2005, 2005, 2005, 2005, 2005, 2005, 2006, 2006, 2007}));
	//	REQUIRE(CHECK_COLUMN(result, 3,
	//	                     {559697.56, 3763178.18, 4251368.55, 3189418.37, 1352577.13, 2458535.62, 1573012.94,
	// 1576562.20, 	                      519905.93, 172524.45})); 	REQUIRE(CHECK_COLUMN(result, 4, {2449684.05,
	// 2449684.05, 2449684.05, 2449684.05, 2449684.05, 2449684.05, 2449684.05, 	                      2138250.72,
	// 2138250.72, 1941678.09})); 	REQUIRE(CHECK_COLUMN(result, 5, 	                     {17147788.35, 17147788.35,
	// 17147788.35, 17147788.35,
	// 17147788.35, 17147788.35, 17147788.35, 	                      19244256.47, 19244256.47, 19416780.93}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/count-transact-sql?view=sql-server-2017

	// TODO report bug, original query has "Title"
	// TODO casting issue
	result = con.Query("SELECT COUNT(DISTINCT JobTitle) FROM HumanResources.Employee; ");
	//	REQUIRE(result->success);
	//	REQUIRE(result->types.size() == 1);
	//	REQUIRE(CHECK_COLUMN(result, 0, {67}));

	result = con.Query("SELECT COUNT(*) FROM HumanResources.Employee; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {290}));

	result = con.Query("SELECT COUNT(*), AVG(Bonus) FROM Sales.SalesPerson WHERE SalesQuota > 25000; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {14}));
	REQUIRE(CHECK_COLUMN(result, 1, {3472.1428}));

	result =
	    con.Query("SELECT DISTINCT Name , MIN(Rate) OVER (PARTITION BY edh.DepartmentID) AS MinSalary , MAX(Rate) OVER "
	              "(PARTITION BY edh.DepartmentID) AS MaxSalary , AVG(Rate) OVER (PARTITION BY edh.DepartmentID) AS "
	              "AvgSalary ,COUNT(edh.BusinessEntityID) OVER (PARTITION BY edh.DepartmentID) AS EmployeesPerDept "
	              "FROM HumanResources.EmployeePayHistory AS eph JOIN HumanResources.EmployeeDepartmentHistory AS edh "
	              "ON eph.BusinessEntityID = edh.BusinessEntityID JOIN HumanResources.Department AS d ON "
	              "d.DepartmentID = edh.DepartmentID WHERE edh.EndDate IS NULL ORDER BY Name;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"Document Control", "Engineering", "Executive", "Facilities and Maintenance", "Finance",
	                      "Human Resources", "Information Services", "Marketing", "Production", "Production Control",
	                      "Purchasing", "Quality Assurance", "Research and Development", "Sales",
	                      "Shipping and Receiving", "Tool Design"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {10.25, 32.6923, 39.06, 9.25, 13.4615, 13.9423, 27.4038, 13.4615, 6.50, 8.62, 9.86, 10.5769,
	                      40.8654, 23.0769, 9.00, 8.62}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {17.7885, 63.4615, 125.50, 24.0385, 43.2692, 27.1394, 50.4808, 37.50, 84.1346, 24.5192, 30.00,
	                      28.8462, 50.4808, 72.1154, 19.2308, 29.8462}));
	// TODO numeric drift
	//	REQUIRE(CHECK_COLUMN(result, 3,
	//	                     {14.3884, 40.1442, 68.3034, 13.0316, 23.935, 18.0248, 34.1586, 18.4318, 13.5537, 16.7746,
	//	                      18.0202, 15.4647, 43.6731, 29.9719, 10.8718, 23.5054}));
	REQUIRE(CHECK_COLUMN(result, 4, {5, 6, 4, 7, 10, 6, 10, 11, 195, 8, 14, 6, 4, 18, 6, 6}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/max-transact-sql?view=sql-server-2017

	result = con.Query("SELECT MAX(TaxRate) FROM Sales.SalesTaxRate; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {19.60}));

	result =
	    con.Query("SELECT DISTINCT Name , MIN(Rate) OVER (PARTITION BY edh.DepartmentID) AS MinSalary , MAX(Rate) OVER "
	              "(PARTITION BY edh.DepartmentID) AS MaxSalary , AVG(Rate) OVER (PARTITION BY edh.DepartmentID) AS "
	              "AvgSalary ,COUNT(edh.BusinessEntityID) OVER (PARTITION BY edh.DepartmentID) AS EmployeesPerDept "
	              "FROM HumanResources.EmployeePayHistory AS eph JOIN HumanResources.EmployeeDepartmentHistory AS edh "
	              "ON eph.BusinessEntityID = edh.BusinessEntityID JOIN HumanResources.Department AS d ON "
	              "d.DepartmentID = edh.DepartmentID WHERE edh.EndDate IS NULL ORDER BY Name;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"Document Control", "Engineering", "Executive", "Facilities and Maintenance", "Finance",
	                      "Human Resources", "Information Services", "Marketing", "Production", "Production Control",
	                      "Purchasing", "Quality Assurance", "Research and Development", "Sales",
	                      "Shipping and Receiving", "Tool Design"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {10.25, 32.6923, 39.06, 9.25, 13.4615, 13.9423, 27.4038, 13.4615, 6.50, 8.62, 9.86, 10.5769,
	                      40.8654, 23.0769, 9.00, 8.62}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {17.7885, 63.4615, 125.50, 24.0385, 43.2692, 27.1394, 50.4808, 37.50, 84.1346, 24.5192, 30.00,
	                      28.8462, 50.4808, 72.1154, 19.2308, 29.8462}));
	// TODO numeric drift
	//	REQUIRE(CHECK_COLUMN(result, 3,
	//	                     {14.3884, 40.1442, 68.3034, 13.0316, 23.935, 18.0248, 34.1586, 18.4318, 13.5537, 16.7746,
	//	                      18.0202, 15.4647, 43.6731, 29.9719, 10.8718, 23.5054}));
	REQUIRE(CHECK_COLUMN(result, 4, {5, 6, 4, 7, 10, 6, 10, 11, 195, 8, 14, 6, 4, 18, 6, 6}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/min-transact-sql?view=sql-server-2017

	result = con.Query("SELECT MIN(TaxRate) FROM Sales.SalesTaxRate; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {5.00}));

	result =
	    con.Query("SELECT DISTINCT Name , MIN(Rate) OVER (PARTITION BY edh.DepartmentID) AS MinSalary , MAX(Rate) OVER "
	              "(PARTITION BY edh.DepartmentID) AS MaxSalary , AVG(Rate) OVER (PARTITION BY edh.DepartmentID) AS "
	              "AvgSalary ,COUNT(edh.BusinessEntityID) OVER (PARTITION BY edh.DepartmentID) AS EmployeesPerDept "
	              "FROM HumanResources.EmployeePayHistory AS eph JOIN HumanResources.EmployeeDepartmentHistory AS edh "
	              "ON eph.BusinessEntityID = edh.BusinessEntityID JOIN HumanResources.Department AS d ON "
	              "d.DepartmentID = edh.DepartmentID WHERE edh.EndDate IS NULL ORDER BY Name;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"Document Control", "Engineering", "Executive", "Facilities and Maintenance", "Finance",
	                      "Human Resources", "Information Services", "Marketing", "Production", "Production Control",
	                      "Purchasing", "Quality Assurance", "Research and Development", "Sales",
	                      "Shipping and Receiving", "Tool Design"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {10.25, 32.6923, 39.06, 9.25, 13.4615, 13.9423, 27.4038, 13.4615, 6.50, 8.62, 9.86, 10.5769,
	                      40.8654, 23.0769, 9.00, 8.62}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {17.7885, 63.4615, 125.50, 24.0385, 43.2692, 27.1394, 50.4808, 37.50, 84.1346, 24.5192, 30.00,
	                      28.8462, 50.4808, 72.1154, 19.2308, 29.8462}));
	// TODO numeric drift
	//	REQUIRE(CHECK_COLUMN(result, 3,
	//	                     {14.3884, 40.1442, 68.3034, 13.0316, 23.935, 18.0248, 34.1586, 18.4318, 13.5537, 16.7746,
	//	                      18.0202, 15.4647, 43.6731, 29.9719, 10.8718, 23.5054}));
	REQUIRE(CHECK_COLUMN(result, 4, {5, 6, 4, 7, 10, 6, 10, 11, 195, 8, 14, 6, 4, 18, 6, 6}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/stdev-transact-sql?view=sql-server-2017

	// TODO unclear what the result is
	//	result = con.Query("SELECT STDEV(Bonus) FROM Sales.SalesPerson; ");
	//	REQUIRE(result->success);
	//	REQUIRE(result->types.size() == 2);
	//	REQUIRE(CHECK_COLUMN(result, 0, {398974.27}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {398450.57}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/sum-transact-sql?view=sql-server-2017

	result = con.Query("SELECT Color, SUM(ListPrice), SUM(StandardCost) FROM Production.Product WHERE Color IS NOT "
	                   "NULL AND ListPrice != 0.00 AND Name LIKE 'Mountain%' GROUP BY Color ORDER BY Color; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {"Black", "Silver", "White"}));
	REQUIRE(CHECK_COLUMN(result, 1, {27404.84, 26462.84, 19.00}));
	REQUIRE(CHECK_COLUMN(result, 2, {15214.9616, 14665.6792, 6.7926}));
	// TODO report error on third column, first val. in doc its 5214.9616

	result =
	    con.Query("SELECT BusinessEntityID, TerritoryID ,YEAR(ModifiedDate) AS SalesYear "
	              ",SalesYTD ,AVG(SalesYTD) OVER (PARTITION BY "
	              "TerritoryID ORDER BY YEAR(ModifiedDate) ) AS MovingAvg ,SUM(SalesYTD) "
	              "OVER (PARTITION BY TerritoryID ORDER BY YEAR(ModifiedDate) ) AS CumulativeTotal FROM "
	              "Sales.SalesPerson WHERE TerritoryID IS NULL OR TerritoryID < 5 ORDER BY TerritoryID,SalesYear;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 6);
	// TODO needs double checking, probably using different adventureworks db
	//
	//	REQUIRE(CHECK_COLUMN(result, 0, {274, 287, 285, 283, 280, 284, 275, 277, 276, 281}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), 1, 1, 1, 2, 3, 4, 4}));
	//	REQUIRE(CHECK_COLUMN(result, 2, {2005, 2006, 2007, 2005, 2005, 2006, 2005, 2005, 2005, 2005}));
	//	REQUIRE(CHECK_COLUMN(result, 3,
	//	                     {559697.56, 519905.93, 172524.45, 1573012.94, 1352577.13, 1576562.20, 3763178.18,
	// 3189418.37, 	                      4251368.55, 2458535.62})); 	REQUIRE(CHECK_COLUMN(result, 4, {559697.56,
	// 539801.75, 417375.98, 1462795.04, 1462795.04, 1500717.42, 3763178.18, 3189418.37, 3354952.08, 3354952.08}));
	//	REQUIRE(CHECK_COLUMN(result, 5,
	//	                     {559697.56, 1079603.50, 1252127.95, 2925590.07, 2925590.07, 4502152.27, 3763178.18,
	// 3189418.37, 	                      6709904.17, 6709904.17}));

	result = con.Query("SELECT BusinessEntityID, TerritoryID ,YEAR(ModifiedDate) AS SalesYear ,SalesYTD "
	                   " ,AVG(SalesYTD) OVER (ORDER BY YEAR(ModifiedDate) ) AS MovingAvg "
	                   ",SUM(SalesYTD) OVER (ORDER BY YEAR(ModifiedDate) ) AS CumulativeTotal FROM "
	                   "Sales.SalesPerson WHERE TerritoryID IS NULL OR TerritoryID < 5 ORDER BY SalesYear;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 6);
	// TODO needs double checking, probably using different adventureworks db
	//
	//	REQUIRE(CHECK_COLUMN(result, 0, {274, 275, 276, 277, 280, 281, 283, 284, 287, 285}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 4, 3, 1, 4, 1, 1, Value(), Value()}));
	//	REQUIRE(CHECK_COLUMN(result, 2, {2005, 2005, 2005, 2005, 2005, 2005, 2005, 2006, 2006, 2007}));
	//	REQUIRE(CHECK_COLUMN(result, 3,
	//	                     {559697.56, 3763178.18, 4251368.55, 3189418.37, 1352577.13, 2458535.62, 1573012.94,
	// 1576562.20, 	                      519905.93, 172524.45})); 	REQUIRE(CHECK_COLUMN(result, 4, {2449684.05,
	// 2449684.05, 2449684.05, 2449684.05, 2449684.05, 2449684.05, 2449684.05, 	                      2138250.72,
	// 2138250.72, 1941678.09})); 	REQUIRE(CHECK_COLUMN(result, 5, 	                     {17147788.35, 17147788.35,
	// 17147788.35, 17147788.35,
	// 17147788.35, 17147788.35, 17147788.35, 	                      19244256.47, 19244256.47, 19416780.93}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/var-transact-sql?view=sql-server-2017
	// TODO not supported
	//	result = con.Query("SELECT VAR(Bonus) FROM Sales.SalesPerson; ");
	//	REQUIRE(result->success);
	//	REQUIRE(result->types.size() == 2);
	//	REQUIRE(CHECK_COLUMN(result, 0, {159180469909.18}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {158762853821.10}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/cume-dist-transact-sql?view=sql-server-2017

	// TODO fix casting issue in this query
	//	result = con.Query(" SELECT Department, LastName, Rate, CUME_DIST () OVER (PARTITION BY Department ORDER BY
	// Rate) " 	                   "AS CumeDist, PERCENT_RANK() OVER (PARTITION BY Department ORDER BY Rate ) AS PctRank
	// FROM " 	                   "HumanResources.vEmployeeDepartmentHistory AS edh INNER JOIN
	// HumanResources.EmployeePayHistory " "AS e ON e.BusinessEntityID = edh.BusinessEntityID WHERE Department IN
	// (N'Information " "Services',N'Document Control')
	// ORDER BY Department, Rate DESC;"); 	REQUIRE(result->success); 	REQUIRE(result->types.size() == 5); 	REQUIRE(
	//	    CHECK_COLUMN(result, 0,
	//	                 {"Document Control", "Document Control", "Document Control", "Document Control",
	//	                  "Document Control", "Information Services", "Information Services", "Information Services",
	//	                  "Information Services", "Information Services", "Information Services", "Information
	// Services", 	                  "Information Services", "Information Services", "Information Services"}));
	// REQUIRE(CHECK_COLUMN(result, 1,
	//	                     {"Arifin", "Norred", "Kharatishvili", "Chai", "Berge", "Trenary", "Conroy", "Ajenstat",
	//	                      "Wilson", "Sharma", "Connelly", "Berg", "Meyyappan", "Bacon", "Bueno"}));
	//	REQUIRE(CHECK_COLUMN(result, 2,
	//	                     {17.7885, 16.8269, 16.8269, 10.25, 10.25, 50.4808, 39.6635, 38.4615, 38.4615,
	// 32.4519, 32.4519, 	                      27.4038, 27.4038, 27.4038, 27.4038})); 	REQUIRE(CHECK_COLUMN(result,
	// 3, {1, 0.8, 0.8, 0.4, 0.4, 1, 0.9, 0.8, 0.8, 0.6, 0.6, 0.4, 0.4, 0.4, 0.4})); 	REQUIRE(CHECK_COLUMN(result, 4,
	// {1, 0.5, 0.5, 0, 0, 1, 0.888888888888889, 0.666666666666667, 0.666666666666667, 0.444444444444444,
	// 0.444444444444444, 0, 0, 0, 0}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/first-value-transact-sql?view=sql-server-2017

	result = con.Query(" SELECT Name, ListPrice, FIRST_VALUE(Name) OVER (ORDER BY ListPrice ASC) AS LeastExpensive "
	                   "FROM Production.Product WHERE ProductSubcategoryID = 37 ORDER BY ListPrice, Name DESC;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"Patch Kit/8 Patches", "Road Tire Tube", "Touring Tire Tube", "Mountain Tire Tube",
	                      "LL Road Tire", "ML Road Tire", "LL Mountain Tire", "Touring Tire", "ML Mountain Tire",
	                      "HL Road Tire", "HL Mountain Tire"}));
	REQUIRE(CHECK_COLUMN(result, 1, {2.29, 3.99, 4.99, 4.99, 21.49, 24.99, 24.99, 28.99, 29.99, 32.60, 35.00}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {"Patch Kit/8 Patches", "Patch Kit/8 Patches", "Patch Kit/8 Patches", "Patch Kit/8 Patches",
	                      "Patch Kit/8 Patches", "Patch Kit/8 Patches", "Patch Kit/8 Patches", "Patch Kit/8 Patches",
	                      "Patch Kit/8 Patches", "Patch Kit/8 Patches", "Patch Kit/8 Patches"}));

	// TODO fix casting issue
	//	result =
	//	    con.Query(" SELECT JobTitle, LastName, VacationHours, FIRST_VALUE(LastName) OVER (PARTITION BY JobTitle
	// ORDER " 	              "BY VacationHours ASC ROWS UNBOUNDED PRECEDING ) AS FewestVacationHours FROM
	// HumanResources.Employee " 	              "AS e INNER JOIN Person.Person AS p ON e.BusinessEntityID =
	// p.BusinessEntityID ORDER BY JobTitle;"); 	REQUIRE(result->success); 	REQUIRE(result->types.size() == 4);
	//	REQUIRE(CHECK_COLUMN(result, 0,
	//	                     {"Accountant", "Accountant", "Accounts Manager", "Accounts Payable Specialist",
	//	                      "Accounts Payable Specialist", "Accounts Receivable Specialist",
	//	                      "Accounts Receivable Specialist", "Accounts Receivable Specialist"}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {"Moreland", "Seamans", "Liu", "Tomic", "Sheperdigian", "Poe", "Spoon",
	//"Walton"})); 	REQUIRE(CHECK_COLUMN(result, 2, {58, 59, 57, 63, 64, 60, 61, 62})); 	REQUIRE(CHECK_COLUMN(result,
	// 3,
	//{"Moreland", "Moreland", "Liu", "Tomic", "Tomic", "Poe", "Poe", "Poe"}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/last-value-transact-sql?view=sql-server-2017

	// TODO fix casting issue
	result = con.Query(" SELECT Department, LastName, Rate, HireDate, LAST_VALUE(HireDate) OVER (PARTITION BY "
	                   "Department ORDER BY Rate) AS LastValue FROM HumanResources.vEmployeeDepartmentHistory AS edh "
	                   "INNER JOIN HumanResources.EmployeePayHistory AS eph ON eph.BusinessEntityID = "
	                   "edh.BusinessEntityID INNER JOIN HumanResources.Employee AS e ON e.BusinessEntityID = "
	                   "edh.BusinessEntityID WHERE Department IN (N'Information Services',N'Document Control');");
	//	REQUIRE(result->success);
	//	REQUIRE(result->types.size() == 5);
	//	REQUIRE(
	//	    CHECK_COLUMN(result, 0,
	//	                 {"Document Control", "Document Control", "Document Control", "Document Control",
	//	                  "Document Control", "Information Services", "Information Services", "Information Services",
	//	                  "Information Services", "Information Services", "Information Services", "Information
	// Services", 	                  "Information Services", "Information Services", "Information Services"}));
	// REQUIRE(CHECK_COLUMN(result, 1,
	//	                     {"Chai", "Berge", "Norred", "Kharatishvili", "Arifin", "Berg", "Meyyappan", "Bacon",
	//"Bueno", 	                      "Sharma", "Connelly", "Ajenstat", "Wilson", "Conroy", "Trenary"}));
	// REQUIRE(CHECK_COLUMN(result, 2, 	                     {10.25, 10.25, 16.8269, 16.8269, 17.7885, 27.4038,
	// 27.4038, 27.4038, 27.4038,
	// 32.4519, 32.4519, 	                      38.4615, 38.4615, 39.6635, 50.4808})); 	REQUIRE(CHECK_COLUMN(result,
	// 3,
	//	                     {"2003-02-23", "2003-03-13", "2003-04-07", "2003-01-17", "2003-02-05", "2003-03-20",
	//	                      "2003-03-07", "2003-02-12", "2003-01-24", "2003-01-05", "2003-03-27", "2003-02-18",
	//	                      "2003-02-23", "2003-03-08", "2003-01-12"}));
	//	REQUIRE(CHECK_COLUMN(result, 4,
	//	                     {"2003-03-13", "2003-03-13", "2003-01-17", "2003-01-17", "2003-02-05", "2003-01-24",
	//	                      "2003-01-24", "2003-01-24", "2003-01-24", "2003-03-27", "2003-03-27", "2003-02-23",
	//	                      "2003-02-23", "2003-03-08", "2003-01-12"}));

	// TODO date part quarter impl
	result = con.Query(
	    " SELECT BusinessEntityID, DATEPART(QUARTER,QuotaDate)AS Quarter, YEAR(QuotaDate) AS SalesYear, SalesQuota AS "
	    "QuotaThisQuarter, SalesQuota - FIRST_VALUE(SalesQuota) OVER (PARTITION BY BusinessEntityID, YEAR(QuotaDate) "
	    "ORDER BY DATEPART(QUARTER,QuotaDate) ) AS DifferenceFromFirstQuarter, SalesQuota - LAST_VALUE(SalesQuota) "
	    "OVER (PARTITION BY BusinessEntityID, YEAR(QuotaDate) ORDER BY DATEPART(QUARTER,QuotaDate) RANGE BETWEEN "
	    "CURRENT ROW AND UNBOUNDED FOLLOWING ) AS DifferenceFromLastQuarter FROM Sales.SalesPersonQuotaHistory WHERE "
	    "YEAR(QuotaDate) > 2005 AND BusinessEntityID BETWEEN 274 AND 275 ORDER BY BusinessEntityID, SalesYear, "
	    "Quarter;");
	//	REQUIRE(result->success);
	//	REQUIRE(result->types.size() == 6);
	//	REQUIRE(CHECK_COLUMN(result, 0, {274, 274, 274, 274, 274, 274, 274, 274, 274, 274,
	//	                                 275, 275, 275, 275, 275, 275, 275, 275, 275, 275}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2}));
	//	REQUIRE(CHECK_COLUMN(result, 2, {2006, 2006, 2006, 2006, 2007, 2007, 2007, 2007, 2008, 2008,
	//	                                 2006, 2006, 2006, 2006, 2007, 2007, 2007, 2007, 2008, 2008}));
	//	REQUIRE(CHECK_COLUMN(result, 3, {91000.00,  140000.00,  70000.00,   154000.00,  107000.00, 58000.00, 263000.00,
	//	                                 116000.00, 84000.00,   187000.00,  502000.00,  550000.00, 1429000.00,
	// 1324000.00, 	                                 729000.00, 1194000.00, 1575000.00, 1218000.00, 849000.00,
	// 869000.00})); 	REQUIRE(CHECK_COLUMN(result, 4, {0.00,    49000.00,  -21000.00, 63000.00,  0.00,     -49000.00,
	// 156000.00, 	                                 9000.00, 0.00,      103000.00, 0.00, 48000.00, 927000.00,
	// 822000.00, 	                                 0.00,    465000.00, 846000.00, 489000.00, 0.00,     20000.00}));
	//	REQUIRE(CHECK_COLUMN(result, 5, {-63000.00,  -14000.00,  -84000.00, 0.00,       -9000.00,   -58000.00,
	// 147000.00, 	                                 0.00,       -103000.00, 0.00,      -822000.00, -774000.00,
	// 105000.00, 0.00, 	                                 -489000.00, -24000.00, 357000.00, 0.00,       -20000.00,
	// 0.00}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/lag-transact-sql?view=sql-server-2017

	// TODO wrong years
	result =
	    con.Query(" SELECT BusinessEntityID, YEAR(QuotaDate) AS SalesYear, SalesQuota AS CurrentQuota, LAG(SalesQuota, "
	              "1,0) OVER (ORDER BY YEAR(QuotaDate)) AS PreviousQuota FROM Sales.SalesPersonQuotaHistory WHERE "
	              "BusinessEntityID = 275 and YEAR(QuotaDate) IN ('2005','2006');");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 4);
	//	REQUIRE(CHECK_COLUMN(result, 0, {275, 275, 275, 275, 275, 275}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {2005, 2005, 2006, 2006, 2006, 2006}));
	//	REQUIRE(CHECK_COLUMN(result, 2, {367000.00, 556000.00, 502000.00, 550000.00, 1429000.00, 1324000.00}));
	//	REQUIRE(CHECK_COLUMN(result, 3, {0.00, 367000.00, 556000.00, 502000.00, 550000.00, 1429000.00}));

	result = con.Query(" SELECT TerritoryName, BusinessEntityID, SalesYTD, LAG (SalesYTD, 1, 0) OVER (PARTITION BY "
	                   "TerritoryName ORDER BY SalesYTD DESC) AS PrevRepSales FROM Sales.vSalesPerson WHERE "
	                   "TerritoryName IN (N'Northwest', N'Canada') ORDER BY TerritoryName, SalesYTD DESC;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(result, 0, {"Canada", "Canada", "Northwest", "Northwest", "Northwest"}));
	REQUIRE(CHECK_COLUMN(result, 1, {282, 278, 284, 283, 280}));
	REQUIRE(CHECK_COLUMN(result, 2, {2604540.7172, 1453719.4653, 1576562.1966, 1573012.9383, 1352577.1325}));
	REQUIRE(CHECK_COLUMN(result, 3, {0.00, 2604540.7172, 0.00, 1576562.1966, 1573012.9383}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE T (a int, b int, c int);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO T VALUES (1, 1, -3), (2, 2, 4), (3, 1, NULL), (4, 3, 1), (5, 2, NULL), (6, 1, 5);"));

	result = con.Query("SELECT b, c, LAG(2*c, b*(SELECT MIN(b) FROM T), "
	                   "-c/2.0) OVER (ORDER BY a) AS i FROM T;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 1, 3, 2, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {-3, 4, Value(), 1, Value(), 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, -2, 8, -6, Value(), Value()}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/lead-transact-sql?view=sql-server-2017
	result =
	    con.Query(" SELECT BusinessEntityID, YEAR(QuotaDate) AS SalesYear, SalesQuota AS CurrentQuota, "
	              "LEAD(SalesQuota, 1,0) OVER (ORDER BY YEAR(QuotaDate)) AS NextQuota FROM "
	              "Sales.SalesPersonQuotaHistory WHERE BusinessEntityID = 275 and YEAR(QuotaDate) IN ('2005','2006');");
	REQUIRE(result->success);
	// TODO wrong years
	//	REQUIRE(result->types.size() == 4);
	//	REQUIRE(CHECK_COLUMN(result, 0, {275, 275, 275, 275, 275, 275}));
	//	REQUIRE(CHECK_COLUMN(result, 1, {2005, 2005, 2006, 2006, 2006, 2006}));
	//	REQUIRE(CHECK_COLUMN(result, 2, {367000.00, 556000.00, 502000.00, 550000.00, 1429000.00, 1324000.00}));
	//	REQUIRE(CHECK_COLUMN(result, 3, {556000.00, 502000.00, 550000.00, 1429000.00, 1324000.00, 0.00}));

	result = con.Query(" SELECT TerritoryName, BusinessEntityID, SalesYTD, LEAD (SalesYTD, 1, 0) OVER (PARTITION BY "
	                   "TerritoryName ORDER BY SalesYTD DESC) AS NextRepSales FROM Sales.vSalesPerson WHERE "
	                   "TerritoryName IN (N'Northwest', N'Canada') ORDER BY TerritoryName, BusinessEntityID DESC;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(result, 0, {"Canada", "Canada", "Northwest", "Northwest", "Northwest"}));
	REQUIRE(CHECK_COLUMN(result, 1, {282, 278, 284, 283, 280}));
	REQUIRE(CHECK_COLUMN(result, 2, {2604540.7172, 1453719.4653, 1576562.1966, 1573012.9383, 1352577.1325}));
	REQUIRE(CHECK_COLUMN(result, 3, {1453719.4653, 0.00, 1573012.9383, 1352577.1325, 0.00}));

	result = con.Query("SELECT b, c, LEAD(2*c, b*(SELECT MIN(b) FROM T), "
	                   "-c/2.0) OVER (ORDER BY a) AS i FROM T;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 1, 3, 2, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {-3, 4, Value(), 1, Value(), 5}));
	// TODO unclear whats going on
	// REQUIRE(CHECK_COLUMN(result, 2, {8, 2, 2, 0, Value(), -2}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE T;"));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/percent-rank-transact-sql?view=sql-server-2017

	result = con.Query("SELECT Department, LastName, Rate, CUME_DIST () OVER (PARTITION BY Department ORDER BY Rate) "
	                   "AS CumeDist, PERCENT_RANK() OVER (PARTITION BY Department ORDER BY Rate ) AS PctRank FROM "
	                   "HumanResources.vEmployeeDepartmentHistory AS edh INNER JOIN HumanResources.EmployeePayHistory "
	                   "AS e ON e.BusinessEntityID = edh.BusinessEntityID WHERE Department IN (N'Information "
	                   "Services',N'Document Control') ORDER BY Department, Rate DESC;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	// TODO ORDER
	//	REQUIRE(
	//	    CHECK_COLUMN(result, 0,
	//	                 {"Document Control", "Document Control", "Document Control", "Document Control",
	//	                  "Document Control", "Information Services", "Information Services", "Information Services",
	//	                  "Information Services", "Information Services", "Information Services", "Information
	// Services", 	                  "Information Services", "Information Services", "Information Services"}));
	// REQUIRE(CHECK_COLUMN(result, 1,
	//	                     {"Arifin", "Norred", "Kharatishvili", "Chai", "Berge", "Trenary", "Conroy", "Ajenstat",
	//	                      "Wilson", "Sharma", "Connelly", "Berg", "Meyyappan", "Bacon", "Bueno"}));
	//	REQUIRE(CHECK_COLUMN(result, 2,
	//	                     {17.7885, 16.8269, 16.8269, 10.25, 10.25, 50.4808, 39.6635, 38.4615, 38.4615,
	// 32.4519, 32.4519, 	                      27.4038, 27.4038, 27.4038, 27.4038})); 	REQUIRE(CHECK_COLUMN(result,
	// 3, {1, 0.8, 0.8, 0.4, 0.4, 1, 0.9, 0.8, 0.8, 0.6, 0.6, 0.4, 0.4, 0.4, 0.4})); 	REQUIRE(CHECK_COLUMN(result, 4,
	// {1, 0.5, 0.5, 0, 0, 1, 0.888888888888889, 0.666666666666667, 0.666666666666667, 0.444444444444444,
	// 0.444444444444444, 0, 0, 0, 0}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/dense-rank-transact-sql?view=sql-server-2017

	result = con.Query(
	    " SELECT i.ProductID, p.Name, i.LocationID, i.Quantity ,DENSE_RANK() OVER (PARTITION BY i.LocationID ORDER BY "
	    "i.Quantity DESC) AS Rank FROM Production.ProductInventory AS i INNER JOIN Production.Product AS p ON "
	    "i.ProductID = p.ProductID WHERE i.LocationID BETWEEN 3 AND 4 ORDER BY i.LocationID, Quantity DESC; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	REQUIRE(CHECK_COLUMN(result, 0, {494, 495, 493, 496, 492, 495, 496, 493, 492, 494}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"Paint - Silver", "Paint - Blue", "Paint - Red", "Paint - Yellow", "Paint - Black",
	                      "Paint - Blue", "Paint - Yellow", "Paint - Red", "Paint - Black", "Paint - Silver"}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 3, 3, 3, 3, 4, 4, 4, 4, 4}));
	REQUIRE(CHECK_COLUMN(result, 3, {49, 49, 41, 30, 17, 35, 25, 24, 14, 12}));
	REQUIRE(CHECK_COLUMN(result, 4, {1, 1, 2, 3, 4, 1, 2, 3, 4, 5}));

	result = con.Query(" SELECT BusinessEntityID, Rate, DENSE_RANK() OVER (ORDER BY Rate DESC) AS RankBySalary "
	                   "FROM HumanResources.EmployeePayHistory order by Rate DESC, BusinessEntityID DESC LIMIT 10;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 3);
	REQUIRE(
	    CHECK_COLUMN(result, 0, {1, 25, 273, 2, 234, 263, 7, 234, 287, 285})); // fixed because ambiguity with rank 8
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {125.50, 84.1346, 72.1154, 63.4615, 60.0962, 50.4808, 50.4808, 48.5577, 48.101, 48.101}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 4, 5, 6, 6, 7, 8, 8}));

	result = con.Query(
	    " SELECT p.FirstName, p.LastName ,ROW_NUMBER() OVER (ORDER BY a.PostalCode) AS \"Row Number\" ,RANK() OVER "
	    "(ORDER BY a.PostalCode) AS Rank ,DENSE_RANK() OVER (ORDER BY a.PostalCode) AS \"Dense Rank\" ,NTILE(4) OVER "
	    "(ORDER BY a.PostalCode) AS Quartile ,s.SalesYTD ,a.PostalCode FROM Sales.SalesPerson AS s INNER JOIN "
	    "Person.Person AS p ON s.BusinessEntityID = p.BusinessEntityID INNER JOIN Person.Address AS a ON a.AddressID = "
	    "p.BusinessEntityID WHERE TerritoryID IS NOT NULL AND SalesYTD <> 0;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 8);
	// TODO order with postal code highly underspec

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/ntile-transact-sql?view=sql-server-2017

	result =
	    con.Query(" SELECT p.FirstName, p.LastName ,NTILE(4) OVER(ORDER BY SalesYTD DESC) AS Quartile "
	              ",s.SalesYTD AS SalesYTD , a.PostalCode FROM Sales.SalesPerson AS s INNER "
	              "JOIN Person.Person AS p ON s.BusinessEntityID = p.BusinessEntityID INNER JOIN Person.Address AS a "
	              "ON a.AddressID = p.BusinessEntityID WHERE TerritoryID IS NOT NULL AND SalesYTD <> 0; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"Linda", "Jae", "Michael", "Jillian", "Ranjit", "José", "Shu", "Tsvi", "Rachel", "Tete",
	                      "David", "Garrett", "Lynn", "Pamela"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"Mitchell", "Pak", "Blythe", "Carson", "Varkey Chudukatil", "Saraiva", "Ito", "Reiter",
	                      "Valdez", "Mensa-Annan", "Campbell", "Vargas", "Tsoflias", "Ansman-Wolfe"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 4}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {4251368.55, 4116871.23, 3763178.18, 3189418.37, 3121616.32, 2604540.72, 2458535.62,
	                      2315185.61, 1827066.71, 1576562.20, 1573012.94, 1453719.47, 1421810.92, 1352577.13}));
	REQUIRE(CHECK_COLUMN(result, 4,
	                     {"98027", "98055", "98027", "98027", "98055", "98055", "98055", "98027", "98055", "98055",
	                      "98055", "98027", "98055", "98027"}));

	result = con.Query(
	    " SELECT p.FirstName, p.LastName ,NTILE(4) OVER(PARTITION BY PostalCode ORDER BY SalesYTD DESC) AS "
	    "Quartile ,s.SalesYTD AS SalesYTD ,a.PostalCode FROM Sales.SalesPerson AS s INNER JOIN "
	    "Person.Person AS p ON s.BusinessEntityID = p.BusinessEntityID INNER JOIN Person.Address AS a ON a.AddressID = "
	    "p.BusinessEntityID WHERE TerritoryID IS NOT NULL AND SalesYTD <> 0; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"Linda", "Michael", "Jillian", "Tsvi", "Garrett", "Pamela", "Jae", "Ranjit", "José", "Shu",
	                      "Rachel", "Tete", "David", "Lynn"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"Mitchell", "Blythe", "Carson", "Reiter", "Vargas", "Ansman-Wolfe", "Pak",
	                      "Varkey Chudukatil", "Saraiva", "Ito", "Valdez", "Mensa-Annan", "Campbell", "Tsoflias"}));
	// FIXME something wrong here with NTILE
	// REQUIRE(CHECK_COLUMN(result, 2, {1, 1, 2, 2, 3, 4, 1, 1, 2, 2, 3, 3, 4, 4}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {4251368.55, 3763178.18, 3189418.37, 2315185.61, 1453719.47, 1352577.13, 4116871.23,
	                      3121616.32, 2604540.72, 2458535.62, 1827066.71, 1576562.20, 1573012.94, 1421810.92}));
	REQUIRE(CHECK_COLUMN(result, 4,
	                     {"98027", "98027", "98027", "98027", "98027", "98027", "98055", "98055", "98055", "98055",
	                      "98055", "98055", "98055", "98055"}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/rank-transact-sql?view=sql-server-2017

	result =
	    con.Query(" SELECT i.ProductID, p.Name, i.LocationID, i.Quantity ,RANK() OVER (PARTITION BY i.LocationID ORDER "
	              "BY i.Quantity DESC) AS Rank FROM Production.ProductInventory AS i INNER JOIN Production.Product AS "
	              "p ON i.ProductID = p.ProductID WHERE i.LocationID BETWEEN 3 AND 4 ORDER BY i.LocationID, i.Quantity "
	              "DESC, i.ProductID; ");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	REQUIRE(CHECK_COLUMN(result, 0, {494, 495, 493, 496, 492, 495, 496, 493, 492, 494}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"Paint - Silver", "Paint - Blue", "Paint - Red", "Paint - Yellow", "Paint - Black",
	                      "Paint - Blue", "Paint - Yellow", "Paint - Red", "Paint - Black", "Paint - Silver"}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 3, 3, 3, 3, 4, 4, 4, 4, 4}));
	REQUIRE(CHECK_COLUMN(result, 3, {49, 49, 41, 30, 17, 35, 25, 24, 14, 12}));
	REQUIRE(CHECK_COLUMN(result, 4, {1, 1, 3, 4, 5, 1, 2, 3, 4, 5}));

	result = con.Query(" SELECT BusinessEntityID, Rate, RANK() OVER (ORDER BY Rate DESC) AS RankBySalary FROM "
	                   "HumanResources.EmployeePayHistory AS eph1 WHERE RateChangeDate = (SELECT MAX(RateChangeDate) "
	                   "FROM HumanResources.EmployeePayHistory AS eph2 WHERE eph1.BusinessEntityID = "
	                   "eph2.BusinessEntityID) ORDER BY BusinessEntityID LIMIT 10;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {125.50, 63.4615, 43.2692, 29.8462, 32.6923, 32.6923, 50.4808, 40.8654, 40.8654, 42.4808}));
	// TODO: fix an initialization issue with RANK here
	//	REQUIRE(CHECK_COLUMN(result, 2, {1, 4, 8, 19, 16, 16, 6, 10, 10, 9}));

	// FROM https://docs.microsoft.com/en-us/sql/t-sql/functions/row-number-transact-sql?view=sql-server-2017

	result = con.Query(" SELECT ROW_NUMBER() OVER(ORDER BY SalesYTD DESC) AS Row, FirstName, LastName, SalesYTD FROM "
	                   "Sales.vSalesPerson WHERE TerritoryName IS NOT NULL AND SalesYTD <> 0;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"Linda", "Jae", "Michael", "Jillian", "Ranjit", "José", "Shu", "Tsvi", "Rachel", "Tete",
	                      "David", "Garrett", "Lynn", "Pamela"}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {"Mitchell", "Pak", "Blythe", "Carson", "Varkey Chudukatil", "Saraiva", "Ito", "Reiter",
	                      "Valdez", "Mensa-Annan", "Campbell", "Vargas", "Tsoflias", "Ansman-Wolfe"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {4251368.54, 4116871.22, 3763178.17, 3189418.36, 3121616.32, 2604540.71, 2458535.61,
	                      2315185.61, 1827066.71, 1576562.19, 1573012.93, 1453719.46, 1421810.92, 1352577.13}));

	result = con.Query(" SELECT FirstName, LastName, TerritoryName,  SalesYTD, ROW_NUMBER() "
	                   "OVER(PARTITION BY TerritoryName ORDER BY SalesYTD DESC) AS Row FROM Sales.vSalesPerson WHERE "
	                   "TerritoryName IS NOT NULL AND SalesYTD <> 0 ORDER BY TerritoryName, SalesYTD DESC;");
	REQUIRE(result->success);
	REQUIRE(result->types.size() == 5);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"Lynn", "José", "Garrett", "Jillian", "Ranjit", "Rachel", "Michael", "Tete", "David",
	                      "Pamela", "Tsvi", "Linda", "Shu", "Jae"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"Tsoflias", "Saraiva", "Vargas", "Carson", "Varkey Chudukatil", "Valdez", "Blythe",
	                      "Mensa-Annan", "Campbell", "Ansman-Wolfe", "Reiter", "Mitchell", "Ito", "Pak"}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {"Australia", "Canada", "Canada", "Central", "France", "Germany", "Northeast", "Northwest",
	                      "Northwest", "Northwest", "Southeast", "Southwest", "Southwest", "United Kingdom"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {1421810.92, 2604540.71, 1453719.46, 3189418.36, 3121616.32, 1827066.71, 3763178.17,
	                      1576562.19, 1573012.93, 1352577.13, 2315185.61, 4251368.54, 2458535.61, 4116871.22}));
	REQUIRE(CHECK_COLUMN(result, 4, {1, 1, 2, 1, 1, 1, 1, 1, 2, 3, 1, 1, 2, 1}));
}
