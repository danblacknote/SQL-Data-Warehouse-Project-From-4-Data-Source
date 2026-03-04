-- =====================================================
-- SILVER LAYER TABLE CREATION AND DATA LOADING PROCEDURE
-- =====================================================
-- Purpose: Create silver layer tables and transform data 
--          from bronze layer with data quality improvements
-- Author: [Your Name]
-- Date: 2024-03-03
-- =====================================================

-- Drop procedure if it exists
IF OBJECT_ID('Silver1.load_silver', 'P') IS NOT NULL
    DROP PROCEDURE Silver1.load_silver;
GO

-- =====================================================
-- STORED PROCEDURE: Silver1.load_silver
-- Description: Creates and populates all silver layer tables
--              with data cleansing and transformation
-- =====================================================
CREATE PROCEDURE Silver1.load_silver
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variable for tracking start time
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @StepStartTime DATETIME;
    DECLARE @StepName NVARCHAR(100);
    
    BEGIN TRY
        -- Start transaction
        BEGIN TRANSACTION;
        
        PRINT '====================================================';
        PRINT 'Starting Silver Layer Data Load at: ' + CONVERT(VARCHAR, @StartTime, 120);
        PRINT '====================================================';
        
        -- =====================================================
        -- SECTION 1: DIMENSION TABLES
        -- =====================================================
        
        -- =====================================================
        -- 1.1 Account Table
        -- =====================================================
        SET @StepName = 'Account Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.Account', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.Account;
            PRINT '- Existing Account table dropped';
        END
        
        -- Create Account table
        CREATE TABLE Silver1.Account
        (
            Gen_AccountKey INT,
            AccountKey INT,
            ParentAccountKey NVARCHAR(50),
            AccountCodeAlternateKey INT,
            ParentAccountCodeAlternateKey NVARCHAR(50),
            AccountDescription NVARCHAR(100),
            AccountType NVARCHAR(50),
            Operator NVARCHAR(30),
            ValueType NVARCHAR(50)
        );
        PRINT '- Account table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.Account 
        (
            Gen_AccountKey,
            AccountKey,
            ParentAccountKey,
            AccountCodeAlternateKey,
            ParentAccountCodeAlternateKey,
            AccountDescription,
            AccountType,
            Operator,
            ValueType
        )
        SELECT 
            ROW_NUMBER() OVER(ORDER BY AccountKey) AS Gen_AccountKey,
            AccountKey,
            CASE 
                WHEN ParentAccountKey IS NULL THEN 'N/A'
                ELSE CAST(ParentAccountKey AS NVARCHAR(50))
            END AS ParentAccountKey,
            AccountCodeAlternateKey,
            CASE 
                WHEN ParentAccountCodeAlternateKey IS NULL THEN 'N/A'
                ELSE CAST(ParentAccountCodeAlternateKey AS NVARCHAR(50))
            END AS ParentAccountCodeAlternateKey,
            LTRIM(RTRIM(AccountDescription)) AS AccountDescription,
            ISNULL(AccountType, 'N/A') AS AccountType,
            ISNULL(Operator, 'N/A') AS Operator,
            ISNULL(ValueType, 'N/A') AS ValueType
        FROM bronz1.Account;
        
        PRINT '- Data loaded into Account table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.2 Currency Table
        -- =====================================================
        SET @StepName = 'Currency Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.currency', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.currency;
            PRINT '- Existing Currency table dropped';
        END
        
        -- Create Currency table
        CREATE TABLE Silver1.currency 
        (
            CurrencyKey INT,
            CurrencyAlternateKey NVARCHAR(50),
            CurrencyName NVARCHAR(50)
        );
        PRINT '- Currency table created';
        
        -- Insert data
        INSERT INTO Silver1.currency
        (
            CurrencyKey,
            CurrencyAlternateKey,
            CurrencyName
        )
        SELECT 
            CurrencyKey,
            LTRIM(RTRIM(CurrencyAlternateKey)) AS CurrencyAlternateKey,
            LTRIM(RTRIM(CurrencyName)) AS CurrencyName
        FROM [AdventureWorksDW2014].[dbo].[DimCurrency];
        
        PRINT '- Data loaded into Currency table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.3 Date Table (S_date)
        -- =====================================================
        SET @StepName = 'Date Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.S_date', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.S_date;
            PRINT '- Existing S_date table dropped';
        END
        
        -- Create Date table
        CREATE TABLE Silver1.S_date 
        (
            Gen_DateKey INT,
            DateKey INT,
            FullDateAlternateKey DATE,
            DayNumberOfWeek INT,
            EnglishDayNameOfWeek NVARCHAR(50),
            SpanishDayNameOfWeek NVARCHAR(50),
            FrenchDayNameOfWeek NVARCHAR(50),
            DayNumberOfMonth INT,
            DayNumberOfYear INT,
            WeekNumberOfYear INT,
            EnglishMonthName NVARCHAR(50),
            SpanishMonthName NVARCHAR(50),
            FrenchMonthName NVARCHAR(50),
            MonthNumberOfYear INT,
            CalendarQuarter INT,
            CalendarYear INT,
            CalendarSemester INT,
            FiscalQuarter INT,
            FiscalYear INT,
            FiscalSemester INT
        );
        PRINT '- S_date table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.S_date 
        (
            Gen_DateKey,
            DateKey,
            FullDateAlternateKey,
            DayNumberOfWeek,
            EnglishDayNameOfWeek,
            SpanishDayNameOfWeek,
            FrenchDayNameOfWeek,
            DayNumberOfMonth,
            DayNumberOfYear,
            WeekNumberOfYear,
            EnglishMonthName,
            SpanishMonthName,
            FrenchMonthName,
            MonthNumberOfYear,
            CalendarQuarter,
            CalendarYear,
            CalendarSemester,
            FiscalQuarter,
            FiscalYear,
            FiscalSemester
        )
        SELECT 
            ROW_NUMBER() OVER(ORDER BY DateKey) AS Gen_DateKey,
            DateKey,
            FullDateAlternateKey,
            DayNumberOfWeek,
            LTRIM(RTRIM(EnglishDayNameOfWeek)) AS EnglishDayNameOfWeek,
            LTRIM(RTRIM(SpanishDayNameOfWeek)) AS SpanishDayNameOfWeek,
            LTRIM(RTRIM(FrenchDayNameOfWeek)) AS FrenchDayNameOfWeek,
            DayNumberOfMonth,
            DayNumberOfYear,
            WeekNumberOfYear,
            LTRIM(RTRIM(EnglishMonthName)) AS EnglishMonthName,
            LTRIM(RTRIM(SpanishMonthName)) AS SpanishMonthName,
            LTRIM(RTRIM(FrenchMonthName)) AS FrenchMonthName,
            MonthNumberOfYear,
            CalendarQuarter,
            CalendarYear,
            CalendarSemester,
            FiscalQuarter,
            FiscalYear,
            FiscalSemester
        FROM bronz1.DimDate;
        
        PRINT '- Data loaded into S_date table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.4 DepartmentGroup Table
        -- =====================================================
        SET @StepName = 'DepartmentGroup Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.DepartmentGroup', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.DepartmentGroup;
            PRINT '- Existing DepartmentGroup table dropped';
        END
        
        -- Create DepartmentGroup table
        CREATE TABLE Silver1.DepartmentGroup
        (
            DepartmentGroupKey INT,
            ParentDepartmentGroupKey NVARCHAR(50),
            DepartmentGroupName NVARCHAR(50)
        );
        PRINT '- DepartmentGroup table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.DepartmentGroup
        (
            DepartmentGroupKey,
            ParentDepartmentGroupKey,
            DepartmentGroupName
        )
        SELECT 
            DepartmentGroupKey,
            CASE 
                WHEN ParentDepartmentGroupKey IS NULL THEN 'N/A'
                ELSE CAST(ParentDepartmentGroupKey AS VARCHAR(10))
            END AS ParentDepartmentGroupKey,
            ISNULL(DepartmentGroupName, 'N/A') AS DepartmentGroupName
        FROM bronz1.DimDepartmentGroup;
        
        PRINT '- Data loaded into DepartmentGroup table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.5 Employee Table
        -- =====================================================
        SET @StepName = 'Employee Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.employee', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.employee;
            PRINT '- Existing Employee table dropped';
        END
        
        -- Create Employee table
        CREATE TABLE Silver1.employee 
        (
            EmployeeKey INT,
            ParentEmployeeKey NVARCHAR(50),
            EmployeeNationalIDAlternateKey NVARCHAR(50),
            ParentEmployeeNationalIDAlternateKey NVARCHAR(50),
            SalesTerritoryKey INT,
            FirstName NVARCHAR(50),
            LastName NVARCHAR(50),
            MiddleName NVARCHAR(50),
            NameStyle INT,
            Title NVARCHAR(50),
            HireDate DATE,
            BirthDate DATE,
            LoginID NVARCHAR(50),
            EmailAddress NVARCHAR(50),
            Phone NVARCHAR(50),
            MaritalStatus NVARCHAR(50),
            EmergencyContactName NVARCHAR(50),
            EmergencyContactPhone NVARCHAR(50),
            SalariedFlag INT,
            Gender NVARCHAR(50),
            PayFrequency INT,
            BaseRate INT,
            VacationHours INT,
            SickLeaveHours INT,
            CurrentFlag INT,
            SalesPersonFlag INT,
            DepartmentName NVARCHAR(50),
            StartDate DATE,
            EndDate DATE,
            Status NVARCHAR(50),
            EmployeePhoto NVARCHAR(50)
        );
        PRINT '- Employee table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.employee 
        (
            EmployeeKey,
            ParentEmployeeKey,
            EmployeeNationalIDAlternateKey,
            ParentEmployeeNationalIDAlternateKey,
            SalesTerritoryKey,
            FirstName,
            LastName,
            MiddleName,
            NameStyle,
            Title,
            HireDate,
            BirthDate,
            LoginID,
            EmailAddress,
            Phone,
            MaritalStatus,
            EmergencyContactName,
            EmergencyContactPhone,
            SalariedFlag,
            Gender,
            PayFrequency,
            BaseRate,
            VacationHours,
            SickLeaveHours,
            CurrentFlag,
            SalesPersonFlag,
            DepartmentName,
            StartDate,
            EndDate,
            Status,
            EmployeePhoto
        )
        SELECT 
            EmployeeKey,
            CASE 
                WHEN ParentEmployeeKey IS NULL THEN 'N/A'
                ELSE CAST(ParentEmployeeKey AS NVARCHAR(10))
            END AS ParentEmployeeKey,
            ISNULL(CAST(EmployeeNationalIDAlternateKey AS NVARCHAR(50)), 'N/A') AS EmployeeNationalIDAlternateKey,
            ISNULL(ParentEmployeeNationalIDAlternateKey, 'N/A') AS ParentEmployeeNationalIDAlternateKey,
            SalesTerritoryKey,
            FirstName,
            LastName,
            ISNULL(MiddleName, 'N/A') AS MiddleName,
            NameStyle,
            ISNULL(Title, 'N/A') AS Title,
            HireDate,
            BirthDate,
            ISNULL(LoginID, 'N/A') AS LoginID,
            ISNULL(EmailAddress, 'N/A') AS EmailAddress,
            CASE 
                WHEN LEN(Phone) != 12 THEN SUBSTRING(Phone, 8, LEN(Phone))
                ELSE ISNULL(Phone, 'N/A')
            END AS Phone,
            CASE 
                WHEN UPPER(MaritalStatus) = 'M' THEN 'Married'
                WHEN UPPER(MaritalStatus) = 'S' THEN 'Single'
                ELSE 'N/A'
            END AS MaritalStatus,
            ISNULL(EmergencyContactName, 'N/A') AS EmergencyContactName,
            ISNULL(EmergencyContactPhone, 'N/A') AS EmergencyContactPhone,
            SalariedFlag,
            CASE 
                WHEN UPPER(Gender) = 'F' THEN 'Female'
                WHEN UPPER(Gender) = 'M' THEN 'Male'
                ELSE 'N/A'
            END AS Gender,
            PayFrequency,
            BaseRate,
            VacationHours,
            SickLeaveHours,
            CurrentFlag,
            SalesPersonFlag,
            ISNULL(DepartmentName, 'N/A') AS DepartmentName,
            StartDate,
            EndDate,
            ISNULL(Status, 'Former') AS Status,
            ISNULL(CAST(EmployeePhoto AS NVARCHAR(50)), 'N/A') AS EmployeePhoto
        FROM bronz1.DimEmployee;
        
        PRINT '- Data loaded into Employee table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.6 Geography Table
        -- =====================================================
        SET @StepName = 'Geography Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.Geography', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.Geography;
            PRINT '- Existing Geography table dropped';
        END
        
        -- Create Geography table
        CREATE TABLE Silver1.Geography 
        (
            GeographyKey INT,
            City NVARCHAR(50),
            StateProvinceCode NVARCHAR(50),
            StateProvinceName NVARCHAR(50),
            CountryRegionCode NVARCHAR(50),
            EnglishCountryRegionName NVARCHAR(50),
            SpanishCountryRegionName NVARCHAR(50),
            FrenchCountryRegionName NVARCHAR(50),
            PostalCode NVARCHAR(50),
            SalesTerritoryKey NVARCHAR(50),
            IpAddressLocator NVARCHAR(50)
        );
        PRINT '- Geography table created';
        
        -- Insert data
        INSERT INTO Silver1.Geography
        (
            GeographyKey,
            City,
            StateProvinceCode,
            StateProvinceName,
            CountryRegionCode,
            EnglishCountryRegionName,
            SpanishCountryRegionName,
            FrenchCountryRegionName,
            PostalCode,
            SalesTerritoryKey,
            IpAddressLocator
        )
        SELECT 
            GeographyKey,
            ISNULL(City, 'N/A') AS City,
            ISNULL(StateProvinceCode, 'N/A') AS StateProvinceCode,
            ISNULL(StateProvinceName, 'N/A') AS StateProvinceName,
            ISNULL(CountryRegionCode, 'N/A') AS CountryRegionCode,
            ISNULL(EnglishCountryRegionName, 'N/A') AS EnglishCountryRegionName,
            ISNULL(SpanishCountryRegionName, 'N/A') AS SpanishCountryRegionName,
            ISNULL(FrenchCountryRegionName, 'N/A') AS FrenchCountryRegionName,
            ISNULL(PostalCode, 'N/A') AS PostalCode,
            ISNULL(CAST(SalesTerritoryKey AS NVARCHAR(50)), 'N/A') AS SalesTerritoryKey,
            ISNULL(IpAddressLocator, 'N/A') AS IpAddressLocator
        FROM bronz1.DimGeography;
        
        PRINT '- Data loaded into Geography table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.7 Organization Table
        -- =====================================================
        SET @StepName = 'Organization Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.Organization', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.Organization;
            PRINT '- Existing Organization table dropped';
        END
        
        -- Create Organization table
        CREATE TABLE Silver1.Organization 
        (
            OrganizationKey INT,
            ParentOrganizationKey INT,
            PercentageOfOwnership NVARCHAR(50),
            OrganizationName NVARCHAR(50),
            CurrencyKey INT
        );
        PRINT '- Organization table created';
        
        -- Insert data
        INSERT INTO Silver1.Organization
        (
            OrganizationKey,
            ParentOrganizationKey,
            PercentageOfOwnership,
            OrganizationName,
            CurrencyKey
        )
        SELECT 
            OrganizationKey,
            ParentOrganizationKey,
            ISNULL(PercentageOfOwnership, 'N/A') AS PercentageOfOwnership,
            ISNULL(OrganizationName, 'N/A') AS OrganizationName,
            CurrencyKey
        FROM bronz1.DimOrganization;
        
        PRINT '- Data loaded into Organization table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.8 Product Table
        -- =====================================================
        SET @StepName = 'Product Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.Product', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.Product;
            PRINT '- Existing Product table dropped';
        END
        
        -- Create Product table
        CREATE TABLE Silver1.Product(
            ProductKey NVARCHAR(50),
            ProductAlternateKey NVARCHAR(50),
            ProductSubcategoryKey NVARCHAR(50),
            WeightUnitMeasureCode NVARCHAR(50),
            SizeUnitMeasureCode NVARCHAR(50),
            EnglishProductName NVARCHAR(100),
            SpanishProductName NVARCHAR(100),
            FrenchProductName NVARCHAR(100),
            StandardCost NVARCHAR(50),
            FinishedGoodsFlag NVARCHAR(10),
            Color NVARCHAR(50),
            SafetyStockLevel NVARCHAR(50),
            ReorderPoint NVARCHAR(50),
            ListPrice NVARCHAR(50),
            Size NVARCHAR(50),
            SizeRange NVARCHAR(50),
            Weight NVARCHAR(50),
            DaysToManufacture NVARCHAR(50),
            ProductLine NVARCHAR(50),
            DealerPrice NVARCHAR(50),
            Class NVARCHAR(50),
            Style NVARCHAR(50),
            ModelName NVARCHAR(100),
            LargePhoto NVARCHAR(MAX),
            EnglishDescription NVARCHAR(500),
            FrenchDescription NVARCHAR(500),
            ChineseDescription NVARCHAR(500),
            ArabicDescription NVARCHAR(500),
            HebrewDescription NVARCHAR(500),
            ThaiDescription NVARCHAR(500),
            GermanDescription NVARCHAR(500),
            JapaneseDescription NVARCHAR(500),
            TurkishDescription NVARCHAR(500),
            StartDate NVARCHAR(50),
            EndDate NVARCHAR(50),
            Status NVARCHAR(50)
        );
        PRINT '- Product table created';
        
        -- Insert data with comprehensive NULL handling
        INSERT INTO Silver1.Product (
            ProductKey,
            ProductAlternateKey,
            ProductSubcategoryKey,
            WeightUnitMeasureCode,
            SizeUnitMeasureCode,
            EnglishProductName,
            SpanishProductName,
            FrenchProductName,
            StandardCost,
            FinishedGoodsFlag,
            Color,
            SafetyStockLevel,
            ReorderPoint,
            ListPrice,
            Size,
            SizeRange,
            Weight,
            DaysToManufacture,
            ProductLine,
            DealerPrice,
            Class,
            Style,
            ModelName,
            LargePhoto,
            EnglishDescription,
            FrenchDescription,
            ChineseDescription,
            ArabicDescription,
            HebrewDescription,
            ThaiDescription,
            GermanDescription,
            JapaneseDescription,
            TurkishDescription,
            StartDate,
            EndDate,
            Status
        )
        SELECT 
            CAST(ProductKey AS NVARCHAR(50)) AS ProductKey,
            ISNULL(ProductAlternateKey, 'N/A') AS ProductAlternateKey,
            CASE 
                WHEN ProductSubcategoryKey IS NULL OR TRY_CAST(ProductSubcategoryKey AS NVARCHAR(50)) = '' THEN 'N/A'
                ELSE CAST(ProductSubcategoryKey AS NVARCHAR(50))
            END AS ProductSubcategoryKey,
            ISNULL(WeightUnitMeasureCode, 'N/A') AS WeightUnitMeasureCode,
            ISNULL(SizeUnitMeasureCode, 'N/A') AS SizeUnitMeasureCode,
            EnglishProductName,
            ISNULL(SpanishProductName, 'N/A') AS SpanishProductName,
            ISNULL(FrenchProductName, 'N/A') AS FrenchProductName,
            ISNULL(CAST(StandardCost AS NVARCHAR(50)), 'N/A') AS StandardCost,
            CAST(FinishedGoodsFlag AS NVARCHAR(10)) AS FinishedGoodsFlag,
            CASE 
                WHEN Color IS NULL OR Color IN ('', ' ', 'NA') THEN 'N/A'
                ELSE Color
            END AS Color,
            CAST(SafetyStockLevel AS NVARCHAR(50)) AS SafetyStockLevel,
            CAST(ReorderPoint AS NVARCHAR(50)) AS ReorderPoint,
            CASE 
                WHEN ListPrice IS NULL OR ListPrice = '' OR ListPrice = ' ' THEN 'N/A'
                ELSE CAST(ListPrice AS NVARCHAR(50))
            END AS ListPrice,
            CASE 
                WHEN Size IS NULL OR Size IN ('', ' ', 'NA') THEN 'N/A'
                ELSE Size
            END AS Size,
            CASE 
                WHEN SizeRange IS NULL OR SizeRange IN ('', ' ', 'NA') THEN 'N/A'
                ELSE SizeRange
            END AS SizeRange,
            CASE 
                WHEN Weight IS NULL OR Weight = '' OR Weight = ' ' THEN 'N/A'
                ELSE CAST(Weight AS NVARCHAR(50))
            END AS Weight,
            CASE 
                WHEN DaysToManufacture IS NULL THEN 'N/A'
                ELSE CAST(DaysToManufacture AS NVARCHAR(50))
            END AS DaysToManufacture,
            ISNULL(ProductLine, 'N/A') AS ProductLine,
            CASE 
                WHEN DealerPrice IS NULL THEN 'N/A'
                ELSE CAST(DealerPrice AS NVARCHAR(50))
            END AS DealerPrice,
            ISNULL(Class, 'N/A') AS Class,
            ISNULL(Style, 'N/A') AS Style,
            CASE 
                WHEN ModelName IS NULL OR ModelName IN ('', ' ', 'NA') THEN 'N/A'
                ELSE ModelName
            END AS ModelName,
            CAST(LargePhoto AS NVARCHAR(MAX)) AS LargePhoto,
            CASE 
                WHEN EnglishDescription IS NULL OR EnglishDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE EnglishDescription
            END AS EnglishDescription,
            CASE 
                WHEN FrenchDescription IS NULL OR FrenchDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE FrenchDescription
            END AS FrenchDescription,
            CASE 
                WHEN ChineseDescription IS NULL OR ChineseDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE ChineseDescription
            END AS ChineseDescription,
            CASE 
                WHEN ArabicDescription IS NULL OR ArabicDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE ArabicDescription
            END AS ArabicDescription,
            CASE 
                WHEN HebrewDescription IS NULL OR HebrewDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE HebrewDescription
            END AS HebrewDescription,
            CASE 
                WHEN ThaiDescription IS NULL OR ThaiDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE ThaiDescription
            END AS ThaiDescription,
            CASE 
                WHEN GermanDescription IS NULL OR GermanDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE GermanDescription
            END AS GermanDescription,
            CASE 
                WHEN JapaneseDescription IS NULL OR JapaneseDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE JapaneseDescription
            END AS JapaneseDescription,
            CASE 
                WHEN TurkishDescription IS NULL OR TurkishDescription IN ('', ' ', 'NA') THEN 'N/A'
                ELSE TurkishDescription
            END AS TurkishDescription,
            CASE 
                WHEN StartDate IS NULL OR TRY_CAST(StartDate AS NVARCHAR(50)) IN ('', ' ', 'NA') THEN 'N/A'
                ELSE TRY_CAST(StartDate AS NVARCHAR(50))
            END AS StartDate,
            CASE 
                WHEN EndDate IS NULL OR TRY_CAST(EndDate AS NVARCHAR(50)) IN ('', ' ', 'NA') THEN 'N/A'
                ELSE TRY_CAST(EndDate AS NVARCHAR(50))
            END AS EndDate,
            CASE 
                WHEN Status IS NULL OR Status IN ('', ' ') THEN 'N/A'
                ELSE Status
            END AS Status
        FROM bronz1.DimProduct;
        
        PRINT '- Data loaded into Product table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.9 ProductCategory Table
        -- =====================================================
        SET @StepName = 'ProductCategory Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.ProductCategory', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.ProductCategory;
            PRINT '- Existing ProductCategory table dropped';
        END
        
        -- Create ProductCategory table
        CREATE TABLE Silver1.ProductCategory 
        (
            ProductCategoryKey INT,
            ProductCategoryAlternateKey INT,
            EnglishProductCategoryName NVARCHAR(50),
            SpanishProductCategoryName NVARCHAR(50),
            FrenchProductCategoryName NVARCHAR(50)
        );
        PRINT '- ProductCategory table created';
        
        -- Insert data
        INSERT INTO Silver1.ProductCategory
        (
            ProductCategoryKey,
            ProductCategoryAlternateKey,
            EnglishProductCategoryName,
            SpanishProductCategoryName,
            FrenchProductCategoryName
        )
        SELECT 
            ProductCategoryKey,
            ProductCategoryAlternateKey,
            ISNULL(EnglishProductCategoryName, 'N/A') AS EnglishProductCategoryName,
            ISNULL(SpanishProductCategoryName, 'N/A') AS SpanishProductCategoryName,
            ISNULL(FrenchProductCategoryName, 'N/A') AS FrenchProductCategoryName
        FROM bronz1.DimProductCategory;
        
        PRINT '- Data loaded into ProductCategory table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.10 ProductSubCategory Table
        -- =====================================================
        SET @StepName = 'ProductSubCategory Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.ProductSubCategory', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.ProductSubCategory;
            PRINT '- Existing ProductSubCategory table dropped';
        END
        
        -- Create ProductSubCategory table
        CREATE TABLE Silver1.ProductSubCategory 
        (
            ProductCategoryKey INT,
            ProductSubcategoryKey INT,
            ProductSubcategoryAlternateKey INT,
            EnglishProductSubcategoryName NVARCHAR(50),
            SpanishProductSubcategoryName NVARCHAR(50),
            FrenchProductSubcategoryName NVARCHAR(50)
        );
        PRINT '- ProductSubCategory table created';
        
        -- Insert data
        INSERT INTO Silver1.ProductSubCategory
        (
            ProductCategoryKey,
            ProductSubcategoryKey,
            ProductSubcategoryAlternateKey,
            EnglishProductSubcategoryName,
            SpanishProductSubcategoryName,
            FrenchProductSubcategoryName
        )
        SELECT 
            ProductCategoryKey,
            ProductSubcategoryKey,
            ProductSubcategoryAlternateKey,
            ISNULL(EnglishProductSubcategoryName, 'N/A') AS EnglishProductSubcategoryName,
            ISNULL(SpanishProductSubcategoryName, 'N/A') AS SpanishProductSubcategoryName,
            ISNULL(FrenchProductSubcategoryName, 'N/A') AS FrenchProductSubcategoryName
        FROM bronz1.DimProductSubcategory;
        
        PRINT '- Data loaded into ProductSubCategory table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.11 Promotion Table
        -- =====================================================
        SET @StepName = 'Promotion Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.Promotion', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.Promotion;
            PRINT '- Existing Promotion table dropped';
        END
        
        -- Create Promotion table
        CREATE TABLE Silver1.Promotion
        (
            PromotionKey INT,
            PromotionAlternateKey INT,
            EnglishPromotionName NVARCHAR(50),
            SpanishPromotionName NVARCHAR(50),
            FrenchPromotionName NVARCHAR(50),
            DiscountPct FLOAT,
            EnglishPromotionType NVARCHAR(50),
            SpanishPromotionType NVARCHAR(50),
            FrenchPromotionType NVARCHAR(50),
            EnglishPromotionCategory NVARCHAR(50),
            SpanishPromotionCategory NVARCHAR(50),
            FrenchPromotionCategory NVARCHAR(50),
            StartDate DATETIME,
            EndDate DATETIME,
            MinQty NVARCHAR(50),
            MaxQty NVARCHAR(50)
        );
        PRINT '- Promotion table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.Promotion
        (
            PromotionKey,
            PromotionAlternateKey,
            EnglishPromotionName,
            SpanishPromotionName,
            FrenchPromotionName,
            DiscountPct,
            EnglishPromotionType,
            SpanishPromotionType,
            FrenchPromotionType,
            EnglishPromotionCategory,
            SpanishPromotionCategory,
            FrenchPromotionCategory,
            StartDate,
            EndDate,
            MinQty,
            MaxQty
        )
        SELECT 
            PromotionKey,
            PromotionAlternateKey,
            ISNULL(EnglishPromotionName, 'N/A') AS EnglishPromotionName,
            ISNULL(SpanishPromotionName, 'N/A') AS SpanishPromotionName,
            ISNULL(FrenchPromotionName, 'N/A') AS FrenchPromotionName,
            DiscountPct,
            ISNULL(EnglishPromotionType, 'N/A') AS EnglishPromotionType,
            ISNULL(SpanishPromotionType, 'N/A') AS SpanishPromotionType,
            ISNULL(FrenchPromotionType, 'N/A') AS FrenchPromotionType,
            ISNULL(EnglishPromotionCategory, 'N/A') AS EnglishPromotionCategory,
            ISNULL(SpanishPromotionCategory, 'N/A') AS SpanishPromotionCategory,
            ISNULL(FrenchPromotionCategory, 'N/A') AS FrenchPromotionCategory,
            StartDate,
            EndDate,
            CASE 
                WHEN MinQty IS NULL OR MinQty IN ('', ' ', 'NA') THEN 'N/A'
                ELSE TRY_CAST(MinQty AS NVARCHAR(50))
            END AS MinQty,
            CASE 
                WHEN MaxQty IS NULL OR MaxQty IN ('', ' ', 'NA') THEN 'N/A'
                ELSE TRY_CAST(MaxQty AS NVARCHAR(50))
            END AS MaxQty
        FROM bronz1.DimPromotion;
        
        PRINT '- Data loaded into Promotion table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.12 Reseller Table
        -- =====================================================
        SET @StepName = 'Reseller Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.Reseller', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.Reseller;
            PRINT '- Existing Reseller table dropped';
        END
        
        -- Create Reseller table
        CREATE TABLE Silver1.Reseller
        (
            ResellerKey INT,
            GeographyKey INT,
            ResellerAlternateKey NVARCHAR(50),
            Phone NVARCHAR(50),
            BusinessType NVARCHAR(50),
            ResellerName NVARCHAR(50),
            NumberEmployees INT,
            OrderFrequency NVARCHAR(10),
            OrderMonth NVARCHAR(10),
            FirstOrderYear NVARCHAR(10),
            LastOrderYear NVARCHAR(10),
            ProductLine NVARCHAR(50),
            AddressLine1 NVARCHAR(50),
            AddressLine2 NVARCHAR(50),
            AnnualSales MONEY,
            BankName NVARCHAR(50),
            MinPaymentType NVARCHAR(50),
            MinPaymentAmount MONEY,
            AnnualRevenue MONEY,
            YearOpened INT
        );
        PRINT '- Reseller table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.Reseller 
        (
            ResellerKey,
            GeographyKey,
            ResellerAlternateKey,
            Phone,
            BusinessType,
            ResellerName,
            NumberEmployees,
            OrderFrequency,
            OrderMonth,
            FirstOrderYear,
            LastOrderYear,
            ProductLine,
            AddressLine1,
            AddressLine2,
            AnnualSales,
            BankName,
            MinPaymentType,
            MinPaymentAmount,
            AnnualRevenue,
            YearOpened
        )
        SELECT 
            ResellerKey,
            GeographyKey,
            ISNULL(ResellerAlternateKey, 'N/A') AS ResellerAlternateKey,
            ISNULL(CAST(Phone AS NVARCHAR(20)), 'N/A') AS Phone,
            ISNULL(BusinessType, 'N/A') AS BusinessType,
            ISNULL(ResellerName, 'N/A') AS ResellerName,
            NumberEmployees,
            ISNULL(OrderFrequency, 'N/A') AS OrderFrequency,
            CASE 
                WHEN OrderMonth IS NULL OR CAST(OrderMonth AS NVARCHAR(10)) IN ('', ' ', 'NA') THEN 'N/A'
                ELSE CAST(OrderMonth AS NVARCHAR(10))
            END AS OrderMonth,
            CASE 
                WHEN FirstOrderYear IS NULL OR CAST(FirstOrderYear AS NVARCHAR(10)) IN ('', ' ', 'NA') THEN 'N/A'
                ELSE CAST(FirstOrderYear AS NVARCHAR(10))
            END AS FirstOrderYear,
            CASE 
                WHEN LastOrderYear IS NULL OR CAST(LastOrderYear AS NVARCHAR(10)) IN ('', ' ', 'NA') THEN 'N/A'
                ELSE CAST(LastOrderYear AS NVARCHAR(10))
            END AS LastOrderYear,
            ISNULL(ProductLine, 'N/A') AS ProductLine,
            ISNULL(AddressLine1, 'N/A') AS AddressLine1,
            CASE 
                WHEN AddressLine2 IS NULL OR LTRIM(RTRIM(AddressLine2)) IN ('', 'NA', 'N/A') THEN 'N/A'
                ELSE AddressLine2
            END AS AddressLine2,
            ISNULL(TRY_CAST(AnnualSales AS MONEY), 0) AS AnnualSales,
            ISNULL(BankName, 'N/A') AS BankName,
            CASE 
                WHEN MinPaymentType IS NULL OR LTRIM(RTRIM(CAST(MinPaymentType AS NVARCHAR(50)))) IN ('', 'NA', 'N/A') THEN 'N/A'
                ELSE CAST(MinPaymentType AS NVARCHAR(50))
            END AS MinPaymentType,
            ISNULL(TRY_CAST(MinPaymentAmount AS MONEY), 0) AS MinPaymentAmount,
            ISNULL(TRY_CAST(AnnualRevenue AS MONEY), 0) AS AnnualRevenue,
            YearOpened
        FROM bronz1.DimReseller;
        
        PRINT '- Data loaded into Reseller table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.13 SalesReason Table
        -- =====================================================
        SET @StepName = 'SalesReason Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.SalesReason', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.SalesReason;
            PRINT '- Existing SalesReason table dropped';
        END
        
        -- Create SalesReason table
        CREATE TABLE Silver1.SalesReason 
        (
            SalesReasonKey INT,
            SalesReasonAlternateKey INT,
            SalesReasonName NVARCHAR(50),
            SalesReasonReasonType NVARCHAR(50)
        );
        PRINT '- SalesReason table created';
        
        -- Insert data
        INSERT INTO Silver1.SalesReason
        (
            SalesReasonKey,
            SalesReasonAlternateKey,
            SalesReasonName,
            SalesReasonReasonType
        )
        SELECT 
            SalesReasonKey,
            SalesReasonAlternateKey,
            ISNULL(SalesReasonName, 'N/A') AS SalesReasonName,
            ISNULL(SalesReasonReasonType, 'N/A') AS SalesReasonReasonType
        FROM bronz1.DimSalesReason;
        
        PRINT '- Data loaded into SalesReason table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.14 SalesTerritory Table
        -- =====================================================
        SET @StepName = 'SalesTerritory Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.SalesTerritory', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.SalesTerritory;
            PRINT '- Existing SalesTerritory table dropped';
        END
        
        -- Create SalesTerritory table
        CREATE TABLE Silver1.SalesTerritory 
        (
            SalesTerritoryKey INT,
            SalesTerritoryAlternateKey INT,
            SalesTerritoryRegion NVARCHAR(50),
            SalesTerritoryCountry NVARCHAR(50),
            SalesTerritoryGroup NVARCHAR(50),
            SalesTerritoryImage NVARCHAR(MAX)
        );
        PRINT '- SalesTerritory table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.SalesTerritory
        (
            SalesTerritoryKey,
            SalesTerritoryAlternateKey,
            SalesTerritoryRegion,
            SalesTerritoryCountry,
            SalesTerritoryGroup,
            SalesTerritoryImage
        )
        SELECT 
            SalesTerritoryKey,
            SalesTerritoryAlternateKey,
            CASE 
                WHEN SalesTerritoryRegion IS NULL OR SalesTerritoryRegion IN ('', ' ', 'NA', 'N/A') THEN 'N/A'
                ELSE SalesTerritoryRegion
            END AS SalesTerritoryRegion,
            CASE 
                WHEN SalesTerritoryCountry IS NULL OR SalesTerritoryCountry IN ('', ' ', 'NA', 'N/A') THEN 'N/A'
                ELSE SalesTerritoryCountry
            END AS SalesTerritoryCountry,
            CASE 
                WHEN SalesTerritoryGroup IS NULL OR SalesTerritoryGroup IN ('', ' ', 'NA', 'N/A') THEN 'N/A'
                ELSE SalesTerritoryGroup
            END AS SalesTerritoryGroup,
            CAST(SalesTerritoryImage AS NVARCHAR(MAX)) AS SalesTerritoryImage
        FROM bronz1.DimSalesTerritory;
        
        PRINT '- Data loaded into SalesTerritory table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 1.15 Scenario Table
        -- =====================================================
        SET @StepName = 'Scenario Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.Scenario', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.Scenario;
            PRINT '- Existing Scenario table dropped';
        END
        
        -- Create Scenario table
        CREATE TABLE Silver1.Scenario 
        (
            ScenarioKey INT,
            ScenarioName NVARCHAR(50)
        );
        PRINT '- Scenario table created';
        
        -- Insert data
        INSERT INTO Silver1.Scenario
        (
            ScenarioKey,
            ScenarioName
        )
        SELECT 
            ScenarioKey,
            ISNULL(ScenarioName, 'N/A') AS ScenarioName
        FROM bronz1.DimScenario;
        
        PRINT '- Data loaded into Scenario table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- SECTION 2: FACT TABLES
        -- =====================================================
        
        -- =====================================================
        -- 2.1 ProductDescription Table
        -- =====================================================
        SET @StepName = 'ProductDescription Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.ProductDescription', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.ProductDescription;
            PRINT '- Existing ProductDescription table dropped';
        END
        
        -- Create ProductDescription table
        CREATE TABLE Silver1.ProductDescription
        (
            Gen_ProductKey INT,
            ProductKey INT,
            CultureName NVARCHAR(50),
            ProductDescription NVARCHAR(MAX)
        );
        PRINT '- ProductDescription table created';
        
        -- Insert data
        INSERT INTO Silver1.ProductDescription
        (
            Gen_ProductKey,
            ProductKey,
            CultureName,
            ProductDescription
        )
        SELECT 
            ROW_NUMBER() OVER(ORDER BY ProductKey) AS Gen_ProductKey,
            ProductKey,
            ISNULL(CultureName, 'N/A') AS CultureName,
            LTRIM(RTRIM(ProductDescription)) AS ProductDescription
        FROM DataWarehouse.bronz1.FactAdditionalInternationalProductDescription;
        
        PRINT '- Data loaded into ProductDescription table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.2 FactCallCenter Table
        -- =====================================================
        SET @StepName = 'FactCallCenter Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactCallCenter', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactCallCenter;
            PRINT '- Existing FactCallCenter table dropped';
        END
        
        -- Create FactCallCenter table
        CREATE TABLE Silver1.FactCallCenter
        (
            FactCallCenterID INT,
            FormattedDate DATETIME,
            WageType NVARCHAR(50),
            Shift NVARCHAR(50),
            LevelOneOperators INT,
            LevelTwoOperators INT,
            TotalOperators INT,
            Calls INT,
            AutomaticResponses INT,
            Orders INT,
            IssuesRaised INT,
            AverageTimePerIssue INT,
            ServiceGrade INT,
            [Date] DATETIME
        );
        PRINT '- FactCallCenter table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.FactCallCenter
        (
            FactCallCenterID,
            FormattedDate,
            WageType,
            Shift,
            LevelOneOperators,
            LevelTwoOperators,
            TotalOperators,
            Calls,
            AutomaticResponses,
            Orders,
            IssuesRaised,
            AverageTimePerIssue,
            ServiceGrade,
            [Date]
        )
        SELECT 
            FactCallCenterID,
            TRY_CAST(
                CONCAT(
                    LEFT(CAST(DateKey AS VARCHAR(8)), 4), '-',
                    SUBSTRING(CAST(DateKey AS VARCHAR(8)), 5, 2), '-',
                    RIGHT(CAST(DateKey AS VARCHAR(8)), 2)
                ) AS DATETIME
            ) AS FormattedDate,
            ISNULL(WageType, 'N/A') AS WageType,
            ISNULL(Shift, 'N/A') AS Shift,
            LevelOneOperators,
            LevelTwoOperators,
            TotalOperators,
            Calls,
            AutomaticResponses,
            Orders,
            IssuesRaised,
            AverageTimePerIssue,
            ServiceGrade,
            [Date]
        FROM bronz1.FactCallCenter;
        
        PRINT '- Data loaded into FactCallCenter table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.3 FactCurrencyRate Table
        -- =====================================================
        SET @StepName = 'FactCurrencyRate Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactCurrencyRate', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactCurrencyRate;
            PRINT '- Existing FactCurrencyRate table dropped';
        END
        
        -- Create FactCurrencyRate table
        CREATE TABLE Silver1.FactCurrencyRate
        (
            Gen_CurrencyKey INT,
            CurrencyKey INT,
            DateKey DATETIME,
            AverageRate INT,
            EndOfDayRate INT,
            [Date] DATETIME
        );
        PRINT '- FactCurrencyRate table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.FactCurrencyRate
        (
            Gen_CurrencyKey,
            CurrencyKey,
            DateKey,
            AverageRate,
            EndOfDayRate,
            [Date]
        )
        SELECT 
            ROW_NUMBER() OVER(ORDER BY CurrencyKey) AS Gen_CurrencyKey,
            CurrencyKey,
            TRY_CAST(
                CONCAT(
                    LEFT(CAST(DateKey AS VARCHAR(8)), 4), '-',
                    SUBSTRING(CAST(DateKey AS VARCHAR(8)), 5, 2), '-',
                    RIGHT(CAST(DateKey AS VARCHAR(8)), 2)
                ) AS DATETIME
            ) AS DateKey,
            AverageRate,
            EndOfDayRate,
            [Date]
        FROM bronz1.FactCurrencyRate;
        
        PRINT '- Data loaded into FactCurrencyRate table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.4 FactFinance Table
        -- =====================================================
        SET @StepName = 'FactFinance Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactFinance', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactFinance;
            PRINT '- Existing FactFinance table dropped';
        END
        
        -- Create FactFinance table
        CREATE TABLE Silver1.FactFinance
        (
            Gen_FinanceKey INT,
            FinanceKey INT,
            DateKey NVARCHAR(50),
            OrganizationKey INT,
            DepartmentGroupKey INT,
            ScenarioKey INT,
            AccountKey INT,
            Amount INT,
            [Date] DATETIME
        );
        PRINT '- FactFinance table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.FactFinance
        (
            Gen_FinanceKey,
            FinanceKey,
            DateKey,
            OrganizationKey,
            DepartmentGroupKey,
            ScenarioKey,
            AccountKey,
            Amount,
            [Date]
        )
        SELECT
            ROW_NUMBER() OVER(ORDER BY FinanceKey) AS Gen_FinanceKey,
            FinanceKey,
            CONCAT(
                LEFT(CAST(DateKey AS VARCHAR(8)), 4), '-',
                SUBSTRING(CAST(DateKey AS VARCHAR(8)), 5, 2), '-',
                RIGHT(CAST(DateKey AS VARCHAR(8)), 2)
            ) AS DateKey,
            OrganizationKey,
            DepartmentGroupKey,
            ScenarioKey,
            AccountKey,
            Amount,
            [Date]
        FROM bronz1.FactFinance;
        
        PRINT '- Data loaded into FactFinance table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.5 FactInternetSalesReason Table
        -- =====================================================
        SET @StepName = 'FactInternetSalesReason Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactInternetSalesReason', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactInternetSalesReason;
            PRINT '- Existing FactInternetSalesReason table dropped';
        END
        
        -- Create FactInternetSalesReason table
        CREATE TABLE Silver1.FactInternetSalesReason
        (
            Gen_SalesOrderNumber INT,
            SalesOrderNumber NVARCHAR(50),
            SalesOrderLineNumber INT,
            SalesReasonKey INT
        );
        PRINT '- FactInternetSalesReason table created';
        
        -- Insert data
        INSERT INTO Silver1.FactInternetSalesReason
        (
            Gen_SalesOrderNumber,
            SalesOrderNumber,
            SalesOrderLineNumber,
            SalesReasonKey
        )
        SELECT  
            ROW_NUMBER() OVER(ORDER BY SalesOrderNumber) AS Gen_SalesOrderNumber,
            SalesOrderNumber,
            SalesOrderLineNumber,
            SalesReasonKey
        FROM bronz1.FactInternetSalesReason;
        
        PRINT '- Data loaded into FactInternetSalesReason table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.6 FactInternetSales Table
        -- =====================================================
        SET @StepName = 'FactInternetSales Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactInternetSales', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactInternetSales;
            PRINT '- Existing FactInternetSales table dropped';
        END
        
        -- Create FactInternetSales table
        CREATE TABLE Silver1.FactInternetSales
        (
            Gen_ProductKey INT,
            ProductKey INT,
            OrderDateKey INT,
            DueDateKey INT,
            ShipDateKey INT,
            CustomerKey INT,
            PromotionKey INT,
            CurrencyKey INT,
            SalesTerritoryKey INT,
            SalesOrderNumber NVARCHAR(50),
            SalesOrderLineNumber INT,
            RevisionNumber INT,
            OrderQuantity INT,
            UnitPrice INT,
            ExtendedAmount INT,
            UnitPriceDiscountPct INT,
            DiscountAmount INT,
            ProductStandardCost INT,
            TotalProductCost INT,
            SalesAmount INT,
            TaxAmt INT,
            Freight INT,
            CarrierTrackingNumber NVARCHAR(50),
            CustomerPONumber NVARCHAR(50),
            OrderDate DATETIME,
            DueDate DATETIME,
            ShipDate DATETIME
        );
        PRINT '- FactInternetSales table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.FactInternetSales
        (
            Gen_ProductKey,
            ProductKey,
            OrderDateKey,
            DueDateKey,
            ShipDateKey,
            CustomerKey,
            PromotionKey,
            CurrencyKey,
            SalesTerritoryKey,
            SalesOrderNumber,
            SalesOrderLineNumber,
            RevisionNumber,
            OrderQuantity,
            UnitPrice,
            ExtendedAmount,
            UnitPriceDiscountPct,
            DiscountAmount,
            ProductStandardCost,
            TotalProductCost,
            SalesAmount,
            TaxAmt,
            Freight,
            CarrierTrackingNumber,
            CustomerPONumber,
            OrderDate,
            DueDate,
            ShipDate
        )
        SELECT 
            ROW_NUMBER() OVER(ORDER BY ProductKey) AS Gen_ProductKey,
            ProductKey,
            OrderDateKey,
            DueDateKey,
            ShipDateKey,
            CustomerKey,
            PromotionKey,
            CurrencyKey,
            SalesTerritoryKey,
            SalesOrderNumber,
            SalesOrderLineNumber,
            RevisionNumber,
            OrderQuantity,
            UnitPrice,
            ExtendedAmount,
            UnitPriceDiscountPct,
            DiscountAmount,
            ProductStandardCost,
            TotalProductCost,
            SalesAmount,
            TaxAmt,
            Freight,
            CASE 
                WHEN CarrierTrackingNumber IS NULL OR TRY_CAST(CarrierTrackingNumber AS NVARCHAR(50)) IN ('', 'NA', 'N/A') THEN 'N/A'
                ELSE CarrierTrackingNumber
            END AS CarrierTrackingNumber,
            CASE 
                WHEN CustomerPONumber IS NULL OR TRY_CAST(CustomerPONumber AS NVARCHAR(50)) IN ('', 'NA', 'N/A') THEN 'N/A'
                ELSE CustomerPONumber
            END AS CustomerPONumber,
            OrderDate,
            DueDate,
            ShipDate
        FROM bronz1.FactInternetSales;
        
        PRINT '- Data loaded into FactInternetSales table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.7 FactProductInventory Table
        -- =====================================================
        SET @StepName = 'FactProductInventory Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactProductInventory', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactProductInventory;
            PRINT '- Existing FactProductInventory table dropped';
        END
        
        -- Create FactProductInventory table
        CREATE TABLE Silver1.FactProductInventory
        (
            Gen_ProductKey INT,
            ProductKey INT,
            DateKey INT,
            MovementDate DATETIME,
            UnitCost INT,
            UnitsIn INT,
            UnitsOut INT,
            UnitsBalance INT
        );
        PRINT '- FactProductInventory table created';
        
        -- Insert data
        INSERT INTO Silver1.FactProductInventory
        (
            Gen_ProductKey,
            ProductKey,
            DateKey,
            MovementDate,
            UnitCost,
            UnitsIn,
            UnitsOut,
            UnitsBalance
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY ProductKey) AS Gen_ProductKey,
            ProductKey,
            DateKey,
            MovementDate,
            UnitCost,
            UnitsIn,
            UnitsOut,
            UnitsBalance
        FROM bronz1.FactProductInventory;
        
        PRINT '- Data loaded into FactProductInventory table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.8 FactResellerSales Table
        -- =====================================================
        SET @StepName = 'FactResellerSales Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactResellerSales', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactResellerSales;
            PRINT '- Existing FactResellerSales table dropped';
        END
        
        -- Create FactResellerSales table
        CREATE TABLE Silver1.FactResellerSales
        (
            Gen_ProductKey INT,
            ProductKey INT,
            OrderDateKey INT,
            DueDateKey INT,
            ShipDateKey INT,
            ResellerKey INT,
            EmployeeKey INT,
            PromotionKey INT,
            CurrencyKey INT,
            SalesTerritoryKey INT,
            SalesOrderNumber NVARCHAR(50),
            SalesOrderLineNumber INT,
            RevisionNumber INT,
            OrderQuantity INT,
            UnitPrice INT,
            ExtendedAmount INT,
            UnitPriceDiscountPct INT,
            DiscountAmount INT,
            ProductStandardCost INT,
            TotalProductCost INT,
            SalesAmount INT,
            Freight INT,
            CarrierTrackingNumber NVARCHAR(50),
            CustomerPONumber NVARCHAR(50),
            OrderDate DATETIME,
            DueDate DATETIME,
            ShipDate DATETIME
        );
        PRINT '- FactResellerSales table created';
        
        -- Insert data
        INSERT INTO Silver1.FactResellerSales
        (
            Gen_ProductKey,
            ProductKey,
            OrderDateKey,
            DueDateKey,
            ShipDateKey,
            ResellerKey,
            EmployeeKey,
            PromotionKey,
            CurrencyKey,
            SalesTerritoryKey,
            SalesOrderNumber,
            SalesOrderLineNumber,
            RevisionNumber,
            OrderQuantity,
            UnitPrice,
            ExtendedAmount,
            UnitPriceDiscountPct,
            DiscountAmount,
            ProductStandardCost,
            TotalProductCost,
            SalesAmount,
            Freight,
            CarrierTrackingNumber,
            CustomerPONumber,
            OrderDate,
            DueDate,
            ShipDate
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY ProductKey) AS Gen_ProductKey,
            ProductKey,
            OrderDateKey,
            DueDateKey,
            ShipDateKey,
            ResellerKey,
            EmployeeKey,
            PromotionKey,
            CurrencyKey,
            SalesTerritoryKey,
            SalesOrderNumber,
            SalesOrderLineNumber,
            RevisionNumber,
            OrderQuantity,
            UnitPrice,
            ExtendedAmount,
            UnitPriceDiscountPct,
            DiscountAmount,
            ProductStandardCost,
            TotalProductCost,
            SalesAmount,
            Freight,
            ISNULL(CarrierTrackingNumber, 'N/A') AS CarrierTrackingNumber,
            ISNULL(CustomerPONumber, 'N/A') AS CustomerPONumber,
            OrderDate,
            DueDate,
            ShipDate
        FROM bronz1.FactResellerSales;
        
        PRINT '- Data loaded into FactResellerSales table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.9 FactSalesQuota Table
        -- =====================================================
        SET @StepName = 'FactSalesQuota Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactSalesQuota', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactSalesQuota;
            PRINT '- Existing FactSalesQuota table dropped';
        END
        
        -- Create FactSalesQuota table
        CREATE TABLE Silver1.FactSalesQuota
        (
            SalesQuotaKey INT,
            EmployeeKey INT,
            DateKey INT,
            CalendarYear INT,
            CalendarQuarter INT,
            SalesAmountQuota INT,
            [Date] DATETIME
        );
        PRINT '- FactSalesQuota table created';
        
        -- Insert data
        INSERT INTO Silver1.FactSalesQuota
        (
            SalesQuotaKey,
            EmployeeKey,
            DateKey,
            CalendarYear,
            CalendarQuarter,
            SalesAmountQuota,
            [Date]
        )
        SELECT 
            SalesQuotaKey,
            EmployeeKey,
            DateKey,
            CalendarYear,
            CalendarQuarter,
            SalesAmountQuota,
            [Date]
        FROM bronz1.FactSalesQuota;
        
        PRINT '- Data loaded into FactSalesQuota table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.10 FactSurveyResponse Table
        -- =====================================================
        SET @StepName = 'FactSurveyResponse Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.FactSurveyResponse', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.FactSurveyResponse;
            PRINT '- Existing FactSurveyResponse table dropped';
        END
        
        -- Create FactSurveyResponse table
        CREATE TABLE Silver1.FactSurveyResponse
        (
            Genrated_Key INT,
            SurveyResponseKey INT,
            DateKey INT,
            CustomerKey INT,
            ProductCategoryKey INT,
            EnglishProductCategoryName NVARCHAR(50),
            ProductSubcategoryKey INT,
            EnglishProductSubcategoryName NVARCHAR(50),
            [Date] DATETIME
        );
        PRINT '- FactSurveyResponse table created';
        
        -- Insert data
        INSERT INTO Silver1.FactSurveyResponse
        (
            Genrated_Key,
            SurveyResponseKey,
            DateKey,
            CustomerKey,
            ProductCategoryKey,
            EnglishProductCategoryName,
            ProductSubcategoryKey,
            EnglishProductSubcategoryName,
            [Date]
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY SurveyResponseKey) AS Genrated_Key,
            SurveyResponseKey,
            DateKey,
            CustomerKey,
            ProductCategoryKey,
            LTRIM(RTRIM(EnglishProductCategoryName)) AS EnglishProductCategoryName,
            ProductSubcategoryKey,
            ISNULL(EnglishProductSubcategoryName, 'N/A') AS EnglishProductSubcategoryName,
            [Date]
        FROM bronz1.FactSurveyResponse;
        
        PRINT '- Data loaded into FactSurveyResponse table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.11 NewFactCurrencyRate Table
        -- =====================================================
        SET @StepName = 'NewFactCurrencyRate Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.NewFactCurrencyRate', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.NewFactCurrencyRate;
            PRINT '- Existing NewFactCurrencyRate table dropped';
        END
        
        -- Create NewFactCurrencyRate table
        CREATE TABLE Silver1.NewFactCurrencyRate
        (
            CurrencyKey INT,
            AverageRate INT,
            CurrencyID NVARCHAR(50),
            CurrencyDate DATETIME,
            EndOfDayRate INT,
            DateKey INT
        );
        PRINT '- NewFactCurrencyRate table created';
        
        -- Insert data
        INSERT INTO Silver1.NewFactCurrencyRate
        (
            CurrencyKey,
            AverageRate,
            CurrencyID,
            CurrencyDate,
            EndOfDayRate,
            DateKey
        )
        SELECT 
            CurrencyKey,
            AverageRate,
            ISNULL(CurrencyID, 'N/A') AS CurrencyID,
            CurrencyDate,
            EndOfDayRate,
            DateKey
        FROM bronz1.NewFactCurrencyRate;
        
        PRINT '- Data loaded into NewFactCurrencyRate table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- 2.12 ProspectiveBuyer Table
        -- =====================================================
        SET @StepName = 'ProspectiveBuyer Table';
        SET @StepStartTime = GETDATE();
        PRINT CHAR(13) + 'Processing ' + @StepName + '...';
        
        -- Drop table if exists
        IF OBJECT_ID('Silver1.ProspectiveBuyer', 'U') IS NOT NULL
        BEGIN
            DROP TABLE Silver1.ProspectiveBuyer;
            PRINT '- Existing ProspectiveBuyer table dropped';
        END
        
        -- Create ProspectiveBuyer table
        CREATE TABLE Silver1.ProspectiveBuyer 
        (
            ProspectiveBuyerKey INT,
            ProspectAlternateKey NVARCHAR(50),
            FirstName NVARCHAR(50),
            MiddleName NVARCHAR(50),
            LastName NVARCHAR(50),
            BirthDate DATETIME,
            MaritalStatus NVARCHAR(50),
            Gender NVARCHAR(50),
            EmailAddress NVARCHAR(50),
            YearlyIncome INT,
            TotalChildren INT,
            NumberChildrenAtHome INT,
            Education NVARCHAR(50),
            Occupation NVARCHAR(50),
            HouseOwnerFlag INT,
            NumberCarsOwned INT,
            AddressLine1 NVARCHAR(50),
            AddressLine2 NVARCHAR(50),
            City NVARCHAR(50),
            StateProvinceCode NVARCHAR(50),
            PostalCode INT,
            Phone NVARCHAR(50),
            Salutation NVARCHAR(50),
            Unknown INT
        );
        PRINT '- ProspectiveBuyer table created';
        
        -- Insert data with transformations
        INSERT INTO Silver1.ProspectiveBuyer
        (
            ProspectiveBuyerKey,
            ProspectAlternateKey,
            FirstName,
            MiddleName,
            LastName,
            BirthDate,
            MaritalStatus,
            Gender,
            EmailAddress,
            YearlyIncome,
            TotalChildren,
            NumberChildrenAtHome,
            Education,
            Occupation,
            HouseOwnerFlag,
            NumberCarsOwned,
            AddressLine1,
            AddressLine2,
            City,
            StateProvinceCode,
            PostalCode,
            Phone,
            Salutation,
            Unknown
        )
        SELECT 
            ProspectiveBuyerKey,
            ISNULL(ProspectAlternateKey, 'N/A') AS ProspectAlternateKey,
            ISNULL(FirstName, 'N/A') AS FirstName,
            CASE 
                WHEN MiddleName IS NULL OR MiddleName IN ('', ' ', 'NA', 'N/A') THEN 'N/A'
                ELSE MiddleName
            END AS MiddleName,
            ISNULL(LastName, 'N/A') AS LastName,
            BirthDate,
            CASE 
                WHEN UPPER(LTRIM(RTRIM(MaritalStatus))) = 'M' THEN 'Married'
                WHEN UPPER(LTRIM(RTRIM(MaritalStatus))) = 'S' THEN 'Single'
                WHEN UPPER(LTRIM(RTRIM(MaritalStatus))) = 'W' THEN 'Widowed'
                WHEN UPPER(LTRIM(RTRIM(MaritalStatus))) = 'D' THEN 'Divorced'
                ELSE 'N/A'
            END AS MaritalStatus,
            CASE 
                WHEN UPPER(LTRIM(RTRIM(Gender))) = 'M' THEN 'Male'
                WHEN UPPER(LTRIM(RTRIM(Gender))) = 'F' THEN 'Female'
                ELSE 'N/A'
            END AS Gender,
            ISNULL(EmailAddress, 'N/A') AS EmailAddress,
            YearlyIncome,
            TotalChildren,
            NumberChildrenAtHome,
            ISNULL(Education, 'N/A') AS Education,
            ISNULL(Occupation, 'N/A') AS Occupation,
            HouseOwnerFlag,
            NumberCarsOwned,
            ISNULL(AddressLine1, 'N/A') AS AddressLine1,
            CASE 
                WHEN AddressLine2 IS NULL OR AddressLine2 IN ('', ' ', 'NA', 'N/A') THEN 'N/A'
                ELSE AddressLine2
            END AS AddressLine2,
            ISNULL(City, 'N/A') AS City,
            ISNULL(StateProvinceCode, 'N/A') AS StateProvinceCode,
            ISNULL(TRY_CAST(PostalCode AS INT), 0) AS PostalCode,
            ISNULL(Phone, 'N/A') AS Phone,
            ISNULL(Salutation, 'N/A') AS Salutation,
            Unknown
        FROM bronz1.ProspectiveBuyer;
        
        PRINT '- Data loaded into ProspectiveBuyer table. Rows affected: ' + FORMAT(@@ROWCOUNT, 'N0');
        PRINT '- Step completed in: ' + FORMAT(DATEDIFF(SECOND, @StepStartTime, GETDATE()), 'N0') + ' seconds';
        
        -- =====================================================
        -- COMMIT TRANSACTION
        -- =====================================================
        COMMIT TRANSACTION;
        
        DECLARE @EndTime DATETIME = GETDATE();
        DECLARE @TotalSeconds INT = DATEDIFF(SECOND, @StartTime, @EndTime);
        
        PRINT CHAR(13) + '====================================================';
        PRINT '    SILVER LAYER DATA LOAD COMPLETED SUCCESSFULLY!';
        PRINT '====================================================';
        PRINT 'Start Time: ' + CONVERT(VARCHAR, @StartTime, 120);
        PRINT 'End Time: ' + CONVERT(VARCHAR, @EndTime, 120);
        PRINT 'Total Duration: ' + FORMAT(@TotalSeconds / 60, 'N0') + ' minutes ' + 
              FORMAT(@TotalSeconds % 60, 'N0') + ' seconds';
        PRINT '====================================================';
        
    END TRY
    BEGIN CATCH
        -- =====================================================
        -- ERROR HANDLING
        -- =====================================================
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        DECLARE @ErrorLine INT = ERROR_LINE();
        DECLARE @ErrorProcedure NVARCHAR(200) = ERROR_PROCEDURE();
        
        PRINT CHAR(13) + '====================================================';
        PRINT '                 ERROR OCCURRED!';
        PRINT '====================================================';
        PRINT 'Error Number: ' + CAST(@ErrorNumber AS VARCHAR(10));
        PRINT 'Error Message: ' + @ErrorMessage;
        PRINT 'Error Severity: ' + CAST(@ErrorSeverity AS VARCHAR(10));
        PRINT 'Error State: ' + CAST(@ErrorState AS VARCHAR(10));
        PRINT 'Error Line: ' + CAST(@ErrorLine AS VARCHAR(10));
        PRINT 'Error Procedure: ' + ISNULL(@ErrorProcedure, 'N/A');
        PRINT 'Step Name: ' + ISNULL(@StepName, 'Unknown');
        PRINT '====================================================';
        
        -- Re-throw the error
        THROW;
    END CATCH;
END;
GO

-- =====================================================
-- EXECUTE THE PROCEDURE
-- =====================================================
PRINT 'Executing Silver1.load_silver procedure...';
EXEC Silver1.load_silver;
GO

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================
-- Uncomment below to verify table counts after execution
/*
SELECT 'Account' AS TableName, COUNT(*) AS RowCount FROM Silver1.Account UNION ALL
SELECT 'Currency', COUNT(*) FROM Silver1.currency UNION ALL
SELECT 'S_date', COUNT(*) FROM Silver1.S_date UNION ALL
SELECT 'DepartmentGroup', COUNT(*) FROM Silver1.DepartmentGroup UNION ALL
SELECT 'Employee', COUNT(*) FROM Silver1.employee UNION ALL
SELECT 'Geography', COUNT(*) FROM Silver1.Geography UNION ALL
SELECT 'Organization', COUNT(*) FROM Silver1.Organization UNION ALL
SELECT 'Product', COUNT(*) FROM Silver1.Product UNION ALL
SELECT 'ProductCategory', COUNT(*) FROM Silver1.ProductCategory UNION ALL
SELECT 'ProductSubCategory', COUNT(*) FROM Silver1.ProductSubCategory UNION ALL
SELECT 'Promotion', COUNT(*) FROM Silver1.Promotion UNION ALL
SELECT 'Reseller', COUNT(*) FROM Silver1.Reseller UNION ALL
SELECT 'SalesReason', COUNT(*) FROM Silver1.SalesReason UNION ALL
SELECT 'SalesTerritory', COUNT(*) FROM Silver1.SalesTerritory UNION ALL
SELECT 'Scenario', COUNT(*) FROM Silver1.Scenario UNION ALL
SELECT 'ProductDescription', COUNT(*) FROM Silver1.ProductDescription UNION ALL
SELECT 'FactCallCenter', COUNT(*) FROM Silver1.FactCallCenter UNION ALL
SELECT 'FactCurrencyRate', COUNT(*) FROM Silver1.FactCurrencyRate UNION ALL
SELECT 'FactFinance', COUNT(*) FROM Silver1.FactFinance UNION ALL
SELECT 'FactInternetSalesReason', COUNT(*) FROM Silver1.FactInternetSalesReason UNION ALL
SELECT 'FactInternetSales', COUNT(*) FROM Silver1.FactInternetSales UNION ALL
SELECT 'FactProductInventory', COUNT(*) FROM Silver1.FactProductInventory UNION ALL
SELECT 'FactResellerSales', COUNT(*) FROM Silver1.FactResellerSales UNION ALL
SELECT 'FactSalesQuota', COUNT(*) FROM Silver1.FactSalesQuota UNION ALL
SELECT 'FactSurveyResponse', COUNT(*) FROM Silver1.FactSurveyResponse UNION ALL
SELECT 'NewFactCurrencyRate', COUNT(*) FROM Silver1.NewFactCurrencyRate UNION ALL
SELECT 'ProspectiveBuyer', COUNT(*) FROM Silver1.ProspectiveBuyer
ORDER BY TableName;
*/
GO

