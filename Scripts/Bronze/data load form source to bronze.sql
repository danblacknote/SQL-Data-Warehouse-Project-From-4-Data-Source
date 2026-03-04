-- =====================================================
-- BRONZE LAYER TABLE CREATION AND DATA LOADING SCRIPT
-- =====================================================
-- Purpose: Create bronze layer tables and load data from 
--          AdventureWorksDW2014 source database
-- Author: [Deneke Zewdu]
-- Date: 2026-03-03
-- =====================================================

-- =====================================================
--SECTION 1: DIMENSION TABLES
-- =====================================================
CREATE OR ALTER PROCEDURE bronze1.load_bronze 
AS
BEGIN TRY
    BEGIN TRANSACTION;
    
    PRINT 'Starting Bronze Layer Table Creation and Data Load...';
    PRINT '====================================================';
    
    -- =====================================================
    --                     Account Table                  --
    -- =====================================================
    PRINT 'Processing Account table...';
    
    IF OBJECT_ID('bronz1.Account', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Account;
        PRINT '- Existing Account table dropped';
    END
    
    CREATE TABLE bronz1.Account(
        [AccountKey] [int] IDENTITY(1,1) NOT NULL,
        [ParentAccountKey] [int] NULL,
        [AccountCodeAlternateKey] [int] NULL,
        [ParentAccountCodeAlternateKey] [int] NULL,
        [AccountDescription] [nvarchar](50) NULL,
        [AccountType] [nvarchar](50) NULL,
        [Operator] [nvarchar](50) NULL,
        [CustomMembers] [nvarchar](300) NULL,
        [ValueType] [nvarchar](50) NULL,
        [CustomMemberOptions] [nvarchar](200) NULL
    );
    PRINT '- Account table created successfully';
    
    SET IDENTITY_INSERT bronz1.Account ON;
    
    INSERT INTO bronz1.Account (
        [AccountKey], 
        [ParentAccountKey], 
        [AccountCodeAlternateKey],
        [ParentAccountCodeAlternateKey], 
        [AccountDescription], 
        [AccountType],
        [Operator], 
        [CustomMembers], 
        [ValueType], 
        [CustomMemberOptions]
    )
    SELECT 
        [AccountKey], 
        [ParentAccountKey], 
        [AccountCodeAlternateKey],
        [ParentAccountCodeAlternateKey], 
        [AccountDescription], 
        [AccountType],
        [Operator], 
        [CustomMembers], 
        [ValueType], 
        [CustomMemberOptions]

    FROM [AdventureWorksDW2014].[dbo].[Account];
    
    SET IDENTITY_INSERT bronz1.Account OFF;
    PRINT '- Data loaded into Account table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                Currency Table                      --
    -- =====================================================
    PRINT 'Processing Currency table...';
    
    IF OBJECT_ID('bronz1.Currency', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Currency;
        PRINT '- Existing Currency table dropped';
    END
    
    CREATE TABLE bronz1.Currency(
        [CurrencyKey] [int] IDENTITY(1,1) NOT NULL,
        [CurrencyAlternateKey] [nchar](3) NOT NULL,
        [CurrencyName] [nvarchar](50) NOT NULL
    );
    PRINT '- Currency table created successfully';
    
    SET IDENTITY_INSERT bronz1.Currency ON;
    
    INSERT INTO bronz1.Currency (
        [CurrencyKey], 
        [CurrencyAlternateKey], 
        [CurrencyName]
    )
    SELECT 
        [CurrencyKey], 
        [CurrencyAlternateKey], 
        [CurrencyName]
    FROM [AdventureWorksDW2014].[dbo].[Currency];
    
    SET IDENTITY_INSERT bronz1.Currency OFF;
    PRINT '- Data loaded into Currency table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                 Customer Table                     --
    -- =====================================================
    PRINT 'Processing Customer table...';
    
    IF OBJECT_ID('bronz1.Customer', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Customer;
        PRINT '- Existing Customer table dropped';
    END
    
    CREATE TABLE bronz1.Customer(
        [CustomerKey] [int] IDENTITY(1,1) NOT NULL,
        [GeographyKey] [int] NULL,
        [CustomerAlternateKey] [nvarchar](15) NOT NULL,
        [Title] [nvarchar](8) NULL,
        [FirstName] [nvarchar](50) NULL,
        [MiddleName] [nvarchar](50) NULL,
        [LastName] [nvarchar](50) NULL,
        [NameStyle] [bit] NULL,
        [BirthDate] [date] NULL,
        [MaritalStatus] [nchar](1) NULL,
        [Suffix] [nvarchar](10) NULL,
        [Gender] [nvarchar](1) NULL,
        [EmailAddress] [nvarchar](50) NULL,
        [YearlyIncome] [money] NULL,
        [TotalChildren] [tinyint] NULL,
        [NumberChildrenAtHome] [tinyint] NULL,
        [EnglishEducation] [nvarchar](40) NULL,
        [SpanishEducation] [nvarchar](40) NULL,
        [FrenchEducation] [nvarchar](40) NULL,
        [EnglishOccupation] [nvarchar](100) NULL,
        [SpanishOccupation] [nvarchar](100) NULL,
        [FrenchOccupation] [nvarchar](100) NULL,
        [HouseOwnerFlag] [nchar](1) NULL,
        [NumberCarsOwned] [tinyint] NULL,
        [AddressLine1] [nvarchar](120) NULL,
        [AddressLine2] [nvarchar](120) NULL,
        [Phone] [nvarchar](20) NULL,
        [DateFirstPurchase] [date] NULL,
        [CommuteDistance] [nvarchar](15) NULL
    );
    PRINT '- Customer table created successfully';
    
    SET IDENTITY_INSERT bronz1.Customer ON;
    
    INSERT INTO bronz1.Customer (
        [CustomerKey], 
        [GeographyKey], 
        [CustomerAlternateKey], 
        [Title],
        [FirstName], 
        [MiddleName], 
        [LastName], 
        [NameStyle], 
        [BirthDate],
        [MaritalStatus], 
        [Suffix], 
        [Gender], 
        [EmailAddress], 
        [YearlyIncome],
        [TotalChildren], 
        [NumberChildrenAtHome], 
        [EnglishEducation],
        [SpanishEducation], 
        [FrenchEducation], 
        [EnglishOccupation],
        [SpanishOccupation], 
        [FrenchOccupation], 
        [HouseOwnerFlag],
        [NumberCarsOwned], 
        [AddressLine1], 
        [AddressLine2], 
        [Phone],
        [DateFirstPurchase], [CommuteDistance]
    )
    SELECT 
        [CustomerKey], 
        [GeographyKey], 
        [CustomerAlternateKey],
        [Title],
        [FirstName], 
        [MiddleName], 
        [LastName], 
        [NameStyle], 
        [BirthDate],
        [MaritalStatus], 
        [Suffix], 
        [Gender], 
        [EmailAddress],
        [YearlyIncome],
        [TotalChildren], 
        [NumberChildrenAtHome], 
        [EnglishEducation],
        [SpanishEducation], 
        [FrenchEducation], 
        [EnglishOccupation],
        [SpanishOccupation], 
        [FrenchOccupation], 
        [HouseOwnerFlag],
        [NumberCarsOwned], 
        [AddressLine1], 
        [AddressLine2], 
        [Phone],
        [DateFirstPurchase], 
        [CommuteDistance]
    FROM [AdventureWorksDW2014].[dbo].[Customer];
    
    SET IDENTITY_INSERT bronz1.Customer OFF;
    PRINT '- Data loaded into Customer table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                     Date Table                     --
    -- =====================================================
    PRINT 'Processing Date table...';
    
    IF OBJECT_ID('bronz1.Date', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Date;
        PRINT '- Existing Date table dropped';
    END
    
    CREATE TABLE bronz1.Date(
        [DateKey] [int] NOT NULL,
        [FullDateAlternateKey] [date] NOT NULL,
        [DayNumberOfWeek] [tinyint] NOT NULL,
        [EnglishDayNameOfWeek] [nvarchar](10) NOT NULL,
        [SpanishDayNameOfWeek] [nvarchar](10) NOT NULL,
        [FrenchDayNameOfWeek] [nvarchar](10) NOT NULL,
        [DayNumberOfMonth] [tinyint] NOT NULL,
        [DayNumberOfYear] [smallint] NOT NULL,
        [WeekNumberOfYear] [tinyint] NOT NULL,
        [EnglishMonthName] [nvarchar](10) NOT NULL,
        [SpanishMonthName] [nvarchar](10) NOT NULL,
        [FrenchMonthName] [nvarchar](10) NOT NULL,
        [MonthNumberOfYear] [tinyint] NOT NULL,
        [CalendarQuarter] [tinyint] NOT NULL,
        [CalendarYear] [smallint] NOT NULL,
        [CalendarSemester] [tinyint] NOT NULL,
        [FiscalQuarter] [tinyint] NOT NULL,
        [FiscalYear] [smallint] NOT NULL,
        [FiscalSemester] [tinyint] NOT NULL
    );
    PRINT '- Date table created successfully';
    
    INSERT INTO bronz1.Date
    SELECT * FROM [AdventureWorksDW2014].[dbo].[Date];
    PRINT '- Data loaded into Date table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --               DepartmentGroup Table                --
    -- =====================================================
    PRINT 'Processing DepartmentGroup table...';
    
    IF OBJECT_ID('bronz1.DepartmentGroup', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.DepartmentGroup;
        PRINT '- Existing DepartmentGroup table dropped';
    END
    
    CREATE TABLE bronz1.DepartmentGroup(
        [DepartmentGroupKey] [int] IDENTITY(1,1) NOT NULL,
        [ParentDepartmentGroupKey] [int] NULL,
        [DepartmentGroupName] [nvarchar](50) NULL
    );
    PRINT '- DepartmentGroup table created successfully';
    
    SET IDENTITY_INSERT bronz1.DepartmentGroup ON;
    
    INSERT INTO bronz1.DepartmentGroup (
        [DepartmentGroupKey], 
        [ParentDepartmentGroupKey], 
        [DepartmentGroupName]
    )
    SELECT 
        [DepartmentGroupKey], 
        [ParentDepartmentGroupKey], 
        [DepartmentGroupName]
    FROM [AdventureWorksDW2014].[dbo].[DepartmentGroup];
    
    SET IDENTITY_INSERT bronz1.DepartmentGroup OFF;
    PRINT '- Data loaded into DepartmentGroup table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                   Employee Table                   --
    -- =====================================================
    PRINT 'Processing Employee table...';
    
    IF OBJECT_ID('bronz1.Employee', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Employee;
        PRINT '- Existing Employee table dropped';
    END
    
    CREATE TABLE bronz1.Employee(
        [EmployeeKey] [int] IDENTITY(1,1) NOT NULL,
        [ParentEmployeeKey] [int] NULL,
        [EmployeeNationalIDAlternateKey] [nvarchar](15) NULL,
        [ParentEmployeeNationalIDAlternateKey] [nvarchar](15) NULL,
        [SalesTerritoryKey] [int] NULL,
        [FirstName] [nvarchar](50) NOT NULL,
        [LastName] [nvarchar](50) NOT NULL,
        [MiddleName] [nvarchar](50) NULL,
        [NameStyle] [bit] NOT NULL,
        [Title] [nvarchar](50) NULL,
        [HireDate] [date] NULL,
        [BirthDate] [date] NULL,
        [LoginID] [nvarchar](256) NULL,
        [EmailAddress] [nvarchar](50) NULL,
        [Phone] [nvarchar](25) NULL,
        [MaritalStatus] [nchar](1) NULL,
        [EmergencyContactName] [nvarchar](50) NULL,
        [EmergencyContactPhone] [nvarchar](25) NULL,
        [SalariedFlag] [bit] NULL,
        [Gender] [nchar](1) NULL,
        [PayFrequency] [tinyint] NULL,
        [BaseRate] [money] NULL,
        [VacationHours] [smallint] NULL,
        [SickLeaveHours] [smallint] NULL,
        [CurrentFlag] [bit] NOT NULL,
        [SalesPersonFlag] [bit] NOT NULL,
        [DepartmentName] [nvarchar](50) NULL,
        [StartDate] [date] NULL,
        [EndDate] [date] NULL,
        [Status] [nvarchar](50) NULL,
        [EmployeePhoto] [varbinary](max) NULL
    );
    PRINT '- Employee table created successfully';
    
    SET IDENTITY_INSERT bronz1.Employee ON;
    
    INSERT INTO bronz1.Employee (
        [EmployeeKey],
        [ParentEmployeeKey], 
        [EmployeeNationalIDAlternateKey],
        [ParentEmployeeNationalIDAlternateKey],
        [SalesTerritoryKey],
        [FirstName],
        [LastName], 
        [MiddleName], 
        [NameStyle], 
        [Title], 
        [HireDate], 
        [BirthDate],
        [LoginID], 
        [EmailAddress], 
        [Phone], 
        [MaritalStatus], 
        [EmergencyContactName],
        [EmergencyContactPhone],
        [SalariedFlag], 
        [Gender], 
        [PayFrequency],
        [BaseRate], 
        [VacationHours], 
        [SickLeaveHours], 
        [CurrentFlag],
        [SalesPersonFlag], 
        [DepartmentName], 
        [StartDate], 
        [EndDate], 
        [Status],
        [EmployeePhoto]
    )
    SELECT 
        [EmployeeKey], 
        [ParentEmployeeKey], 
        [EmployeeNationalIDAlternateKey],
        [ParentEmployeeNationalIDAlternateKey], 
        [SalesTerritoryKey], 
        [FirstName],
        [LastName], 
        [MiddleName], 
        [NameStyle],
        [Title], 
        [HireDate],
        [BirthDate],
        [LoginID], 
        [EmailAddress], 
        [Phone], 
        [MaritalStatus], 
        [EmergencyContactName],
        [EmergencyContactPhone], 
        [SalariedFlag], 
        [Gender], 
        [PayFrequency],
        [BaseRate], 
        [VacationHours], 
        [SickLeaveHours], 
        [CurrentFlag],
        [SalesPersonFlag], 
        [DepartmentName], 
        [StartDate], 
        [EndDate], 
        [Status],
        [EmployeePhoto]
    FROM [AdventureWorksDW2014].[dbo].[Employee];
    
    SET IDENTITY_INSERT bronz1.Employee OFF;
    PRINT '- Data loaded into Employee table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                   Geography Table                  --
    -- =====================================================
    PRINT 'Processing Geography table...';
    
    IF OBJECT_ID('bronz1.Geography', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Geography;
        PRINT '- Existing Geography table dropped';
    END
    
    CREATE TABLE bronz1.Geography(
        [GeographyKey] [int] IDENTITY(1,1) NOT NULL,
        [City] [nvarchar](30) NULL,
        [StateProvinceCode] [nvarchar](3) NULL,
        [StateProvinceName] [nvarchar](50) NULL,
        [CountryRegionCode] [nvarchar](3) NULL,
        [EnglishCountryRegionName] [nvarchar](50) NULL,
        [SpanishCountryRegionName] [nvarchar](50) NULL,
        [FrenchCountryRegionName] [nvarchar](50) NULL,
        [PostalCode] [nvarchar](15) NULL,
        [SalesTerritoryKey] [int] NULL,
        [IpAddressLocator] [nvarchar](15) NULL
    );
    PRINT '- Geography table created successfully';
    
    SET IDENTITY_INSERT bronz1.Geography ON;
    
    INSERT INTO bronz1.Geography (
        [GeographyKey], 
        [City], 
        [StateProvinceCode], 
        [StateProvinceName],
        [CountryRegionCode], 
        [EnglishCountryRegionName], 
        [SpanishCountryRegionName],
        [FrenchCountryRegionName], 
        [PostalCode], 
        [SalesTerritoryKey], 
        [IpAddressLocator]
    )
    SELECT 
        [GeographyKey], 
        [City], 
        [StateProvinceCode], 
        [StateProvinceName],
        [CountryRegionCode], 
        [EnglishCountryRegionName], 
        [SpanishCountryRegionName],
        [FrenchCountryRegionName], 
        [PostalCode], 
        [SalesTerritoryKey], 
        [IpAddressLocator]
    FROM [AdventureWorksDW2014].[dbo].[Geography];
    
    SET IDENTITY_INSERT bronz1.Geography OFF;
    PRINT '- Data loaded into Geography table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                     Organization Table             --
    -- =====================================================
    PRINT 'Processing Organization table...';
    
    IF OBJECT_ID('bronz1.Organization', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Organization;
        PRINT '- Existing Organization table dropped';
    END
    
    CREATE TABLE bronz1.Organization(
        [OrganizationKey] [int] IDENTITY(1,1) NOT NULL,
        [ParentOrganizationKey] [int] NULL,
        [PercentageOfOwnership] [nvarchar](16) NULL,
        [OrganizationName] [nvarchar](50) NULL,
        [CurrencyKey] [int] NULL
    );
    PRINT '- Organization table created successfully';
    
    SET IDENTITY_INSERT bronz1.Organization ON;
    
    INSERT INTO bronz1.Organization (
        [OrganizationKey], 
        [ParentOrganizationKey], 
        [PercentageOfOwnership],
        [OrganizationName], 
        [CurrencyKey]
    )
    SELECT 
        [OrganizationKey], [ParentOrganizationKey], [PercentageOfOwnership],
        [OrganizationName], [CurrencyKey]
    FROM [AdventureWorksDW2014].[dbo].[Organization];
    
    SET IDENTITY_INSERT bronz1.Organization OFF;
    PRINT '- Data loaded into Organization table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                     Product Table                  --
    -- =====================================================
    PRINT 'Processing Product table...';
    
    IF OBJECT_ID('bronz1.Product', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Product;
        PRINT '- Existing Product table dropped';
    END
    
    CREATE TABLE bronz1.Product(
        [ProductKey] [int] IDENTITY(1,1) NOT NULL,
        [ProductAlternateKey] [nvarchar](25) NULL,
        [ProductSubcategoryKey] [int] NULL,
        [WeightUnitMeasureCode] [nchar](3) NULL,
        [SizeUnitMeasureCode] [nchar](3) NULL,
        [EnglishProductName] [nvarchar](50) NOT NULL,
        [SpanishProductName] [nvarchar](50) NOT NULL,
        [FrenchProductName] [nvarchar](50) NOT NULL,
        [StandardCost] [money] NULL,
        [FinishedGoodsFlag] [bit] NOT NULL,
        [Color] [nvarchar](15) NOT NULL,
        [SafetyStockLevel] [smallint] NULL,
        [ReorderPoint] [smallint] NULL,
        [ListPrice] [money] NULL,
        [Size] [nvarchar](50) NULL,
        [SizeRange] [nvarchar](50) NULL,
        [Weight] [float] NULL,
        [DaysToManufacture] [int] NULL,
        [ProductLine] [nchar](2) NULL,
        [DealerPrice] [money] NULL,
        [Class] [nchar](2) NULL,
        [Style] [nchar](2) NULL,
        [ModelName] [nvarchar](50) NULL,
        [LargePhoto] [varbinary](max) NULL,
        [EnglishDescription] [nvarchar](400) NULL,
        [FrenchDescription] [nvarchar](400) NULL,
        [ChineseDescription] [nvarchar](400) NULL,
        [ArabicDescription] [nvarchar](400) NULL,
        [HebrewDescription] [nvarchar](400) NULL,
        [ThaiDescription] [nvarchar](400) NULL,
        [GermanDescription] [nvarchar](400) NULL,
        [JapaneseDescription] [nvarchar](400) NULL,
        [TurkishDescription] [nvarchar](400) NULL,
        [StartDate] [datetime] NULL,
        [EndDate] [datetime] NULL,
        [Status] [nvarchar](7) NULL
    );
    PRINT '- Product table created successfully';
    
    SET IDENTITY_INSERT bronz1.Product ON;
    
    INSERT INTO bronz1.Product (
        [ProductKey], 
        [ProductAlternateKey], 
        [ProductSubcategoryKey],
        [WeightUnitMeasureCode],
        [SizeUnitMeasureCode], 
        [EnglishProductName],
        [SpanishProductName],
        [FrenchProductName], 
        [StandardCost],
        [FinishedGoodsFlag], 
        [Color], [SafetyStockLevel], 
        [ReorderPoint],
        [ListPrice], 
        [Size],
        [SizeRange], 
        [Weight], 
        [DaysToManufacture],
        [ProductLine], 
        [DealerPrice], 
        [Class], 
        [Style], 
        [ModelName],
        [LargePhoto], 
        [EnglishDescription], 
        [FrenchDescription],
        [ChineseDescription], 
        [ArabicDescription], 
        [HebrewDescription],
        [ThaiDescription], 
        [GermanDescription], 
        [JapaneseDescription],
        [TurkishDescription], 
        [StartDate], 
        [EndDate], 
        [Status]
    )
    SELECT 
        [ProductKey], 
        [ProductAlternateKey], 
        [ProductSubcategoryKey],
        [WeightUnitMeasureCode], 
        [SizeUnitMeasureCode], 
        [EnglishProductName],
        [SpanishProductName], 
        [FrenchProductName],
        [StandardCost],
        [FinishedGoodsFlag], 
        [Color], 
        [SafetyStockLevel], 
        [ReorderPoint],
        [ListPrice], 
        [Size], 
        [SizeRange], 
        [Weight], 
        [DaysToManufacture],
        [ProductLine], 
        [DealerPrice], 
        [Class], 
        [Style], 
        [ModelName],
        [LargePhoto],
        [EnglishDescription], 
        [FrenchDescription],
        [ChineseDescription], 
        [ArabicDescription], 
        [HebrewDescription],
        [ThaiDescription], 
        [GermanDescription], 
        [JapaneseDescription],
        [TurkishDescription], 
        [StartDate], 
        [EndDate], 
        [Status]
    FROM [AdventureWorksDW2014].[dbo].[Product];
    
    SET IDENTITY_INSERT bronz1.Product OFF;
    PRINT '- Data loaded into Product table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --               ProductCategory Table                --
    -- =====================================================
    PRINT 'Processing ProductCategory table...';
    
    IF OBJECT_ID('bronz1.ProductCategory', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.ProductCategory;
        PRINT '- Existing ProductCategory table dropped';
    END
    
    CREATE TABLE bronz1.ProductCategory(
        [ProductCategoryKey] [int] IDENTITY(1,1) NOT NULL,
        [ProductCategoryAlternateKey] [int] NULL,
        [EnglishProductCategoryName] [nvarchar](50) NOT NULL,
        [SpanishProductCategoryName] [nvarchar](50) NOT NULL,
        [FrenchProductCategoryName] [nvarchar](50) NOT NULL
    );
    PRINT '- ProductCategory table created successfully';
    
    SET IDENTITY_INSERT bronz1.ProductCategory ON;
    
    INSERT INTO bronz1.ProductCategory (
        [ProductCategoryKey],
        [ProductCategoryAlternateKey],
        [EnglishProductCategoryName],
        [SpanishProductCategoryName],
        [FrenchProductCategoryName]
    )
    SELECT 
        [ProductCategoryKey], 
        [ProductCategoryAlternateKey],
        [EnglishProductCategoryName], 
        [SpanishProductCategoryName],
        [FrenchProductCategoryName]
    FROM [AdventureWorksDW2014].[dbo].[ProductCategory];
    
    SET IDENTITY_INSERT bronz1.ProductCategory OFF;
    PRINT '- Data loaded into ProductCategory table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --               ProductSubcategory Table             --
    -- =====================================================
    PRINT 'Processing ProductSubcategory table...';
    
    IF OBJECT_ID('bronz1.ProductSubcategory', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.ProductSubcategory;
        PRINT '- Existing ProductSubcategory table dropped';
    END
    
    CREATE TABLE bronz1.ProductSubcategory(
        [ProductSubcategoryKey] [int] IDENTITY(1,1) NOT NULL,
        [ProductSubcategoryAlternateKey] [int] NULL,
        [EnglishProductSubcategoryName] [nvarchar](50) NOT NULL,
        [SpanishProductSubcategoryName] [nvarchar](50) NOT NULL,
        [FrenchProductSubcategoryName] [nvarchar](50) NOT NULL,
        [ProductCategoryKey] [int] NULL
    );
    PRINT '- ProductSubcategory table created successfully';
    
    SET IDENTITY_INSERT bronz1.ProductSubcategory ON;
    
    INSERT INTO bronz1.ProductSubcategory (
        [ProductSubcategoryKey], 
        [ProductSubcategoryAlternateKey],
        [EnglishProductSubcategoryName], 
        [SpanishProductSubcategoryName],
        [FrenchProductSubcategoryName], 
        [ProductCategoryKey]
    )
    SELECT 
        [ProductSubcategoryKey], 
        [ProductSubcategoryAlternateKey],
        [EnglishProductSubcategoryName], 
        [SpanishProductSubcategoryName],
        [FrenchProductSubcategoryName], 
        [ProductCategoryKey]
    FROM [AdventureWorksDW2014].[dbo].[ProductSubcategory];
    
    SET IDENTITY_INSERT bronz1.ProductSubcategory OFF;
    PRINT '- Data loaded into ProductSubcategory table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                   Promotion Table                  --
    -- =====================================================
    PRINT 'Processing Promotion table...';
    
    IF OBJECT_ID('bronz1.Promotion', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Promotion;
        PRINT '- Existing Promotion table dropped';
    END
    
    CREATE TABLE bronz1.Promotion(
        [PromotionKey] [int] IDENTITY(1,1) NOT NULL,
        [PromotionAlternateKey] [int] NULL,
        [EnglishPromotionName] [nvarchar](255) NULL,
        [SpanishPromotionName] [nvarchar](255) NULL,
        [FrenchPromotionName] [nvarchar](255) NULL,
        [DiscountPct] [float] NULL,
        [EnglishPromotionType] [nvarchar](50) NULL,
        [SpanishPromotionType] [nvarchar](50) NULL,
        [FrenchPromotionType] [nvarchar](50) NULL,
        [EnglishPromotionCategory] [nvarchar](50) NULL,
        [SpanishPromotionCategory] [nvarchar](50) NULL,
        [FrenchPromotionCategory] [nvarchar](50) NULL,
        [StartDate] [datetime] NOT NULL,
        [EndDate] [datetime] NULL,
        [MinQty] [int] NULL,
        [MaxQty] [int] NULL
    );
    PRINT '- Promotion table created successfully';
    
    SET IDENTITY_INSERT bronz1.Promotion ON;
    
    INSERT INTO bronz1.Promotion (
        [PromotionKey], 
        [PromotionAlternateKey], 
        [EnglishPromotionName],
        [SpanishPromotionName], 
        [FrenchPromotionName],
        [DiscountPct],
        [EnglishPromotionType], 
        [SpanishPromotionType],
        [FrenchPromotionType],
        [EnglishPromotionCategory],
        [SpanishPromotionCategory],
        [FrenchPromotionCategory], 
        [StartDate], 
        [EndDate], 
        [MinQty], 
        [MaxQty]
    )
    SELECT 
        [PromotionKey], 
        [PromotionAlternateKey], 
        [EnglishPromotionName],
        [SpanishPromotionName], 
        [FrenchPromotionName], 
        [DiscountPct],
        [EnglishPromotionType], 
        [SpanishPromotionType], 
        [FrenchPromotionType],
        [EnglishPromotionCategory], 
        [SpanishPromotionCategory],
        [FrenchPromotionCategory], 
        [StartDate], 
        [EndDate],
        [MinQty], 
        [MaxQty]
    FROM [AdventureWorksDW2014].[dbo].[Promotion];
    
    SET IDENTITY_INSERT bronz1.Promotion OFF;
    PRINT '- Data loaded into Promotion table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                     Reseller Table                 --
    -- =====================================================
    PRINT 'Processing Reseller table...';
    
    IF OBJECT_ID('bronz1.Reseller', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Reseller;
        PRINT '- Existing Reseller table dropped';
    END
    
    CREATE TABLE bronz1.Reseller(
        [ResellerKey] [int] IDENTITY(1,1) NOT NULL,
        [GeographyKey] [int] NULL,
        [ResellerAlternateKey] [nvarchar](15) NULL,
        [Phone] [nvarchar](25) NULL,
        [BusinessType] [varchar](20) NOT NULL,
        [ResellerName] [nvarchar](50) NOT NULL,
        [NumberEmployees] [int] NULL,
        [OrderFrequency] [char](1) NULL,
        [OrderMonth] [tinyint] NULL,
        [FirstOrderYear] [int] NULL,
        [LastOrderYear] [int] NULL,
        [ProductLine] [nvarchar](50) NULL,
        [AddressLine1] [nvarchar](60) NULL,
        [AddressLine2] [nvarchar](60) NULL,
        [AnnualSales] [money] NULL,
        [BankName] [nvarchar](50) NULL,
        [MinPaymentType] [tinyint] NULL,
        [MinPaymentAmount] [money] NULL,
        [AnnualRevenue] [money] NULL,
        [YearOpened] [int] NULL
    );
    PRINT '- Reseller table created successfully';
    
    SET IDENTITY_INSERT bronz1.Reseller ON;
    
    INSERT INTO bronz1.Reseller (
        [ResellerKey], 
        [GeographyKey], 
        [ResellerAlternateKey], 
        [Phone],
        [BusinessType], 
        [ResellerName], 
        [NumberEmployees], 
        [OrderFrequency],
        [OrderMonth], 
        [FirstOrderYear], 
        [LastOrderYear], 
        [ProductLine],
        [AddressLine1], 
        [AddressLine2], 
        [AnnualSales], 
        [BankName],
        [MinPaymentType], 
        [MinPaymentAmount], 
        [AnnualRevenue], 
        [YearOpened]
    )
    SELECT 
        [ResellerKey], 
        [GeographyKey], 
        [ResellerAlternateKey], 
        [Phone],
        [BusinessType], 
        [ResellerName], 
        [NumberEmployees], 
        [OrderFrequency],
        [OrderMonth], 
        [FirstOrderYear], 
        [LastOrderYear], 
        [ProductLine],
        [AddressLine1], 
        [AddressLine2], 
        [AnnualSales], 
        [BankName],
        [MinPaymentType], [MinPaymentAmount], [AnnualRevenue], [YearOpened]
    FROM [AdventureWorksDW2014].[dbo].[Reseller];
    
    SET IDENTITY_INSERT bronz1.Reseller OFF;
    PRINT '- Data loaded into Reseller table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                    SalesReason Table               --
    -- =====================================================
    PRINT 'Processing SalesReason table...';
    
    IF OBJECT_ID('bronz1.SalesReason', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.SalesReason;
        PRINT '- Existing SalesReason table dropped';
    END
    
    CREATE TABLE bronz1.SalesReason(
        [SalesReasonKey] [int] IDENTITY(1,1) NOT NULL,
        [SalesReasonAlternateKey] [int] NOT NULL,
        [SalesReasonName] [nvarchar](50) NOT NULL,
        [SalesReasonReasonType] [nvarchar](50) NOT NULL
    );
    PRINT '- SalesReason table created successfully';
    
    SET IDENTITY_INSERT bronz1.SalesReason ON;
    
    INSERT INTO bronz1.SalesReason (
        [SalesReasonKey],
        [SalesReasonAlternateKey], 
        [SalesReasonName],
        [SalesReasonReasonType]
    )
    SELECT 
        [SalesReasonKey], 
        [SalesReasonAlternateKey],
        [SalesReasonName],
        [SalesReasonReasonType]
    FROM [AdventureWorksDW2014].[dbo].[SalesReason];
    
    SET IDENTITY_INSERT bronz1.SalesReason OFF;
    PRINT '- Data loaded into SalesReason table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                    SalesTerritory Table            --
    -- =====================================================
    PRINT 'Processing SalesTerritory table...';
    
    IF OBJECT_ID('bronz1.SalesTerritory', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.SalesTerritory;
        PRINT '- Existing SalesTerritory table dropped';
    END
    
    CREATE TABLE bronz1.SalesTerritory(
        [SalesTerritoryKey] [int] IDENTITY(1,1) NOT NULL,
        [SalesTerritoryAlternateKey] [int] NULL,
        [SalesTerritoryRegion] [nvarchar](50) NOT NULL,
        [SalesTerritoryCountry] [nvarchar](50) NOT NULL,
        [SalesTerritoryGroup] [nvarchar](50) NULL,
        [SalesTerritoryImage] [varbinary](max) NULL
    );
    PRINT '- SalesTerritory table created successfully';
    
    SET IDENTITY_INSERT bronz1.SalesTerritory ON;
    
    INSERT INTO bronz1.SalesTerritory (
        [SalesTerritoryKey], 
        [SalesTerritoryAlternateKey],
        [SalesTerritoryRegion], 
        [SalesTerritoryCountry],
        [SalesTerritoryGroup], 
        [SalesTerritoryImage]
    )
    SELECT 
        [SalesTerritoryKey], 
        [SalesTerritoryAlternateKey],
        [SalesTerritoryRegion], 
        [SalesTerritoryCountry],
        [SalesTerritoryGroup],
        [SalesTerritoryImage]
    FROM [AdventureWorksDW2014].[dbo].[SalesTerritory];
    
    SET IDENTITY_INSERT bronz1.SalesTerritory OFF;
    PRINT '- Data loaded into SalesTerritory table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                  Scenario Table                    --
    -- =====================================================
    PRINT 'Processing Scenario table...';
    
    IF OBJECT_ID('bronz1.Scenario', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.Scenario;
        PRINT '- Existing Scenario table dropped';
    END
    
    CREATE TABLE bronz1.Scenario(
        [ScenarioKey] [int] IDENTITY(1,1) NOT NULL,
        [ScenarioName] [nvarchar](50) NULL
    );
    PRINT '- Scenario table created successfully';
    
    SET IDENTITY_INSERT bronz1.Scenario ON;
    
    INSERT INTO bronz1.Scenario (
        [ScenarioKey], 
        [ScenarioName]
    )
    SELECT 
        [ScenarioKey], 
        [ScenarioName]
    FROM [AdventureWorksDW2014].[dbo].[Scenario];
    
    SET IDENTITY_INSERT bronz1.Scenario OFF;
    PRINT '- Data loaded into Scenario table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =======================================================
    --                  SECTION 2: FACT TABLES              --
    -- =======================================================
    
    -- ======================================================================
    --           FactAdditionalInternationalProductDescription Table       --
    -- ======================================================================
    PRINT 'Processing FactAdditionalInternationalProductDescription table...';
    
    IF OBJECT_ID('bronz1.FactAdditionalInternationalProductDescription', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactAdditionalInternationalProductDescription;
        PRINT '- Existing FactAdditionalInternationalProductDescription table dropped';
    END
    
    CREATE TABLE bronz1.FactAdditionalInternationalProductDescription(
        [ProductKey] [int] NOT NULL,
        [CultureName] [nvarchar](50) NOT NULL,
        [ProductDescription] [nvarchar](max) NOT NULL
    );
    PRINT '- FactAdditionalInternationalProductDescription table created successfully';
    
    INSERT INTO bronz1.FactAdditionalInternationalProductDescription
    SELECT * FROM [AdventureWorksDW2014].[dbo].[FactAdditionalInternationalProductDescription];
    PRINT '- Data loaded into FactAdditionalInternationalProductDescription table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                FactCallCenter Table                --  
    -- =====================================================
    PRINT 'Processing FactCallCenter table...';
    
    IF OBJECT_ID('bronz1.FactCallCenter', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactCallCenter;
        PRINT '- Existing FactCallCenter table dropped';
    END
    
    CREATE TABLE bronz1.FactCallCenter(
        [FactCallCenterID] [int] IDENTITY(1,1) NOT NULL,
        [DateKey] [int] NOT NULL,
        [WageType] [nvarchar](15) NOT NULL,
        [Shift] [nvarchar](20) NOT NULL,
        [LevelOneOperators] [smallint] NOT NULL,
        [LevelTwoOperators] [smallint] NOT NULL,
        [TotalOperators] [smallint] NOT NULL,
        [Calls] [int] NOT NULL,
        [AutomaticResponses] [int] NOT NULL,
        [Orders] [int] NOT NULL,
        [IssuesRaised] [smallint] NOT NULL,
        [AverageTimePerIssue] [smallint] NOT NULL,
        [ServiceGrade] [float] NOT NULL,
        [Date] [datetime] NULL
    );
    PRINT '- FactCallCenter table created successfully';
    
    SET IDENTITY_INSERT bronz1.FactCallCenter ON;
    
    INSERT INTO bronz1.FactCallCenter (
        [FactCallCenterID],
        [DateKey], 
        [WageType], 
        [Shift], 
        [LevelOneOperators],
        [LevelTwoOperators], 
        [TotalOperators], 
        [Calls], 
        [AutomaticResponses],
        [Orders], 
        [IssuesRaised], 
        [AverageTimePerIssue], 
        [ServiceGrade], 
        [Date]
    )
    SELECT 
        [FactCallCenterID], 
        [DateKey], 
        [WageType], 
        [Shift], 
        [LevelOneOperators],
        [LevelTwoOperators], 
        [TotalOperators], 
        [Calls], 
        [AutomaticResponses],
        [Orders], 
        [IssuesRaised], 
        [AverageTimePerIssue], 
        [ServiceGrade], 
        [Date]
    FROM [AdventureWorksDW2014].[dbo].[FactCallCenter];
    
    SET IDENTITY_INSERT bronz1.FactCallCenter OFF;
    PRINT '- Data loaded into FactCallCenter table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --               FactCurrencyRate Table               --
    -- =====================================================
    PRINT 'Processing FactCurrencyRate table...';
    
    IF OBJECT_ID('bronz1.FactCurrencyRate', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactCurrencyRate;
        PRINT '- Existing FactCurrencyRate table dropped';
    END
    
    CREATE TABLE bronz1.FactCurrencyRate(
        [CurrencyKey] [int] NOT NULL,
        [DateKey] [int] NOT NULL,
        [AverageRate] [float] NOT NULL,
        [EndOfDayRate] [float] NOT NULL,
        [Date] [datetime] NULL
    );
    PRINT '- FactCurrencyRate table created successfully';
    
    INSERT INTO bronz1.FactCurrencyRate
    SELECT * FROM [AdventureWorksDW2014].[dbo].[FactCurrencyRate];
    PRINT '- Data loaded into FactCurrencyRate table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                  FactFinance Table                 --
    -- =====================================================
    PRINT 'Processing FactFinance table...';
    
    IF OBJECT_ID('bronz1.FactFinance', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactFinance;
        PRINT '- Existing FactFinance table dropped';
    END
    
    CREATE TABLE bronz1.FactFinance(
        [FinanceKey] [int] IDENTITY(1,1) NOT NULL,
        [DateKey] [int] NOT NULL,
        [OrganizationKey] [int] NOT NULL,
        [DepartmentGroupKey] [int] NOT NULL,
        [ScenarioKey] [int] NOT NULL,
        [AccountKey] [int] NOT NULL,
        [Amount] [float] NOT NULL,
        [Date] [datetime] NULL
    );
    PRINT '- FactFinance table created successfully';
    
    SET IDENTITY_INSERT bronz1.FactFinance ON;
    
    INSERT INTO bronz1.FactFinance (
        [FinanceKey],
        [DateKey], 
        [OrganizationKey],
        [DepartmentGroupKey],
        [ScenarioKey], 
        [AccountKey], 
        [Amount], 
        [Date]
    )
    SELECT 
        [FinanceKey], 
        [DateKey], 
        [OrganizationKey], 
        [DepartmentGroupKey],
        [ScenarioKey], 
        [AccountKey], 
        [Amount], 
        [Date]
    FROM [AdventureWorksDW2014].[dbo].[FactFinance];
    
    SET IDENTITY_INSERT bronz1.FactFinance OFF;
    PRINT '- Data loaded into FactFinance table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --               FactInternetSales Table              --
    -- =====================================================
    PRINT 'Processing FactInternetSales table...';
    
    IF OBJECT_ID('bronz1.FactInternetSales', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactInternetSales;
        PRINT '- Existing FactInternetSales table dropped';
    END
    
    CREATE TABLE bronz1.FactInternetSales(
        [ProductKey] [int] NOT NULL,
        [OrderDateKey] [int] NOT NULL,
        [DueDateKey] [int] NOT NULL,
        [ShipDateKey] [int] NOT NULL,
        [CustomerKey] [int] NOT NULL,
        [PromotionKey] [int] NOT NULL,
        [CurrencyKey] [int] NOT NULL,
        [SalesTerritoryKey] [int] NOT NULL,
        [SalesOrderNumber] [nvarchar](20) NOT NULL,
        [SalesOrderLineNumber] [tinyint] NOT NULL,
        [RevisionNumber] [tinyint] NOT NULL,
        [OrderQuantity] [smallint] NOT NULL,
        [UnitPrice] [money] NOT NULL,
        [ExtendedAmount] [money] NOT NULL,
        [UnitPriceDiscountPct] [float] NOT NULL,
        [DiscountAmount] [float] NOT NULL,
        [ProductStandardCost] [money] NOT NULL,
        [TotalProductCost] [money] NOT NULL,
        [SalesAmount] [money] NOT NULL,
        [TaxAmt] [money] NOT NULL,
        [Freight] [money] NOT NULL,
        [CarrierTrackingNumber] [nvarchar](25) NULL,
        [CustomerPONumber] [nvarchar](25) NULL,
        [OrderDate] [datetime] NULL,
        [DueDate] [datetime] NULL,
        [ShipDate] [datetime] NULL
    );
    PRINT '- FactInternetSales table created successfully';
    
    INSERT INTO bronz1.FactInternetSales
    SELECT * FROM [AdventureWorksDW2014].[dbo].[FactInternetSales];
    PRINT '- Data loaded into FactInternetSales table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --           FactInternetSalesReason Table            --
    -- =====================================================
    PRINT 'Processing FactInternetSalesReason table...';
    
    IF OBJECT_ID('bronz1.FactInternetSalesReason', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactInternetSalesReason;
        PRINT '- Existing FactInternetSalesReason table dropped';
    END
    
    CREATE TABLE bronz1.FactInternetSalesReason(
        [SalesOrderNumber] [nvarchar](20) NOT NULL,
        [SalesOrderLineNumber] [tinyint] NOT NULL,
        [SalesReasonKey] [int] NOT NULL
    );
    PRINT '- FactInternetSalesReason table created successfully';
    
    INSERT INTO bronz1.FactInternetSalesReason
    SELECT * FROM [AdventureWorksDW2014].[dbo].[FactInternetSalesReason];
    PRINT '- Data loaded into FactInternetSalesReason table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                FactProductInventory Table          --
    -- =====================================================
    PRINT 'Processing FactProductInventory table...';
    
    IF OBJECT_ID('bronz1.FactProductInventory', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactProductInventory;
        PRINT '- Existing FactProductInventory table dropped';
    END
    
    CREATE TABLE bronz1.FactProductInventory(
        [ProductKey] [int] NOT NULL,
        [DateKey] [int] NOT NULL,
        [MovementDate] [date] NOT NULL,
        [UnitCost] [money] NOT NULL,
        [UnitsIn] [int] NOT NULL,
        [UnitsOut] [int] NOT NULL,
        [UnitsBalance] [int] NOT NULL
    );
    PRINT '- FactProductInventory table created successfully';
    
    INSERT INTO bronz1.FactProductInventory
    SELECT * FROM [AdventureWorksDW2014].[dbo].[FactProductInventory];
    PRINT '- Data loaded into FactProductInventory table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                FactResellerSales Table             --
    -- =====================================================
    PRINT 'Processing FactResellerSales table...';
    
    IF OBJECT_ID('bronz1.FactResellerSales', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactResellerSales;
        PRINT '- Existing FactResellerSales table dropped';
    END
    
    CREATE TABLE bronz1.FactResellerSales(
        [ProductKey] [int] NOT NULL,
        [OrderDateKey] [int] NOT NULL,
        [DueDateKey] [int] NOT NULL,
        [ShipDateKey] [int] NOT NULL,
        [ResellerKey] [int] NOT NULL,
        [EmployeeKey] [int] NOT NULL,
        [PromotionKey] [int] NOT NULL,
        [CurrencyKey] [int] NOT NULL,
        [SalesTerritoryKey] [int] NOT NULL,
        [SalesOrderNumber] [nvarchar](20) NOT NULL,
        [SalesOrderLineNumber] [tinyint] NOT NULL,
        [RevisionNumber] [tinyint] NULL,
        [OrderQuantity] [smallint] NULL,
        [UnitPrice] [money] NULL,
        [ExtendedAmount] [money] NULL,
        [UnitPriceDiscountPct] [float] NULL,
        [DiscountAmount] [float] NULL,
        [ProductStandardCost] [money] NULL,
        [TotalProductCost] [money] NULL,
        [SalesAmount] [money] NULL,
        [TaxAmt] [money] NULL,
        [Freight] [money] NULL,
        [CarrierTrackingNumber] [nvarchar](25) NULL,
        [CustomerPONumber] [nvarchar](25) NULL,
        [OrderDate] [datetime] NULL,
        [DueDate] [datetime] NULL,
        [ShipDate] [datetime] NULL
    );
    PRINT '- FactResellerSales table created successfully';
    
    INSERT INTO bronz1.FactResellerSales
    SELECT * FROM [AdventureWorksDW2014].[dbo].[FactResellerSales];
    PRINT '- Data loaded into FactResellerSales table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --               FactSalesQuota Table                 --
    -- =====================================================
    PRINT 'Processing FactSalesQuota table...';
    
    IF OBJECT_ID('bronz1.FactSalesQuota', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactSalesQuota;
        PRINT '- Existing FactSalesQuota table dropped';
    END
    
    CREATE TABLE bronz1.FactSalesQuota(
        [SalesQuotaKey] [int] IDENTITY(1,1) NOT NULL,
        [EmployeeKey] [int] NOT NULL,
        [DateKey] [int] NOT NULL,
        [CalendarYear] [smallint] NOT NULL,
        [CalendarQuarter] [tinyint] NOT NULL,
        [SalesAmountQuota] [money] NOT NULL,
        [Date] [datetime] NULL
    );
    PRINT '- FactSalesQuota table created successfully';
    
    SET IDENTITY_INSERT bronz1.FactSalesQuota ON;
    
    INSERT INTO bronz1.FactSalesQuota (
        [SalesQuotaKey], 
        [EmployeeKey], 
        [DateKey],
        [CalendarYear],
        [CalendarQuarter], 
        [SalesAmountQuota], 
        [Date]
    )
    SELECT 
        [SalesQuotaKey], 
        [EmployeeKey], 
        [DateKey], 
        [CalendarYear],
        [CalendarQuarter], 
        [SalesAmountQuota], 
        [Date]
    FROM [AdventureWorksDW2014].[dbo].[FactSalesQuota];
    
    SET IDENTITY_INSERT bronz1.FactSalesQuota OFF;
    PRINT '- Data loaded into FactSalesQuota table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --             FactSurveyResponse Table               --
    -- =====================================================
    PRINT 'Processing FactSurveyResponse table...';
    
    IF OBJECT_ID('bronz1.FactSurveyResponse', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.FactSurveyResponse;
        PRINT '- Existing FactSurveyResponse table dropped';
    END
    
    CREATE TABLE bronz1.FactSurveyResponse(
        [SurveyResponseKey] [int] IDENTITY(1,1) NOT NULL,
        [DateKey] [int] NOT NULL,
        [CustomerKey] [int] NOT NULL,
        [ProductCategoryKey] [int] NOT NULL,
        [EnglishProductCategoryName] [nvarchar](50) NOT NULL,
        [ProductSubcategoryKey] [int] NOT NULL,
        [EnglishProductSubcategoryName] [nvarchar](50) NOT NULL,
        [Date] [datetime] NULL
    );
    PRINT '- FactSurveyResponse table created successfully';
    
    SET IDENTITY_INSERT bronz1.FactSurveyResponse ON;
    
    INSERT INTO bronz1.FactSurveyResponse (
        [SurveyResponseKey],
        [DateKey], 
        [CustomerKey], 
        [ProductCategoryKey],
        [EnglishProductCategoryName], 
        [ProductSubcategoryKey],
        [EnglishProductSubcategoryName], 
        [Date]
    )
    SELECT 
        [SurveyResponseKey], 
        [DateKey], 
        [CustomerKey], 
        [ProductCategoryKey],
        [EnglishProductCategoryName], 
        [ProductSubcategoryKey],
        [EnglishProductSubcategoryName], 
        [Date]
    FROM [AdventureWorksDW2014].[dbo].[FactSurveyResponse];
    
    SET IDENTITY_INSERT bronz1.FactSurveyResponse OFF;
    PRINT '- Data loaded into FactSurveyResponse table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                NewFactCurrencyRate Table           --
    -- =====================================================
    PRINT 'Processing NewFactCurrencyRate table...';
    
    IF OBJECT_ID('bronz1.NewFactCurrencyRate', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.NewFactCurrencyRate;
        PRINT '- Existing NewFactCurrencyRate table dropped';
    END
    
    CREATE TABLE bronz1.NewFactCurrencyRate(
        [AverageRate] [real] NULL,
        [CurrencyID] [nvarchar](3) NULL,
        [CurrencyDate] [date] NULL,
        [EndOfDayRate] [real] NULL,
        [CurrencyKey] [int] NULL,
        [DateKey] [int] NULL
    );
    PRINT '- NewFactCurrencyRate table created successfully';
    
    INSERT INTO bronz1.NewFactCurrencyRate
    SELECT * FROM [AdventureWorksDW2014].[dbo].[NewFactCurrencyRate];
    PRINT '- Data loaded into NewFactCurrencyRate table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                 ProspectiveBuyer Table             --
    -- =====================================================
    PRINT 'Processing ProspectiveBuyer table...';
    
    IF OBJECT_ID('bronz1.ProspectiveBuyer', 'U') IS NOT NULL
    BEGIN
        DROP TABLE bronz1.ProspectiveBuyer;
        PRINT '- Existing ProspectiveBuyer table dropped';
    END
    
    CREATE TABLE bronz1.ProspectiveBuyer(
        [ProspectiveBuyerKey] [int] IDENTITY(1,1) NOT NULL,
        [ProspectAlternateKey] [nvarchar](15) NULL,
        [FirstName] [nvarchar](50) NULL,
        [MiddleName] [nvarchar](50) NULL,
        [LastName] [nvarchar](50) NULL,
        [BirthDate] [datetime] NULL,
        [MaritalStatus] [nchar](1) NULL,
        [Gender] [nvarchar](1) NULL,
        [EmailAddress] [nvarchar](50) NULL,
        [YearlyIncome] [money] NULL,
        [TotalChildren] [tinyint] NULL,
        [NumberChildrenAtHome] [tinyint] NULL,
        [Education] [nvarchar](40) NULL,
        [Occupation] [nvarchar](100) NULL,
        [HouseOwnerFlag] [nchar](1) NULL,
        [NumberCarsOwned] [tinyint] NULL,
        [AddressLine1] [nvarchar](120) NULL,
        [AddressLine2] [nvarchar](120) NULL,
        [City] [nvarchar](30) NULL,
        [StateProvinceCode] [nvarchar](3) NULL,
        [PostalCode] [nvarchar](15) NULL,
        [Phone] [nvarchar](20) NULL,
        [Salutation] [nvarchar](8) NULL,
        [Unknown] [int] NULL
    );
    PRINT '- ProspectiveBuyer table created successfully';
    
    SET IDENTITY_INSERT bronz1.ProspectiveBuyer ON;
    
    INSERT INTO bronz1.ProspectiveBuyer (
        [ProspectiveBuyerKey],
        [ProspectAlternateKey], 
        [FirstName],
        [MiddleName], 
        [LastName], 
        [BirthDate], 
        [MaritalStatus], 
        [Gender],
        [EmailAddress], 
        [YearlyIncome], 
        [TotalChildren], 
        [NumberChildrenAtHome],
        [Education], 
        [Occupation], 
        [HouseOwnerFlag], 
        [NumberCarsOwned],
        [AddressLine1], 
        [AddressLine2], 
        [City], 
        [StateProvinceCode],
        [PostalCode],
        [Phone], 
        [Salutation], 
        [Unknown]
    )
    SELECT 
        [ProspectiveBuyerKey], 
        [ProspectAlternateKey], 
        [FirstName],
        [MiddleName], 
        [LastName], 
        [BirthDate], 
        [MaritalStatus], 
        [Gender],
        [EmailAddress], 
        [YearlyIncome], 
        [TotalChildren], 
        [NumberChildrenAtHome],
        [Education], 
        [Occupation], 
        [HouseOwnerFlag], 
        [NumberCarsOwned],
        [AddressLine1],
        [AddressLine2], 
        [City],
        [StateProvinceCode],
        [PostalCode], 
        [Phone],
        [Salutation], 
        [Unknown]
    FROM [AdventureWorksDW2014].[dbo].[ProspectiveBuyer];
    
    SET IDENTITY_INSERT bronz1.ProspectiveBuyer OFF;
    PRINT '- Data loaded into ProspectiveBuyer table. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    
    -- =====================================================
    --                  COMMIT TRANSACTION                --
    -- =====================================================
    COMMIT TRANSACTION;
    
    PRINT '=========================================================================';
    PRINT '    Bronze Layer Table Creation and Data Load COMPLETED SUCCESSFULLY!';
    PRINT '=========================================================================';
    
END TRY
BEGIN CATCH
    -- =====================================================
    --                   ERROR HANDLING                   --
    -- =====================================================
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    PRINT '====================================================';
    PRINT '      ERROR OCCURRED! Transaction rolled back.';
    PRINT '====================================================';
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
    PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
    PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
    PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    PRINT '====================================================';
    
    -- Re-throw the error
    THROW;
END CATCH;
END

GO

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================
-- Uncomment below to verify table counts after execution
/*
SELECT 'Account' AS TableName, COUNT(*) AS RowCount FROM bronz1.Account UNION ALL
SELECT 'Currency', COUNT(*) FROM bronz1.Currency UNION ALL
SELECT 'Customer', COUNT(*) FROM bronz1.Customer UNION ALL
SELECT 'Date', COUNT(*) FROM bronz1.Date UNION ALL
SELECT 'DepartmentGroup', COUNT(*) FROM bronz1.DepartmentGroup UNION ALL
SELECT 'Employee', COUNT(*) FROM bronz1.Employee UNION ALL
SELECT 'Geography', COUNT(*) FROM bronz1.Geography UNION ALL
SELECT 'Organization', COUNT(*) FROM bronz1.Organization UNION ALL
SELECT 'Product', COUNT(*) FROM bronz1.Product UNION ALL
SELECT 'ProductCategory', COUNT(*) FROM bronz1.ProductCategory UNION ALL
SELECT 'ProductSubcategory', COUNT(*) FROM bronz1.ProductSubcategory UNION ALL
SELECT 'Promotion', COUNT(*) FROM bronz1.Promotion UNION ALL
SELECT 'Reseller', COUNT(*) FROM bronz1.Reseller UNION ALL
SELECT 'SalesReason', COUNT(*) FROM bronz1.SalesReason UNION ALL
SELECT 'SalesTerritory', COUNT(*) FROM bronz1.SalesTerritory UNION ALL
SELECT 'Scenario', COUNT(*) FROM bronz1.Scenario UNION ALL
SELECT 'FactAdditionalInternationalProductDescription', COUNT(*) FROM bronz1.FactAdditionalInternationalProductDescription UNION ALL
SELECT 'FactCallCenter', COUNT(*) FROM bronz1.FactCallCenter UNION ALL
SELECT 'FactCurrencyRate', COUNT(*) FROM bronz1.FactCurrencyRate UNION ALL
SELECT 'FactFinance', COUNT(*) FROM bronz1.FactFinance UNION ALL
SELECT 'FactInternetSales', COUNT(*) FROM bronz1.FactInternetSales UNION ALL
SELECT 'FactInternetSalesReason', COUNT(*) FROM bronz1.FactInternetSalesReason UNION ALL
SELECT 'FactProductInventory', COUNT(*) FROM bronz1.FactProductInventory UNION ALL
SELECT 'FactResellerSales', COUNT(*) FROM bronz1.FactResellerSales UNION ALL
SELECT 'FactSalesQuota', COUNT(*) FROM bronz1.FactSalesQuota UNION ALL
SELECT 'FactSurveyResponse', COUNT(*) FROM bronz1.FactSurveyResponse UNION ALL
SELECT 'NewFactCurrencyRate', COUNT(*) FROM bronz1.NewFactCurrencyRate UNION ALL
SELECT 'ProspectiveBuyer', COUNT(*) FROM bronz1.ProspectiveBuyer
ORDER BY TableName;
*/
GO
