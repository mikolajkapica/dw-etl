-- =====================================================
-- Himalayan Expeditions Data Warehouse Schema
-- MS SQL Server DDL Scripts
-- =====================================================

-- Create database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'HimalayanExpeditionsDW')
BEGIN
    CREATE DATABASE HimalayanExpeditionsDW;
END
GO

USE HimalayanExpeditionsDW;
GO

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- DIM_Date: Date dimension for time-based analysis
CREATE TABLE DIM_Date (
    DateKey INT IDENTITY(1,1) PRIMARY KEY,
    Date DATE UNIQUE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    DayOfYear INT NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName VARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL,
    Season VARCHAR(20) NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- Create index on Date for fast lookups
CREATE INDEX IX_DIM_Date_Date ON DIM_Date(Date);
CREATE INDEX IX_DIM_Date_Year ON DIM_Date(Year);

-- DIM_Nationality: Member and expedition nationalities
CREATE TABLE DIM_Nationality (
    NationalityKey INT IDENTITY(1,1) PRIMARY KEY,
    CountryCode VARCHAR(3) UNIQUE NOT NULL,
    CountryName VARCHAR(100) NOT NULL,
    Region VARCHAR(100),
    SubRegion VARCHAR(100),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- DIM_Peak: Mountain peaks information
CREATE TABLE DIM_Peak (
    PeakKey INT IDENTITY(1,1) PRIMARY KEY,
    PeakID VARCHAR(20) UNIQUE NOT NULL,
    PeakName VARCHAR(200) NOT NULL,
    HeightMeters DECIMAL(8,2),
    ClimbingStatus VARCHAR(50),
    FirstAscentYear INT,
    HostCountryCode VARCHAR(3),
    Coordinates VARCHAR(50),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- Create indexes for peak lookups
CREATE INDEX IX_DIM_Peak_PeakID ON DIM_Peak(PeakID);
CREATE INDEX IX_DIM_Peak_HeightMeters ON DIM_Peak(HeightMeters);

-- DIM_Route: Climbing routes
CREATE TABLE DIM_Route (
    RouteKey INT IDENTITY(1,1) PRIMARY KEY,
    Route1 VARCHAR(200),
    Route2 VARCHAR(200),
    CombinedRoute VARCHAR(400),
    RouteType VARCHAR(50),
    DifficultyLevel VARCHAR(20),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- DIM_ExpeditionStatus: Status codes and termination reasons
CREATE TABLE DIM_ExpeditionStatus (
    StatusKey INT IDENTITY(1,1) PRIMARY KEY,
    TerminationReason VARCHAR(50) UNIQUE NOT NULL,
    StatusCategory VARCHAR(50) NOT NULL,
    IsSuccess BIT NOT NULL,
    StatusDescription VARCHAR(200),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- DIM_HostCountry: Host countries for expeditions
CREATE TABLE DIM_HostCountry (
    HostCountryKey INT IDENTITY(1,1) PRIMARY KEY,
    CountryCode VARCHAR(3) UNIQUE NOT NULL,
    CountryName VARCHAR(100) NOT NULL,
    Region VARCHAR(100),
    PoliticalSystem VARCHAR(50),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- DIM_Member: Expedition members (SCD Type 2)
CREATE TABLE DIM_Member (
    MemberKey INT IDENTITY(1,1) PRIMARY KEY,
    MemberID VARCHAR(50) NOT NULL,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    FullName VARCHAR(200),
    Gender VARCHAR(10),
    CitizenshipCountryCode VARCHAR(3),
    Age INT,
    Occupation VARCHAR(100),
    IsCurrentRecord BIT DEFAULT 1,
    EffectiveDate DATETIME2 DEFAULT GETDATE(),
    ExpirationDate DATETIME2 DEFAULT '9999-12-31',
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- Create indexes for member lookups
CREATE INDEX IX_DIM_Member_MemberID ON DIM_Member(MemberID);
CREATE INDEX IX_DIM_Member_FullName ON DIM_Member(FullName);
CREATE INDEX IX_DIM_Member_Current ON DIM_Member(IsCurrentRecord);

-- DIM_CountryIndicators: World Bank economic indicators
CREATE TABLE DIM_CountryIndicators (
    IndicatorKey INT IDENTITY(1,1) PRIMARY KEY,
    CountryCode VARCHAR(3) NOT NULL,
    IndicatorCode VARCHAR(50) NOT NULL,
    IndicatorName VARCHAR(200) NOT NULL,
    Year INT NOT NULL,
    Value DECIMAL(18,4),
    LastUpdated DATETIME2 DEFAULT GETDATE(),
    DataSource VARCHAR(50) DEFAULT 'World Bank',
    UNIQUE(CountryCode, IndicatorCode, Year)
);

-- Create indexes for indicator lookups
CREATE INDEX IX_DIM_CountryIndicators_Country_Year ON DIM_CountryIndicators(CountryCode, Year);
CREATE INDEX IX_DIM_CountryIndicators_Indicator ON DIM_CountryIndicators(IndicatorCode);

-- =====================================================
-- FACT TABLE
-- =====================================================

-- FACT_Expeditions: Main fact table for expedition analysis
CREATE TABLE FACT_Expeditions (
    ExpeditionKey INT IDENTITY(1,1) PRIMARY KEY,
    ExpeditionID VARCHAR(20) NOT NULL,
    
    -- Foreign Keys to Dimensions
    DateKey INT NOT NULL,
    PeakKey INT NOT NULL,
    RouteKey INT,
    StatusKey INT NOT NULL,
    HostCountryKey INT NOT NULL,
    LeaderNationalityKey INT,
    
    -- Measures and Facts
    ExpeditionYear INT NOT NULL,
    Season VARCHAR(20),
    TotalMembers INT DEFAULT 0,
    TotalDays INT,
    Basecamp_Date DATE,
    Highpoint_Meters DECIMAL(8,2),
    
    -- Expedition Details
    ExpeditionName VARCHAR(200),
    Agency VARCHAR(200),
    Sponsor VARCHAR(500),
    
    -- Success Metrics
    IsSuccess BIT DEFAULT 0,
    SummitAttempts INT DEFAULT 0,
    SummitSuccesses INT DEFAULT 0,
    Deaths INT DEFAULT 0,
    Injuries INT DEFAULT 0,
    
    -- Oxygen Usage
    OxygenUsed BIT DEFAULT 0,
    
    -- Costs (if available)
    TotalCost DECIMAL(12,2),
    
    -- Metadata
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE(),
    
    -- Foreign Key Constraints
    CONSTRAINT FK_FACT_Expeditions_Date FOREIGN KEY (DateKey) REFERENCES DIM_Date(DateKey),
    CONSTRAINT FK_FACT_Expeditions_Peak FOREIGN KEY (PeakKey) REFERENCES DIM_Peak(PeakKey),
    CONSTRAINT FK_FACT_Expeditions_Route FOREIGN KEY (RouteKey) REFERENCES DIM_Route(RouteKey),
    CONSTRAINT FK_FACT_Expeditions_Status FOREIGN KEY (StatusKey) REFERENCES DIM_ExpeditionStatus(StatusKey),
    CONSTRAINT FK_FACT_Expeditions_HostCountry FOREIGN KEY (HostCountryKey) REFERENCES DIM_HostCountry(HostCountryKey),
    CONSTRAINT FK_FACT_Expeditions_LeaderNationality FOREIGN KEY (LeaderNationalityKey) REFERENCES DIM_Nationality(NationalityKey)
);

-- Create indexes for fact table performance
CREATE INDEX IX_FACT_Expeditions_ExpeditionID ON FACT_Expeditions(ExpeditionID);
CREATE INDEX IX_FACT_Expeditions_Year ON FACT_Expeditions(ExpeditionYear);
CREATE INDEX IX_FACT_Expeditions_Peak ON FACT_Expeditions(PeakKey);
CREATE INDEX IX_FACT_Expeditions_Date ON FACT_Expeditions(DateKey);
CREATE INDEX IX_FACT_Expeditions_Success ON FACT_Expeditions(IsSuccess);

-- =====================================================
-- BRIDGE TABLE FOR MANY-TO-MANY RELATIONSHIPS
-- =====================================================

-- Bridge table for expedition members (many-to-many)
CREATE TABLE BRIDGE_ExpeditionMembers (
    ExpeditionKey INT NOT NULL,
    MemberKey INT NOT NULL,
    MemberRole VARCHAR(50),
    IsLeader BIT DEFAULT 0,
    IsDeputyLeader BIT DEFAULT 0,
    ReachedSummit BIT DEFAULT 0,
    Death BIT DEFAULT 0,
    Injury BIT DEFAULT 0,
    OxygenUsed BIT DEFAULT 0,
    AgeAtExpedition INT,
    
    PRIMARY KEY (ExpeditionKey, MemberKey),
    
    CONSTRAINT FK_BRIDGE_ExpeditionMembers_Expedition 
        FOREIGN KEY (ExpeditionKey) REFERENCES FACT_Expeditions(ExpeditionKey),
    CONSTRAINT FK_BRIDGE_ExpeditionMembers_Member 
        FOREIGN KEY (MemberKey) REFERENCES DIM_Member(MemberKey)
);

-- =====================================================
-- ETL CONTROL TABLES
-- =====================================================

-- ETL Audit Log
CREATE TABLE ETL_AuditLog (
    AuditLogKey INT IDENTITY(1,1) PRIMARY KEY,
    JobName VARCHAR(100) NOT NULL,
    StepName VARCHAR(100) NOT NULL,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2,
    Status VARCHAR(20) NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    RowsProcessed INT DEFAULT 0,
    ErrorMessage NVARCHAR(MAX),
    CreatedDate DATETIME2 DEFAULT GETDATE()
);

-- ETL Configuration
CREATE TABLE ETL_Configuration (
    ConfigKey VARCHAR(50) PRIMARY KEY,
    ConfigValue VARCHAR(500) NOT NULL,
    Description VARCHAR(200),
    LastModified DATETIME2 DEFAULT GETDATE()
);

-- Insert initial ETL configuration
INSERT INTO ETL_Configuration (ConfigKey, ConfigValue, Description) VALUES
('LastProcessedDate', '1900-01-01', 'Last date processed for incremental loads'),
('DataSourcePath', 'data/', 'Path to source data files'),
('MaxRetryAttempts', '3', 'Maximum retry attempts for failed operations'),
('BatchSize', '1000', 'Batch size for bulk operations');

GO

-- =====================================================
-- STORED PROCEDURES FOR ETL OPERATIONS
-- =====================================================

-- Procedure to get next surrogate key (if needed)
CREATE PROCEDURE sp_GetNextSurrogateKey
    @TableName VARCHAR(100),
    @NextKey INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @TableName = 'DIM_Date'
        SELECT @NextKey = ISNULL(MAX(DateKey), 0) + 1 FROM DIM_Date;
    ELSE IF @TableName = 'DIM_Nationality'
        SELECT @NextKey = ISNULL(MAX(NationalityKey), 0) + 1 FROM DIM_Nationality;
    ELSE IF @TableName = 'DIM_Peak'
        SELECT @NextKey = ISNULL(MAX(PeakKey), 0) + 1 FROM DIM_Peak;
    ELSE IF @TableName = 'DIM_Route'
        SELECT @NextKey = ISNULL(MAX(RouteKey), 0) + 1 FROM DIM_Route;
    ELSE IF @TableName = 'DIM_ExpeditionStatus'
        SELECT @NextKey = ISNULL(MAX(StatusKey), 0) + 1 FROM DIM_ExpeditionStatus;
    ELSE IF @TableName = 'DIM_HostCountry'
        SELECT @NextKey = ISNULL(MAX(HostCountryKey), 0) + 1 FROM DIM_HostCountry;
    ELSE IF @TableName = 'DIM_Member'
        SELECT @NextKey = ISNULL(MAX(MemberKey), 0) + 1 FROM DIM_Member;
    ELSE IF @TableName = 'DIM_CountryIndicators'
        SELECT @NextKey = ISNULL(MAX(IndicatorKey), 0) + 1 FROM DIM_CountryIndicators;
    ELSE
        SET @NextKey = 1;
END;
GO

PRINT 'Database schema created successfully!';
