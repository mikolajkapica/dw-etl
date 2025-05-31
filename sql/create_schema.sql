IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'HimalayanExpeditionsDW')
BEGIN
    CREATE DATABASE HimalayanExpeditionsDW;
END
GO

USE HimalayanExpeditionsDW;
GO

-- dim_date: ['Date', 'Year', 'Quarter', 'Month', 'MonthName', 'DayOfYear', 'DayOfMonth', 'DayOfWeek', 'DayName', 'IsWeekend', 'DateKey', 'Season', 'CreatedDate', 'ModifiedDate']

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
-- dim_nationality: ['CountryCode', 'CountryName', 'Region', 'SubRegion', 'IsActive', 'CreatedDate', 'ModifiedDate', 'NationalityKey']

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

-- dim_peak: ['PeakID', 'PeakName', 'HeightMeters', 'ClimbingStatus', 'HostCountryCode', 'Coordinates', 'IsActive', 'CreatedDate', 'ModifiedDate', 'PeakKey']
CREATE TABLE DIM_Peak (
    PeakKey INT IDENTITY(1,1) PRIMARY KEY,
    PeakID VARCHAR(20) UNIQUE NOT NULL,
    PeakName VARCHAR(200) NOT NULL,
    HeightMeters DECIMAL(8,2),
    ClimbingStatus VARCHAR(50),
    HostCountryCode VARCHAR(3),
    Coordinates VARCHAR(50),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- dim_route: ['Route1', 'Route2', 'CombinedRoute', 'RouteType', 'DifficultyLevel', 'IsActive', 'CreatedDate', 'ModifiedDate', 'RouteKey']
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

-- dim_expedition_status: ['TerminationReason', 'StatusCategory', 'IsSuccess', 'StatusDescription', 'IsActive', 'CreatedDate', 'ModifiedDate', 'StatusKey']
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

-- dim_host_country: ['CountryCode', 'CountryName', 'CreatedDate', 'ModifiedDate', 'Region', 'Subregion', 'HostCountryKey']
CREATE TABLE DIM_HostCountry (
    HostCountryKey INT IDENTITY(1,1) PRIMARY KEY,
    CountryCode VARCHAR(3) UNIQUE NOT NULL,
    CountryName VARCHAR(100) NOT NULL,
    Region VARCHAR(100),
    Subregion VARCHAR(100),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- dim_member: ['MemberID', 'FullName', 'FNAME', 'LNAME', 'Gender', 'Age', 'AgeGroup', 'BirthYear', 'CitizenshipCountry', 'CreatedDate', 'ModifiedDate', 'MemberKey']
CREATE TABLE DIM_Member (
    MemberKey INT IDENTITY(1,1) PRIMARY KEY,
    MemberID VARCHAR(50) NOT NULL,
    FNAME VARCHAR(100) NOT NULL,
    LNAME VARCHAR(100) NOT NULL,
    FullName VARCHAR(200),
    BirthYear INT,
    Gender VARCHAR(10),
    CitizenshipCountry VARCHAR(50) NOT NULL,
    Age INT,
    AgeGroup VARCHAR(20),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE DIM_CountryIndicators (
    IndicatorKey INT IDENTITY(1,1) PRIMARY KEY,
    CountryCode VARCHAR(3) NOT NULL,
    IndicatorCode VARCHAR(50) NOT NULL,
    HEALTH_EXPENDITURE DECIMAL(18,4),
    GDP_PER_CAPITA_USD DECIMAL(18,4),
    LAND_AREA DECIMAL(18,4),
    LIFE_EXPECTANCY DECIMAL(18,4),
    LITERACY_RATE DECIMAL(18,4),
    IndicatorName VARCHAR(200) NOT NULL,
    Year INT NOT NULL,
    Value DECIMAL(18,4),
    LastUpdated DATETIME2 DEFAULT GETDATE(),
    DataSource VARCHAR(50) DEFAULT 'World Bank',
    UNIQUE(CountryCode, IndicatorCode, Year)
);

CREATE TABLE FACT_Expeditions (
    ExpeditionKey INT IDENTITY(1,1) PRIMARY KEY,
    ExpeditionID VARCHAR(20) NOT NULL,
    MemberKey INT NOT NULL,
    DateKey INT NOT NULL,
    PeakKey INT NOT NULL,
    RouteKey INT,
    StatusKey INT NOT NULL,
    HostCountryKey INT NOT NULL,
    LeaderNationalityKey INT,
    ExpeditionYear INT NOT NULL,
    Season VARCHAR(20),
    TotalMembers INT DEFAULT 0,
    TotalDays INT,
    Basecamp_Date DATE,
    Highpoint_Meters DECIMAL(8,2),
    ExpeditionName VARCHAR(200),
    Agency VARCHAR(200),
    Sponsor VARCHAR(500),
    IsSuccess BIT DEFAULT 0,
    SummitAttempts INT DEFAULT 0,
    SummitSuccesses INT DEFAULT 0,
    Deaths INT DEFAULT 0,
    Injuries INT DEFAULT 0,
    TotalCost DECIMAL(12,2),
    MemberRole VARCHAR(50),
    IsLeader BIT DEFAULT 0,
    IsDeputyLeader BIT DEFAULT 0,
    ReachedSummit BIT DEFAULT 0,
    Death BIT DEFAULT 0,
    Injury BIT DEFAULT 0,
    OxygenUsed BIT DEFAULT 0,
    AgeAtExpedition INT,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FACT_Expeditions_Date FOREIGN KEY (DateKey) REFERENCES DIM_Date(DateKey),
    CONSTRAINT FK_FACT_Expeditions_Peak FOREIGN KEY (PeakKey) REFERENCES DIM_Peak(PeakKey),
    CONSTRAINT FK_FACT_Expeditions_Route FOREIGN KEY (RouteKey) REFERENCES DIM_Route(RouteKey),
    CONSTRAINT FK_FACT_Expeditions_Status FOREIGN KEY (StatusKey) REFERENCES DIM_ExpeditionStatus(StatusKey),
    CONSTRAINT FK_FACT_Expeditions_HostCountry FOREIGN KEY (HostCountryKey) REFERENCES DIM_HostCountry(HostCountryKey),
    CONSTRAINT FK_FACT_Expeditions_LeaderNationality FOREIGN KEY (LeaderNationalityKey) REFERENCES DIM_Nationality(NationalityKey),
    CONSTRAINT FK_FACT_Expeditions_Member FOREIGN KEY (MemberKey) REFERENCES DIM_Member(MemberKey)
);

SET IDENTITY_INSERT DIM_Date ON;
SET IDENTITY_INSERT DIM_Nationality ON;
SET IDENTITY_INSERT DIM_Peak ON;
SET IDENTITY_INSERT DIM_Route ON;
SET IDENTITY_INSERT DIM_ExpeditionStatus ON;
SET IDENTITY_INSERT DIM_HostCountry ON;
SET IDENTITY_INSERT DIM_Member ON;
SET IDENTITY_INSERT DIM_CountryIndicators ON;
SET IDENTITY_INSERT FACT_Expeditions ON;

