IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'HimalayanExpeditionsDW')
BEGIN
    CREATE DATABASE HimalayanExpeditionsDW;
END
GO

USE HimalayanExpeditionsDW;
GO

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

CREATE INDEX IX_DIM_Date_Date ON DIM_Date(Date);
CREATE INDEX IX_DIM_Date_Year ON DIM_Date(Year);

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

CREATE INDEX IX_DIM_Peak_PeakID ON DIM_Peak(PeakID);
CREATE INDEX IX_DIM_Peak_HeightMeters ON DIM_Peak(HeightMeters);

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

CREATE INDEX IX_DIM_Member_MemberID ON DIM_Member(MemberID);
CREATE INDEX IX_DIM_Member_FullName ON DIM_Member(FullName);
CREATE INDEX IX_DIM_Member_Current ON DIM_Member(IsCurrentRecord);

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

CREATE INDEX IX_DIM_CountryIndicators_Country_Year ON DIM_CountryIndicators(CountryCode, Year);
CREATE INDEX IX_DIM_CountryIndicators_Indicator ON DIM_CountryIndicators(IndicatorCode);

CREATE TABLE FACT_Expeditions (
    ExpeditionKey INT IDENTITY(1,1) PRIMARY KEY,
    ExpeditionID VARCHAR(20) NOT NULL,
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
    OxygenUsed BIT DEFAULT 0,
    TotalCost DECIMAL(12,2),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FACT_Expeditions_Date FOREIGN KEY (DateKey) REFERENCES DIM_Date(DateKey),
    CONSTRAINT FK_FACT_Expeditions_Peak FOREIGN KEY (PeakKey) REFERENCES DIM_Peak(PeakKey),
    CONSTRAINT FK_FACT_Expeditions_Route FOREIGN KEY (RouteKey) REFERENCES DIM_Route(RouteKey),
    CONSTRAINT FK_FACT_Expeditions_Status FOREIGN KEY (StatusKey) REFERENCES DIM_ExpeditionStatus(StatusKey),
    CONSTRAINT FK_FACT_Expeditions_HostCountry FOREIGN KEY (HostCountryKey) REFERENCES DIM_HostCountry(HostCountryKey),
    CONSTRAINT FK_FACT_Expeditions_LeaderNationality FOREIGN KEY (LeaderNationalityKey) REFERENCES DIM_Nationality(NationalityKey)
);

CREATE INDEX IX_FACT_Expeditions_ExpeditionID ON FACT_Expeditions(ExpeditionID);
CREATE INDEX IX_FACT_Expeditions_Year ON FACT_Expeditions(ExpeditionYear);
CREATE INDEX IX_FACT_Expeditions_Peak ON FACT_Expeditions(PeakKey);
CREATE INDEX IX_FACT_Expeditions_Date ON FACT_Expeditions(DateKey);
CREATE INDEX IX_FACT_Expeditions_Success ON FACT_Expeditions(IsSuccess);

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
