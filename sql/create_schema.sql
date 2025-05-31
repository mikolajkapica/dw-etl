IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'HimalayanExpeditionsDW')
BEGIN
    CREATE DATABASE HimalayanExpeditionsDW;
END
GO

USE HimalayanExpeditionsDW;
GO

CREATE TABLE DIM_Expedition (
    Id INT PRIMARY KEY,
    Host VARCHAR(100) NOT NULL,
    ROUTE1 VARCHAR(200) NOT NULL,
    ROUTE2 VARCHAR(200),
    ROUTE3 VARCHAR(200),
);

CREATE TABLE DIM_Peak(
    Id INT PRIMARY KEY,
    Name VARCHAR(200) NOT NULL,
    HeightMeters DECIMAL(8,2),
);

CREATE TABLE DIM_CountryIndicator (
    Id INT PRIMARY KEY,
    CountryCode VARCHAR(3) NOT NULL,
    CountryName VARCHAR(100) NOT NULL,
    GDPPerCapita DECIMAL(18,4),
    HumanCapitalIndex DECIMAL(18,4),
    InternetUsersPercentage DECIMAL(5,2),
    PhysiciansPer1000People DECIMAL(5,2),
    PoliticalStabilityIndex DECIMAL(5,2),
);

CREATE TABLE DIM_Date (
    Id INT PRIMARY KEY,
    Year INT NOT NULL,
    Season VARCHAR(20) NOT NULL,
);

CREATE TABLE FACT_MemberExpedition (
    Id INT PRIMARY KEY,
    ExpeditionId INT NOT NULL,
    PeakId INT NOT NULL,
    FirstName VARCHAR(100) NOT NULL,
    LastName VARCHAR(100) NOT NULL,
    BirthDate DATE NOT NULL,
    Gender VARCHAR(1) NOT NULL,
    CitizenshipCountry VARCHAR(50) NOT NULL,
    AgeGroup VARCHAR(20) NOT NULL
);