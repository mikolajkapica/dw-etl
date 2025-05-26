# Himalayan Expeditions ETL Pipeline

## Przegląd
Pipeline ETL dla danych ekspedycji himalajskich implementuje proces Extract-Transform-Load (ETL) wykorzystujący framework Dagster. System przetwarza dane z plików CSV oraz API World Bank, transformuje je i ładuje do hurtowni danych opartej na architekturze gwiazdy (star schema).

## Dokumentacja zadań ETL

### 📁 extraction.py - Ekstrakcja danych źródłowych

#### `extract_expeditions_data`
**Cel:** Ładuje dane ekspedycji z pliku CSV i wykonuje podstawową walidację struktury danych.

**Proces:**
- Odczytuje plik `expeditions.csv` z katalogu danych
- Sprawdza obecność wymaganych kolumn: `EXPID`, `PEAKID`, `YEAR`, `SEASON`
- Raportuje metryki jakości danych (duplikaty, brakujące wartości)
- Zwraca surowe dane ekspedycji

**Dane wejściowe:** Plik CSV z danymi ekspedycji
**Dane wyjściowe:** DataFrame z surowymi danymi ekspedycji

#### `extract_members_data`
**Cel:** Ładuje dane członków ekspedycji z pliku CSV z walidacją kompletności rekordów.

**Proces:**
- Odczytuje plik `members.csv`
- Sprawdza wymagane kolumny: `EXPID`, `FNAME`, `LNAME`
- Analizuje jakość danych (brakujące imiona, liczba unikalnych ekspedycji)
- Zwraca surowe dane członków

**Dane wejściowe:** Plik CSV z danymi członków ekspedycji
**Dane wyjściowe:** DataFrame z surowymi danymi członków

#### `extract_peaks_data`
**Cel:** Ładuje dane szczytów górskich z pliku CSV z walidacją identyfikatorów.

**Proces:**
- Odczytuje plik `peaks.csv`
- Waliduje strukturę danych szczytów
- Sprawdza poprawność identyfikatorów szczytów

**Dane wejściowe:** Plik CSV z danymi szczytów
**Dane wyjściowe:** DataFrame z surowymi danymi szczytów

### 📁 world_bank.py - Integracja danych zewnętrznych

#### `extract_world_bank_data`
**Cel:** Pobiera wskaźniki rozwoju krajów z API World Bank dla wzbogacenia analizy ekspedycji.

**Proces:**
- Pobiera dane z World Bank API dla wskaźników:
  - `NY.GDP.PCAP.CD` - PKB per capita
  - `HD.HCI.OVRL` - Indeks Kapitału Ludzkiego
  - `IT.NET.USER.ZS` - Procent użytkowników Internetu
  - `SH.MED.PHYS.ZS` - Liczba lekarzy na 1000 mieszkańców
  - `PV.EST` - Indeks Stabilności Politycznej
- Obsługuje paginację i retry logic
- Normalizuje format danych z API

**Dane wejściowe:** API World Bank
**Dane wyjściowe:** DataFrame z wskaźnikami krajów

#### `clean_world_bank_data`
**Cel:** Czyści i waliduje dane World Bank, stosuje reguły biznesowe dla wskaźników.

**Proces:**
- Waliduje zakresy wartości wskaźników
- Oblicza wskaźnik jakości danych na podstawie wieku danych
- Usuwa rekordy spoza rozsądnych zakresów
- Normalizuje kody krajów

#### `create_dim_country_indicators`
**Cel:** Tworzy wymiar wskaźników krajów łączący dane ekspedycji z danymi makroekonomicznymi.

**Proces:**
- Agreguje wskaźniki World Bank na poziomie kraj-rok
- Tworzy tabele wymiaru z surogat keys
- Łączy z krajami występującymi w danych ekspedycji

### 📁 cleaning.py - Oczyszczanie i standaryzacja danych

#### `clean_expeditions_data`
**Cel:** Oczyszcza dane ekspedycji z deduplikacją i standaryzacją formatów.

**Proces:**
- **Deduplikacja:** Usuwa duplikaty `EXPID` wybierając rekord z najwyższą kompletnością danych
- **Czyszczenie dat:** Konwertuje pola dat (`SMTDATE`, `BCDATE`, `STDATE`) do formatu datetime
- **Standaryzacja sezonów:** Mapuje kody sezonów na standardowe wartości
- **Konwersja numeryczna:** Przekształca pola numeryczne (`YEAR`, `HEIGHTM`, `TOTMEMBERS`, etc.)
- **Parsowanie liderów:** Ekstraktuje listę liderów z pola tekstowego `LEADERS`
- **Określenie sukcesu:** Tworzy pole `IS_SUCCESS` na podstawie dostępnych wskaźników

**Dane wejściowe:** Surowe dane ekspedycji
**Dane wyjściowe:** Oczyszczone dane ekspedycji gotowe do transformacji

#### `clean_members_data`
**Cel:** Oczyszcza dane członków z normalizacją imion i tworzeniem identyfikatorów.

**Proces:**
- **Czyszczenie imion:** Standaryzuje format imion i nazwisk (proper case)
- **Tworzenie pełnych imion:** Łączy `FNAME` i `LNAME` w `FULL_NAME`
- **Normalizacja obywatelstwa:** Standaryzuje kody krajów w `CITIZEN`
- **Konwersja płci:** Mapuje wartości płci na standardowe kody
- **Generowanie ID:** Tworzy unikalne `MEMBER_ID` dla każdego członka
- **Walidacja wieku:** Sprawdza poprawność pól wiekowych

**Dane wejściowe:** Surowe dane członków
**Dane wyjściowe:** Oczyszczone dane członków z dodatkowymi polami

#### `clean_peaks_data`
**Cel:** Oczyszcza dane szczytów z walidacją wysokości i lokalizacji.

**Proces:**
- Waliduje wysokości szczytów
- Normalizuje nazwy szczytów
- Sprawdza poprawność koordinat geograficznych
- Standaryzuje kody krajów gospodarzy

**Dane wejściowe:** Surowe dane szczytów
**Dane wyjściowe:** Oczyszczone dane szczytów

### 📁 dimensions.py - Tworzenie tabel wymiarów

#### `create_dim_date`
**Cel:** Tworzy wymiar czasu pokrywający zakres dat ekspedycji z atrybutami temporalnymi.

**Proces:**
- Określa zakres dat na podstawie lat ekspedycji (1900 - obecny rok + 5)
- Generuje ciągły kalendarz dziennie
- Dodaje atrybuty: rok, kwartał, miesiąc, dzień roku, dzień tygodnia
- Tworzy pole `Season` na podstawie miesiąca
- Generuje klucze `DateKey` i `DATE_KEY` dla joinów

**Dane wejściowe:** Oczyszczone dane ekspedycji (dla zakresu dat)
**Dane wyjściowe:** Kompletny wymiar czasu

#### `create_dim_nationality`
**Cel:** Tworzy wymiar narodowości ekstraktując unikalne kraje z ekspedycji i członków.

**Proces:**
- Ekstraktuje narodowości z pól: `NATION`, `HOST`, `CITIZEN` 
- Deduplikuje i standaryzuje kody krajów
- Mapuje kody krajów na standardowe nazwy (ISO3)
- Dodaje informacje o regionach geograficznych
- Tworzy klucze surogat dla każdego kraju

**Dane wejściowe:** Oczyszczone dane ekspedycji i członków
**Dane wyjściowe:** Wymiar narodowości z regionami

#### `create_dim_peak`
**Cel:** Tworzy wymiar szczytów łączący dane szczytów z informacjami o ekspedycjach.

**Proces:**
- Łączy dane z `peaks.csv` z danymi ekspedycji
- Agreguje statystyki ekspedycji na szczyt
- Dodaje informacje o statusie wspinaczkowym
- Normalizuje nazwy szczytów i współrzędne

**Dane wejściowe:** Oczyszczone dane szczytów i ekspedycji
**Dane wyjściowe:** Wymiar szczytów z dodatkowymi atrybutami

#### `create_dim_route`
**Cel:** Tworzy wymiar tras wspinaczkowych z kombinacji pól ROUTE1-ROUTE4.

**Proces:**
- Kombinuje pola tras (`ROUTE1`, `ROUTE2`, `ROUTE3`, `ROUTE4`)
- Kategoryzuje typ trasy (standardowa, wariantowa, kombinowana)
- Szacuje poziom trudności na podstawie nazw tras
- Deduplikuje unikalne kombinacje tras

**Dane wejściowe:** Oczyszczone dane ekspedycji
**Dane wyjściowe:** Wymiar tras z typologią

#### `create_dim_expedition_status`
**Cel:** Tworzy wymiar statusów ekspedycji na podstawie przyczyn zakończenia.

**Proces:**
- Ekstraktuje unikalne wartości z `TERMREASON`
- Kategoryzuje statusy (sukces, niepowodzenie, przerwanie)
- Mapuje przyczyny na opisowe kategorie
- Dodaje flagi sukcesu dla analiz

**Dane wejściowe:** Oczyszczone dane ekspedycji
**Dane wyjściowe:** Wymiar statusów ekspedycji

#### `create_dim_host_country`
**Cel:** Tworzy wymiar krajów gospodarzy ekspedycji.

**Proces:**
- Ekstraktuje kraje z pola `HOST`
- Standaryzuje nazwy i kody krajów
- Dodaje informacje geograficzne (region, podregion)
- Generuje klucze surogat

**Dane wejściowe:** Oczyszczone dane ekspedycji
**Dane wyjściowe:** Wymiar krajów gospodarzy

#### `create_dim_member`
**Cel:** Tworzy wymiar członków ekspedycji z atrybutami demograficznymi.

**Proces:**
- Agreguje dane członków na poziomie osoby
- Tworzy grupy wiekowe na podstawie wieku
- Dodaje informacje o obywatelstwie
- Generuje unikalne klucze członków

**Dane wejściowe:** Oczyszczone dane członków
**Dane wyjściowe:** Wymiar członków

#### `load_dimension_table`
**Cel:** Uniwersalna funkcja ładowania wymiarów do bazy danych z logiką upsert.

**Proces:**
- Implementuje strategię Slowly Changing Dimension (SCD Type 1)
- Porównuje dane z istniejącymi rekordami w bazie
- Aktualizuje zmienione rekordy, wstawia nowe
- Obsługuje bulk operations dla wydajności

### 📁 facts.py - Tworzenie tabeli faktów

#### `prepare_fact_expeditions`
**Cel:** Przygotowuje tabelę faktów łącząc dane ekspedycji z członkami w model member-centric.

**Proces:**
- **Join strategii:** Wykonuje RIGHT JOIN między ekspedycjami a wymiarem członków
- **Denormalizacja:** Łączy dane ekspedycji z danymi członków na poziomie EXPID
- **Mapowanie wymiarów:** Łączy klucze surogat z wszystkich wymiarów
- **Konstrukcja faktów:** Tworzy rekord dla każdego członka każdej ekspedycji z:
  - Kluczami wymiarów (PeakKey, MemberKey, DateKey, etc.)
  - Atrybutami ekspedycji (rok, sezon, liczba członków, długość)
  - Metrykami ekspedycji (sukcesy, śmierci, kontuzje)
  - Atrybutami członka (wiek, rola, użycie tlenu, sukces osobisty)

**Dane wejściowe:** Wszystkie oczyszczone dane + wszystkie wymiary
**Dane wyjściowe:** Tabela faktów w formacie member-centric

#### `load_fact_expeditions`
**Cel:** Ładuje przygotowaną tabelę faktów do bazy danych z walidacją integralności.

**Proces:**
- Waliduje integralność referentialną z wymiarami
- Wykonuje walidację biznesową (zakresy dat, wartości logiczne)
- Ładuje dane w batches dla wydajności
- Tworzy indeksy i statystyki

**Dane wejściowe:** Przygotowana tabela faktów + wyniki ładowania wymiarów
**Dane wyjściowe:** Status ładowania z metrykami

## Architektura hurtowni danych

### Główne tabele:

**FACT_Expeditions** - Tabela faktów (member-centric):
- Jeden rekord na członka ekspedycji
- Zawiera klucze do wszystkich wymiarów
- Metryki: sukcesy, śmierci, kontuzje, użycie tlenu
- Atrybuty ekspedycji i członka

**Wymiary:**
- **DIM_Date** - Kalendarz z atrybutami czasu
- **DIM_Peak** - Szczyty z lokalizacją i statusem
- **DIM_Member** - Członkowie z demografią  
- **DIM_Nationality** - Kraje z regionami
- **DIM_Route** - Trasy z typologią trudności
- **DIM_ExpeditionStatus** - Statusy ekspedycji
- **DIM_HostCountry** - Kraje gospodarze
- **DIM_CountryIndicators** - Wskaźniki makroekonomiczne

## Uruchomienie pipeline

### Wymagania
- Python 3.8+
- Dagster
- pandas, sqlalchemy
- SQL Server

### Uruchomienie
```bash
# Instalacja zależności
pip install -r requirements.txt

# Uruchomienie Dagster UI
dagster dev

# Wykonanie job'a
dagster job execute --job himalayan_etl_full_load
```

### Konfiguracja
Pipeline wymaga konfiguracji:
- Połączenia do bazy SQL Server
- Ścieżek do plików CSV
- Parametrów API World Bank

## Monitorowanie i logowanie

Pipeline zawiera:
- **Retry policies** - Automatyczne ponawianie operacji
- **Data quality checks** - Walidacja na każdym etapie  
- **Szczegółowe logowanie** - Metryki i status każdej operacji
- **Error handling** - Graceful degradation przy błędach

## Rozszerzenia

Pipeline może być rozszerzony o:
- **Incremental loading** - Ładowanie przyrostowe (obecnie implementuje full refresh)
- **Data lineage tracking** - Śledzenie pochodzenia danych
- **Advanced SCD** - Slowly Changing Dimensions Type 2
- **Real-time streaming** - Przetwarzanie w czasie rzeczywistym

### Aktualny stan implementacji

**Już zaimplementowane:**
- Basic upsert logic w tabelach wymiarów
- CreatedDate/ModifiedDate w schemacie bazy
- Retry policies i error handling
- Data quality checks

**Do implementacji:**
- Timestamp-based incremental extraction
- Delta loading logic
- Watermark tracking
- Change data capture (CDC)