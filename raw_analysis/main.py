import pandas as pd
from dbfread import DBF
import io
import os

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 2000)
pd.set_option("display.max_colwidth", 100)
pd.set_option("display.max_rows", None)


def dbf_to_df(filename):
    return pd.DataFrame(iter(DBF(filename, load=True)))


def remove_newlines(df):
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.replace(r"\r\n", " ", regex=True)
        df[col] = df[col].str.replace(r"\n\r", " ", regex=True)
        df[col] = df[col].str.replace(r"\r", " ", regex=True)
        df[col] = df[col].str.replace(r"\n", " ", regex=True)
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)
        df[col] = df[col].str.strip()
    return df


def dbf_to_csv(dbf_filename, csv_filename=None):
    """Convert a DBF file to CSV format, cleaning problematic characters"""
    if csv_filename is None:
        csv_filename = dbf_filename.replace(".DBF", ".csv").replace(".dbf", ".csv")

    df = dbf_to_df(dbf_filename)
    df = remove_newlines(df)
    df.to_csv(
        csv_filename, index=False, encoding="utf-8", quoting=1, lineterminator="\n"
    )
    print(f"Converted {dbf_filename} to {csv_filename}")
    return csv_filename


def stats(filename, output_file="dbf_stats_output.txt"):
    # Allow CSV as well as DBF
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(filename)
    else:
        df = dbf_to_df(filename)
    output_lines = []

    output_lines.append(f"{filename}: {df.shape}, {df.columns.tolist()}")

    # Print all columns and all rows for the sample head
    output_lines.append(str(df.head()))

    output_lines.append("\nDataFrame Info:")
    buf = io.StringIO()
    df.info(buf=buf)
    output_lines.append(buf.getvalue())

    # Print all columns and all rows for missing values per column
    output_lines.append("\nMissing values per column:")
    output_lines.append(str(df.isnull().sum()))

    # Print all columns in the summary statistics
    output_lines.append("\nSummary statistics (all columns):")
    output_lines.append(str(df.describe(include="all")))

    duplicates = df.duplicated().sum()
    output_lines.append(f"\nNumber of duplicate rows: {duplicates}")

    # --- DODATKOWE: Szczegółowy zakres wartości i ocena jakości danych ---
    output_lines.append("\nZakres wartości i Uwagi – ocena jakości danych:")
    for col in df.columns:
        col_info = f"\nKolumna: {col}"
        ser = df[col]
        n_unique = ser.nunique(dropna=False)
        n_missing = ser.isnull().sum()
        dtype = ser.dtype
        col_info += f"\n  Typ: {dtype}, Unikalnych: {n_unique}, Braków: {n_missing}"
        # Only run numeric stats and outlier detection for non-bool numeric columns
        if pd.api.types.is_numeric_dtype(ser) and not pd.api.types.is_bool_dtype(ser):
            desc = ser.describe(percentiles=[0.01, 0.05, 0.95, 0.99])
            col_info += f"\n  Statystyki: {desc.to_dict()}"
            # Outlier detection (simple: 1.5*IQR)
            if ser.dropna().size > 0:
                q1 = ser.quantile(0.25)
                q3 = ser.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                n_outliers = ((ser < lower) | (ser > upper)).sum()
                col_info += f"\n  Outliers (1.5*IQR): {n_outliers}"
                if n_outliers > 0:
                    col_info += " [UWAGA: wykryto wartości odstające]"
        elif pd.api.types.is_string_dtype(ser) or pd.api.types.is_object_dtype(ser):
            top_vals = ser.value_counts(dropna=False).head(10)
            col_info += f"\n  Top 10 wartości: {top_vals.to_dict()}"
            lengths = ser.dropna().astype(str).map(len)
            if not lengths.empty:
                col_info += f"\n  Długość tekstu (min/średnia/max): {lengths.min()}/{lengths.mean():.1f}/{lengths.max()}"
            if (ser == "").sum() > 0:
                col_info += f"\n  [UWAGA: występują puste ciągi znaków]"
        elif pd.api.types.is_bool_dtype(ser):
            counts = ser.value_counts(dropna=False).to_dict()
            col_info += f"\n  Rozkład wartości: {counts}"
        # Prosta ocena jakości
        if n_missing > 0:
            if n_missing / len(df) > 0.2:
                col_info += "\n  [UWAGA: dużo brakujących wartości]"
            else:
                col_info += "\n  [OK: akceptowalna liczba braków]"
        else:
            col_info += "\n  [OK: brak braków]"
        output_lines.append(col_info)

    # Print to console
    for line in output_lines:
        print(line)

    # Write to file
    with open(output_file, "w", encoding="utf-8") as f:
        for line in output_lines:
            f.write(str(line) + "\n")


if __name__ == "__main__":
    # change dbf to csv
    dbf_to_csv("data/exped.dbf", "../data/expeditions.csv")
    dbf_to_csv("data/members.dbf", "../data/members.csv")
    dbf_to_csv("data/peaks.dbf", "../data/peaks.csv")
