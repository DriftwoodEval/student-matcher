import csv
import logging
import os

import chardet
import pandas as pd


def standardize_dob(df, dob_field):
    """Standardize the date of birth field to 'YYYY-MM-DD' format."""
    df["dob"] = pd.to_datetime(df[dob_field], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def detect_encoding(filepath):
    with open(filepath, "rb") as f:
        rawdata = f.read()
        result = chardet.detect(rawdata)
        return result["encoding"]


def detect_delimiter(filepath, encoding, sample_size=1024, num_lines_fallback=10):
    try:
        with open(filepath, "r", encoding=encoding) as f:
            sample = f.read(sample_size)
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            return delimiter
    except Exception as e:
        logging.warning(f"Sniffer failed: {e}. Falling back to frequency analysis.")
        return analyze_most_common_delimiter(filepath, num_lines_fallback)


def analyze_most_common_delimiter(filepath, num_lines=10):
    delimiter_counts = {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                for char in line:
                    if char not in delimiter_counts:
                        delimiter_counts[char] = 0
                    delimiter_counts[char] += 1

        if not delimiter_counts:
            return None

        sorted_delimiters = sorted(
            delimiter_counts.items(), key=lambda item: item[1], reverse=True
        )

        potential_delimiters = [
            item[0]
            for item in sorted_delimiters
            if item[0] in [",", "\t", ";"] and item[1] > 0
        ]

        if potential_delimiters:
            return potential_delimiters[0]
        else:
            return None

    except Exception as e:
        logging.error(f"Delimiter frequency analysis failed: {e}")
        return None


def find_header_start(filepath, encoding):
    """
    Finds the starting line of the header based on the presence of ANY of the keywords.

    Args:
        filepath (str): The path to the file.
        header_keywords (list): A list of strings that, if ANY are present, indicate a header line.

    Returns:
        int: The line number (0-based) where the header starts, or None if not found.
    """
    try:
        with open(filepath, "r", encoding=encoding) as f:
            for i, line in enumerate(f):
                line_lower = line.lower()

                # Requirement 1: Either "name" or ("firstname" and "lastname")
                has_name = "name" in line_lower
                has_firstname = "firstname" in line_lower
                has_lastname = "lastname" in line_lower
                name_condition = has_name or (has_firstname and has_lastname)

                # Requirement 2: One of "dob", "date of birth", or "birthdate"
                has_dob = "dob" in line_lower
                has_date_of_birth = "date of birth" in line_lower
                has_birthdate = "birthdate" in line_lower
                dob_condition = has_dob or has_date_of_birth or has_birthdate

                if name_condition and dob_condition:
                    return i
        return None  # Header not found
    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
        return None
    except Exception as e:
        logging.error(f"Error finding header: {e}")
        return None


def find_dob_field(df):
    for field in ["birthdate", "date of birth", "dob"]:
        if field in df.columns:
            return field
    return None


def read_csv_with_detections(filepath):
    encoding = detect_encoding(filepath)
    header_start = find_header_start(filepath, encoding)
    delimiter = detect_delimiter(filepath, encoding)

    if header_start is not None:
        try:
            df = pd.read_csv(
                filepath, encoding=encoding, delimiter=delimiter, skiprows=header_start
            ).rename(columns=str.lower)
            return df
        except Exception as e:
            logging.error(f"Error reading CSV: {e}")
            return None
    else:
        logging.error(f"Header line not found in {filepath} based on keywords.")
        return None


def find_matching_students(file1, file2):
    df1 = read_csv_with_detections(file1)
    df2 = read_csv_with_detections(file2)

    if df1 is None or df2 is None:
        return None

    dob_field1 = find_dob_field(df1)
    dob_field2 = find_dob_field(df2)

    df1 = standardize_dob(df1, dob_field1)
    df2 = standardize_dob(df2, dob_field2)

    for df in [df1, df2]:
        if "name" in df.columns:
            df["firstname"] = df["name"].apply(
                lambda x: x.split()[0] if isinstance(x, str) and x.split() else None
            )
            df["lastname"] = df["name"].apply(
                lambda x: x.split()[-1] if isinstance(x, str) and x.split() else None
            )
            df.drop(columns=["name"], inplace=True)

    return df1.merge(df2, on=["firstname", "lastname", "dob"], how="inner")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s - %(message)s",
    )
    current_dir = os.getcwd()
    csv_files = [f for f in os.listdir(current_dir) if f.endswith(".csv")]

    if len(csv_files) != 2:
        logging.error("Please provide exactly two CSV files in the current directory.")
    else:
        file1, file2 = csv_files
        matching_students = find_matching_students(file1, file2)

        if matching_students is not None and not matching_students.empty:
            print("Matching students found:")
            print(matching_students[["firstname", "lastname", "dob"]])
            matching_students[["firstname", "lastname", "dob"]].to_csv(
                "matching_students.csv", index=False
            )
        else:
            logging.warning("No matching students found.")
