import csv
import logging
from tkinter.filedialog import askopenfilename

import chardet
import pandas as pd


def standardize_dob(df, dob_field):
    """Standardize the date of birth field to 'YYYY-MM-DD' format."""
    df["helper_dob"] = pd.to_datetime(df[dob_field], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
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


def pick_file(title):
    filepath = askopenfilename(title=title, filetypes=[("CSV files", "*.csv")])
    return filepath


def find_changed_students(filter_sped, all_sped):
    filter_df = read_csv_with_detections(filter_sped)
    all_df = read_csv_with_detections(all_sped)

    if filter_df is None or all_df is None:
        return None

    dob_field_filtered = find_dob_field(filter_df)
    dob_field_all = find_dob_field(all_df)

    filter_df = standardize_dob(filter_df, dob_field_filtered)
    all_df = standardize_dob(all_df, dob_field_all)

    for df in [filter_df, all_df]:
        if "name" in df.columns:
            df["helper_firstname"] = df["name"].apply(
                lambda x: x.split()[0] if isinstance(x, str) and x.split() else None
            )
            df["helper_lastname"] = df["name"].apply(
                lambda x: handle_suffix_lastname(x) if isinstance(x, str) else None
            )
            df.drop(columns=["name"], inplace=True)
        if "lastname" in df.columns:
            df["helper_lastname"] = df["lastname"].apply(
                lambda x: handle_suffix_lastname(x) if isinstance(x, str) else None
            )
        if "firstname" in df.columns:
            df["helper_firstname"] = df["firstname"].apply(
                lambda x: x.split()[0] if isinstance(x, str) and x.split() else None
            )

    for index, row in filter_df.iterrows():
        if all_df[
            (all_df["helper_lastname"] == row["helper_lastname"])
            & (all_df["helper_firstname"] == row["helper_firstname"])
            & (all_df["helper_dob"] == row["helper_dob"])
        ].any(axis=None):
            filter_df.loc[index, "isspecialed"] = True
        else:
            filter_df.loc[index, "isspecialed"] = False

    filter_df.drop(
        columns=["helper_lastname", "helper_firstname", "helper_dob"], inplace=True
    )

    return filter_df


def handle_suffix_lastname(name_str):
    parts = name_str.split()
    if not parts:
        return None

    last_part = parts[-1].lower()
    roman_numerals = ["ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x"]
    if last_part in ["jr", "sr"] + roman_numerals and len(parts) > 1:
        return " ".join(parts[-2:])  # Combine the last two parts
    else:
        return parts[-1]  # Return the last part as is


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s - %(message)s",
    )
    filter_sped = pick_file("Select the CSV file that needs to be filtered")
    all_sped = pick_file("Select the CSV file that contains only SpEd kids")

    changed_students = find_changed_students(filter_sped, all_sped)

    if changed_students is not None and not changed_students.empty:
        print("Writing new file, updated_students.csv")
        changed_students.to_csv("updated_students.csv", index=False)
    else:
        logging.warning("No changed students found.")
