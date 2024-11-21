import os

import pandas as pd


def standardize_dob(df, dob_field):
    # Make sure the date of birth is in the format YYYY-MM-DD, and
    # has the header "dob".
    df["dob"] = pd.to_datetime(df[dob_field], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def find_matching_students(file1, file2):
    # Read the two CSV files and rename columns to lowercase.
    df1 = pd.read_csv(file1).rename(columns=str.lower)
    df2 = pd.read_csv(file2).rename(columns=str.lower)

    # Determine which column to use for the date of birth
    dob_field1 = "birthdate" if "birthdate" in df1.columns else "dob"
    dob_field2 = "birthdate" if "birthdate" in df2.columns else "dob"

    df1 = standardize_dob(df1, dob_field1)
    df2 = standardize_dob(df2, dob_field2)

    return df1.merge(df2, on=["firstname", "lastname", "dob"], how="inner")


if __name__ == "__main__":
    current_dir = os.getcwd()
    csv_files = [f for f in os.listdir(current_dir) if f.endswith(".csv")]

    if len(csv_files) != 2:
        print("Please provide exactly two CSV files in the current directory.")
    else:
        file1, file2 = csv_files
        matching_students = find_matching_students(file1, file2)

        if not matching_students.empty:
            print("Matching students found:")
            print(matching_students[["firstname", "lastname", "dob"]])
            matching_students[["firstname", "lastname", "dob"]].to_csv(
                "matching_students.csv", index=False
            )
        else:
            print("No matching students found.")
