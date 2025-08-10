"""
This script loops through the directory of all the static zip files of Indego Bike data, and unzips
them. Each CSV file is then loaded as a Pandas DataFrame and saved as a pickle file.

NOTE: Ideally, I would make this more dynamic with using webscraping or an API, but neither seemed
to be possible with the current Indego Bike data source. Also, while there are links to data from
2015, I could not download them, so data starts from 2016.
"""

import io
import os
import pandas as pd
import zipfile


def aggregate_zipped_csvs(directory_path: str) -> pd.DataFrame:
    """
    Loops through a directory, unzips CSV files, and aggregates them into a single DataFrame.

    Parameters:
    -----------
    directory_path: str
        The path to the directory containing the zip files.

    Returns:
    --------
        pd.DataFrame: A single DataFrame containing all the data, or None if an error occurs.
    """
    # Create an empty list to store individual DataFrames
    all_dataframes = []

    # Get a list of all files in the specified directory
    files_in_directory = os.listdir(directory_path)

    # Print statement to validate the directory contents
    print(f"Found {len(files_in_directory)} items in the directory. Processing...")

    # Loop through each file in the directory
    for filename in files_in_directory:
        # Check if the file is a zip file
        if filename.endswith(".zip"):
            file_path = os.path.join(directory_path, filename)
            print(f"Processing zip file: {filename}")

            try:
                # Open the zip file
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    # Get the single CSV file inside the zip
                    csv_filename = zip_ref.namelist()[0]

                    # Read the CSV file into a file-like object in memory
                    with zip_ref.open(csv_filename) as csv_file:
                        # Load the CSV data directly into a pandas DataFrame
                        df = pd.read_csv(io.TextIOWrapper(csv_file, "utf-8"))

                        # Append the DataFrame to our list
                        all_dataframes.append(df)
                        print(f"Successfully loaded {csv_filename} from {filename}.")

            except Exception as e:
                print(f"An error occurred while processing {filename}: {e}")
                # Move on to the next file if an error occurs
                continue

    # Check if any dataframes were loaded
    if not all_dataframes:
        print("No CSV files were found or processed. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Concatenate all the DataFrames in the list into one
    print("All individual DataFrames have been loaded. Concatenating them now...")
    final_dataframe = pd.concat(all_dataframes, ignore_index=True)

    # Final progress indicator
    print("Concatenation complete!")
    print(f"The final combined DataFrame has a shape of: {final_dataframe.shape}")

    return final_dataframe


if __name__ == "__main__":
    my_directory_path = "./raw_data"
    df_bike = aggregate_zipped_csvs(my_directory_path)
    df_bike.to_pickle("indego_bike_data.pkl")
