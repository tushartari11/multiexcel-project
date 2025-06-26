import pandas as pd
import os


def import_excel(file_path):
    """
    Imports an Excel file and returns a DataFrame.
    
    Args:
        file_path (str): The path to the Excel file.
        
    Returns:
        pd.DataFrame: The DataFrame containing the data from the Excel file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    try:
        xls = pd.ExcelFile(file_path)
        num_sheets = len(xls.sheet_names)
        if num_sheets == 0:
            raise ValueError("The Excel file does not contain any sheets.")
        # Read the first sheet by default
        for sheet_index in range(3,num_sheets):
            print(f"sheet name : {xls.sheet_names[sheet_index]}")
            print(f"Processing sheet {sheet_index + 1} of {num_sheets}: {xls.sheet_names[sheet_index]}")
            df = pd.read_excel(xls, sheet_name=sheet_index)
            if not df.empty:
                df.to_csv(file_path.replace('.xlsx', f'_sheet{sheet_index + 1}.csv'), index=False)
                return df
        raise ValueError("All sheets in the Excel file are empty.")
    except ValueError as ve:
        raise ValueError(f"Error processing the Excel file: {ve}")
    except Exception as e:
        raise Exception(f"An error occurred while importing the Excel file: {e}")


def main():
    file_path = input("Enter the path to the Excel file: ")
    try:
        df = import_excel(file_path)
        print("Data imported successfully:")
        print(df.head())
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__=="__main__":
    main()
