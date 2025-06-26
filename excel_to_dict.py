import openpyxl
from collections import defaultdict
import json
import datetime

def excel_to_dictionary(file_path):
    """
    Read multiple sheets from an Excel file and create dictionaries
    using headers as keys and cell values as values for each row.
    
    Args:
        file_path (str): Path to the Excel file
    
    Returns:
        dict: Dictionary with sheet names as keys and list of row dictionaries as values
    """
    
    # Open the Excel workbook
    workbook = openpyxl.load_workbook(file_path, data_only=True)
    
    # Dictionary to store all sheets data
    all_sheets_data = {}
    
    # Get all sheet names
    sheet_names = workbook.sheetnames
    print(f"Found {len(sheet_names)} sheets: {sheet_names}")
    
    # Iterate through each sheet
    for sheet_name in sheet_names:
        print(f"\nProcessing sheet: {sheet_name}")
        
        # Select the current sheet
        worksheet = workbook[sheet_name]
        
        # Get the headers from the first row
        headers = []
        first_row = worksheet[1]  # First row (1-indexed)
        
        for cell in first_row:
            if cell.value is not None:
                headers.append(str(cell.value).strip())
            else:
                headers.append(f"Column_{len(headers) + 1}")  # Default name for empty headers
        
        print(f"Headers found: {headers}")
        
        # List to store dictionaries for each row
        sheet_data = []
        
        # Iterate through rows starting from row 2 (skip header row)
        for row_num, row in enumerate(worksheet.iter_rows(min_row=2, values_only=True), start=2):
            # Skip completely empty rows
            if all(cell is None or str(cell).strip() == '' for cell in row):
                continue
            
            # Create dictionary for current row
            row_dict = {}
            
            # Iterate through each cell in the row
            for col_index, cell_value in enumerate(row):
                # Make sure we don't exceed the number of headers
                if col_index < len(headers):
                    header = headers[col_index]
                    # Handle None values and convert to appropriate type
                    if cell_value is None:
                        row_dict[header] = ""
                    else:
                        row_dict[header] = cell_value
            
            # Add the row dictionary to sheet data
            sheet_data.append(row_dict)
            print(f"Row {row_num}: {row_dict}")
        
        # Add sheet data to main dictionary
        all_sheets_data[sheet_name] = sheet_data
        print(f"Sheet '{sheet_name}' processed: {len(sheet_data)} rows")
    
    # Close the workbook
    workbook.close()
    
    return all_sheets_data

def print_sheet_summary(data_dict):
    """
    Print a summary of the data structure
    """
    print("\n" + "="*50)
    print("DATA SUMMARY")
    print("="*50)
    
    print(f"Processed Data: {data_dict}")

    for sheet_name, rows in data_dict.items():
        print(f"\nSheet: {sheet_name}")
        print(f"Number of rows: {len(rows)}")
        
        if rows:
            print(f"Columns: {list(rows[0].keys())}")
            print(f"Sample row: {rows[0]}")

# exports the dictionary to a Json file
def export_to_json(data_dict, output_file):
    """
    Export the data dictionary to a JSON file.
    
    Args:
        data_dict (dict): The data dictionary to export.
        output_file (str): The path to the output JSON file.
    """
    
    def json_serializer(obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, datetime.time):
            return obj.strftime('%H:%M:%S')
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    with open(output_file, 'w', encoding='utf-8') as json_file:
        json.dump(data_dict, json_file, indent=4, ensure_ascii=False, default=json_serializer)
    print(f"Data exported to {output_file}")


# Example usage
if __name__ == "__main__":
    # Replace with your Excel file path
    excel_file_path = "/Users/tushartari/tushar/study/courses/IraSkills/work/JCB_DATA_PUNE_CLEANED.xlsx"

    output_json_file = "/Users/tushartari/tushar/study/courses/IraSkills/work/JCB_DATA_PUNE_CLEANED.json"
    
    try:
        # Read the Excel file and convert to dictionary
        result = excel_to_dictionary(excel_file_path)
        
        # Print summary
        print_sheet_summary(result)
        # Export to JSON file
        export_to_json(result, output_json_file)
    except Exception as e:
        print(f"An error occurred: {e}")