
# Alternative function if you want to process just one specific sheet
def single_sheet_to_dictionary(file_path, sheet_name=None):
    """
    Read a single sheet from Excel file and return list of dictionaries
    
    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet (if None, uses first sheet)
    
    Returns:
        list: List of dictionaries, one for each row
    """
    workbook = openpyxl.load_workbook(file_path)
    
    # Use first sheet if no sheet name specified
    if sheet_name is None:
        worksheet = workbook.active
    else:
        worksheet = workbook[sheet_name]
    
    # Get headers
    headers = [str(cell.value).strip() if cell.value else f"Column_{i+1}" 
               for i, cell in enumerate(worksheet[1])]
    
    # Create list of dictionaries
    data = []
    for row in worksheet.iter_rows(min_row=2, values_only=True):
        if any(cell is not None and str(cell).strip() != '' for cell in row):
            row_dict = {}
            for i, cell_value in enumerate(row):
                if i < len(headers):
                    row_dict[headers[i]] = cell_value if cell_value is not None else ""
            data.append(row_dict)
    
    workbook.close()
    return data