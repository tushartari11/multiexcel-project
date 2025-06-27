
def print_sheet_summary(data_dict):
    """
    Print a summary of the data structure
    """
    print("\n" + "="*50)
    print("DATA SUMMARY")
    print("="*50)
    

    for sheet_name, rows in data_dict.items():
        print(f"\nSheet: {sheet_name}")
        print(f"Number of rows: {len(rows)}")
        
        if rows:
            print(f"Columns: {list(rows[0].keys())}")
            print(f"Sample row: {rows[0]}")