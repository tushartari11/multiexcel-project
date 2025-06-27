from collections import defaultdict
import json
import datetime
import logging

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
    logging.info("Data exported to path : %s ", output_file)