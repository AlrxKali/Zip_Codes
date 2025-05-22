import pymongo
import csv
import os

# MongoDB connection details
MONGO_URI = ""
DATABASE_NAME = "zip_code_data" 

# File names
ES_FILE = "ES.txt"
US_FILE = "US.txt"
CA_FILE = "CA_full.txt"

def connect_to_db(uri, db_name):
    """Establishes a connection to MongoDB and returns the database object."""
    try:
        client = pymongo.MongoClient(uri)
        # Ping to confirm a successful connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        db = client[db_name]
        return db
    except pymongo.errors.ConfigurationError as e:
        print(f"MongoDB Configuration Error: {e}")
        print("Please ensure your MongoDB URI is correct and your IP is whitelisted if necessary.")
        exit(1)
    except pymongo.errors.ConnectionFailure as e:
        print(f"MongoDB Connection Error: {e}")
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during MongoDB connection: {e}")
        exit(1)

def process_file(db, filepath, collection_name, expected_columns_list, field_mapper_func):
    """
    Generic function to process a zip code file and insert data into MongoDB.
    """
    if not os.path.exists(filepath):
        print(f"Error: File not found - {filepath}. Skipping this file.")
        return

    collection = db[collection_name]
    collection.drop()
    print(f"Processing {filepath} into '{collection_name}' collection...")
    
    documents = []
    processed_count = 0
    skipped_count = 0

    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='\t')
        for i, row in enumerate(reader):
            if len(row) in expected_columns_list:
                try:
                    doc = field_mapper_func(row)
                    documents.append(doc)
                    processed_count += 1
                except ValueError as e:
                    print(f"Skipping row {i+1} in {filepath} due to data conversion error: {e} - Row: {row}")
                    skipped_count += 1
                except Exception as e:
                    print(f"Skipping row {i+1} in {filepath} due to an unexpected error: {e} - Row: {row}")
                    skipped_count += 1
            else:
                is_likely_header = any(s.isalpha() for s in row) and not any(s.isdigit() for s in row[:2])
                if i == 0 and is_likely_header:
                    print(f"Skipping likely header row in {filepath}: {row}")
                else:
                    print(f"Skipping malformed row {i+1} in {filepath} (expected {expected_columns_list} columns, got {len(row)}): {row}")
                skipped_count += 1
            
            if len(documents) >= 10000:
                if documents:
                    collection.insert_many(documents)
                    documents = [] # Reset batch

        if documents:  # Insert any remaining documents
            collection.insert_many(documents)

    print(f"Finished processing {filepath}.")
    print(f"Inserted {processed_count} documents into '{collection_name}'.")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} rows from {filepath}.")
    print("-" * 30)

# Mapper functions for each file type
def map_es_fields(row):
    return {
        "country_code": row[0],
        "postal_code": row[1],
        "place_name": row[2],
        "admin1_name": row[3],  # state_name
        "admin1_code": row[4],  # state_code
        "admin2_name": row[5],  # county_name
        "admin2_code": row[6],  # county_code
        "admin3_name": row[7] if row[7] else None,  # community_name
        "admin3_code": row[8] if row[8] else None,  # community_code
        "latitude": float(row[9]) if row[9] else None,
        "longitude": float(row[10]) if row[10] else None,
        "accuracy": int(row[11]) if row[11] else None
    }

def map_us_fields(row):
    return {
        "country_code": row[0],
        "postal_code": row[1],  # zip_code
        "place_name": row[2],
        "admin1_name": row[3],  # state_name
        "admin1_code": row[4],  # state_code
        "admin2_name": row[5],  # county_name
        "admin2_code": row[6],  # county_code
        # row[7] (column 8) and row[8] (column 9) are typically empty for US.txt
        "latitude": float(row[9]) if row[9] else None,
        "longitude": float(row[10]) if row[10] else None,
        "accuracy": int(row[11]) if row[11] else None
    }

def map_ca_fields_flexible(row):
    """Maps Canadian zip code data, handling rows with 10 or 12 columns."""
    doc = {
        "country_code": row[0],
        "postal_code": row[1],
        "place_name": row[2],
        "admin1_name": row[3] if len(row) > 3 and row[3] else None,  # province_name
        "admin1_code": row[4] if len(row) > 4 and row[4] else None,  # province_code
        "admin2_name": None,
        "admin2_code": None,
        "admin3_name": None,
        "admin3_code": None,
        "latitude": None,
        "longitude": None,
        "accuracy": None
    }

    num_cols = len(row)

    if num_cols == 10:
        doc["admin2_name"] = row[5] if row[5] else None
        doc["admin2_code"] = row[6] if row[6] else None
        try:
            doc["latitude"] = float(row[7]) if row[7] else None
            doc["longitude"] = float(row[8]) if row[8] else None
            doc["accuracy"] = int(row[9]) if row[9] else None
        except (IndexError, ValueError) as e:
            print(f"Warning: Could not parse lat/lon/acc for 10-column CA row: {row} - Error: {e}")
            pass
            
    elif num_cols == 12:
        doc["admin2_name"] = row[5] if row[5] else None
        doc["admin2_code"] = row[6] if row[6] else None
        doc["admin3_name"] = row[7] if row[7] else None
        doc["admin3_code"] = row[8] if row[8] else None
        try:
            doc["latitude"] = float(row[9]) if row[9] else None
            doc["longitude"] = float(row[10]) if row[10] else None
            doc["accuracy"] = int(row[11]) if row[11] else None
        except (IndexError, ValueError) as e:
            print(f"Warning: Could not parse lat/lon/acc for 12-column CA row: {row} - Error: {e}")
            pass
    else:
        raise ValueError(f"Unexpected number of columns ({num_cols}) for CA data mapper. Row: {row}")
    
    return doc

if __name__ == "__main__":
    print("Starting zip code import process...")
    db = connect_to_db(MONGO_URI, DATABASE_NAME)

    # Process Spain (ES.txt)
    # Expected columns: country_code, postal_code, place_name, state_name, state_code, county_name, county_code, community_name, community_code, latitude, longitude, accuracy
    process_file(db, ES_FILE, "es_zip_codes", [12], map_es_fields)

    # Process USA (US.txt)
    # Expected columns: country_code, zip_code, place_name, state_name, state_code, county_name, county_code, <empty>, <empty>, latitude, longitude, accuracy
    process_file(db, US_FILE, "us_zip_codes", [12], map_us_fields)

    # Process Canada (CA_full.txt)
    # Expected columns can be 10 or 12
    process_file(db, CA_FILE, "ca_zip_codes", [10, 12], map_ca_fields_flexible)

    print("All files processed. Data import complete.")
