import os
import requests
import time

# --- CONFIGURATION ---
BASEROW_TOKEN = os.getenv('BASEROW_TOKEN')
SLEEP_SECONDS = os.getenv('SLEEP_SECONDS')
DATABASE_ID = os.getenv('DATABASE_ID')
PRIMARY_TABLE_ID = os.getenv('PRIMARY_TABLE_ID')
MULTI_SELECT_COLUMN_NAME = os.getenv('MULTI_SELECT_COLUMN_NAME', 'MultiSelect')
PRIMARY_ID_TRACKER = os.getenv('PRIMARY_ID_TRACKER', 'Primary_Row_ID')

BASEROW_URL = 'https://api.baserow.io' # Change if self-hosting

# DATABASE_ID = 100 # Your Database ID (where tables are stored)
# PRIMARY_TABLE_ID = 1001 # Your Primary Table ID
# MULTI_SELECT_COLUMN_NAME = 'MultiSelect' # The exact name of your Multiple Select column
# PRIMARY_ID_TRACKER = 'Primary_Row_ID' # The name of the tracking column in secondary tables

HEADERS = {
    'Authorization': f'Token {BASEROW_TOKEN}',
    'Content-Type': 'application/json'
}

def make_request(method, url, **kwargs):
    """Helper function to make API requests and handle errors."""
    kwargs['headers'] = HEADERS
    response = requests.request(method, url, **kwargs)
    if not response.ok:
        print(f"API Error: {response.status_code} on {method} {url}")
        print(response.text)
        response.raise_for_status()
    return response.json() if response.content else None

def get_all_rows(table_id):
    """Fetches all rows from a given Baserow table using user-friendly field names."""
    rows = []
    url = f"{BASEROW_URL}/api/database/rows/table/{table_id}/?user_field_names=true"
    
    while url:
        data = make_request('GET', url)
        if data:
            rows.extend(data['results'])
            url = data.get('next')
        else:
            break
        
    return rows

def get_tables():
    """Fetches all tables in the database."""
    url = f"{BASEROW_URL}/api/database/tables/database/{DATABASE_ID}/"
    return make_request('GET', url)

def get_fields(table_id):
    """Fetches the schema/fields of a table."""
    url = f"{BASEROW_URL}/api/database/fields/table/{table_id}/"
    return make_request('GET', url)

def generate_secondary_table(table_name, primary_fields):
    """Creates a new table and duplicates the schema from the primary table."""
    print(f"--> Generating new table: {table_name}")
    
    # 1. Create the base table
    create_table_url = f"{BASEROW_URL}/api/database/tables/database/{DATABASE_ID}/"
    new_table = make_request('POST', create_table_url, json={"name": table_name})
    new_table_id = new_table['id']
    
    # 2. Get the default fields and isolate the primary one
    default_fields = get_fields(new_table_id)
    default_primary_field = next(f for f in default_fields if f.get('primary', False))
    # Keep track of only the NON-primary default fields to delete later
    fields_to_delete = [f['id'] for f in default_fields if not f.get('primary', False)]
    
    # 3. Add our Primary_Row_ID tracking column
    print(f"    Adding {PRIMARY_ID_TRACKER} column...")
    field_url = f"{BASEROW_URL}/api/database/fields/table/{new_table_id}/"
    make_request('POST', field_url, json={"name": PRIMARY_ID_TRACKER, "type": "number"})
    
    # 4. Replicate fields from the primary table
    for field in primary_fields:
        if field['name'] == PRIMARY_ID_TRACKER or field.get('read_only', False):
            continue
            
        # Strip properties that shouldn't be sent in a request
        payload = {k: v for k, v in field.items() if k not in ['id', 'table_id', 'order', 'primary', 'read_only']}
        
        try:
            if field.get('primary', False):
                # Update the existing default primary field instead of creating a new one
                print(f"    Updating default primary field to match: {field['name']}")
                update_url = f"{BASEROW_URL}/api/database/fields/{default_primary_field['id']}/"
                make_request('PATCH', update_url, json=payload)
            else:
                # Create standard field
                print(f"    Replicating field: {field['name']} ({field['type']})")
                make_request('POST', field_url, json=payload)
        except requests.exceptions.HTTPError:
            print(f"    [!] Warning: Failed to replicate field '{field['name']}'. Complex types may need manual setup.")
            continue
            
    # 5. Delete the unused default fields (Notes, Active, etc.)
    for f_id in fields_to_delete:
        make_request('DELETE', f"{BASEROW_URL}/api/database/fields/{f_id}/")

    return new_table_id

def sync_database():
    print("Fetching primary table schema and rows...")
    primary_fields = get_fields(PRIMARY_TABLE_ID)
    primary_rows = get_all_rows(PRIMARY_TABLE_ID)
    
    existing_tables = {t['name']: t['id'] for t in get_tables()}
    
    # Map out which rows belong to which city
    city_distributions = {}
    
    for row in primary_rows:
        selected_cities = row.get(CITY_COLUMN_NAME, [])
        for city_data in selected_cities:
            city = city_data.get('value')
            if city not in city_distributions:
                city_distributions[city] = []
            city_distributions[city].append(row)

    # Process each required city table
    for city, target_rows in city_distributions.items():
        table_name = f"Options_{city}"
        
        # Create table if it doesn't exist
        if table_name not in existing_tables:
            new_id = generate_secondary_table(table_name, primary_fields)
            existing_tables[table_name] = new_id
            
        table_id = existing_tables[table_name]
        print(f"\nProcessing sync for '{table_name}' (Table ID: {table_id})...")
        
        # Fetch current rows in this secondary table
        sec_rows = get_all_rows(table_id)
        
        # Map existing secondary rows by their original Primary ID
        sec_rows_by_primary_id = {
            int(row[PRIMARY_ID_TRACKER]): row 
            for row in sec_rows if row.get(PRIMARY_ID_TRACKER) is not None
        }
        
        expected_primary_ids = []
        
        # Add or Update rows
        for p_row in target_rows:
            p_id = p_row['id']
            expected_primary_ids.append(p_id)
            
            # Prepare payload: Copy primary data and inject the tracking ID
            payload = p_row.copy()
            payload[PRIMARY_ID_TRACKER] = p_id
            payload.pop('id', None) 
            payload.pop('order', None)
            
            if p_id in sec_rows_by_primary_id:
                # Update existing row
                sec_row_id = sec_rows_by_primary_id[p_id]['id']
                update_url = f"{BASEROW_URL}/api/database/rows/table/{table_id}/{sec_row_id}/?user_field_names=true"
                make_request('PATCH', update_url, json=payload)
                print(f"  - Updated Row (Primary ID: {p_id})")
            else:
                # Create new row
                create_url = f"{BASEROW_URL}/api/database/rows/table/{table_id}/?user_field_names=true"
                make_request('POST', create_url, json=payload)
                print(f"  - Created Row (Primary ID: {p_id})")

        # Delete rows that no longer belong
        for sec_p_id, sec_row in sec_rows_by_primary_id.items():
            if sec_p_id not in expected_primary_ids:
                sec_row_id = sec_row['id']
                delete_url = f"{BASEROW_URL}/api/database/rows/table/{table_id}/{sec_row_id}/"
                make_request('DELETE', delete_url)
                print(f"  - Deleted Row (Primary ID: {sec_p_id}) - No longer assigned to {city}")

if __name__ == "__main__":
    while True:
        try:
            sync_database()
            print("\nSync complete! Sleeping for {SLEEP_SECONDS} seconds...")
        except Exception as e:
            print(f"An error occurred: {e}")
            error_sleep_seconds = SLEEP_SECONDS / 5
            print("Retrying in {error_sleep_seconds} seconds...")
            time.sleep(error_sleep_seconds)
            continue
            
        time.sleep(SLEEP_SECONDS)
