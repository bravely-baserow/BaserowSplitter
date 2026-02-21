import os
import requests
import time

# --- CONFIGURATION ---
BASEROW_TOKEN = os.getenv('BASEROW_TOKEN')
SLEEP_SECONDS = int(os.getenv('SLEEP_SECONDS', 3600))
DATABASE_ID = os.getenv('DATABASE_ID')
PRIMARY_TABLE_ID = os.getenv('PRIMARY_TABLE_ID')
MULTI_SELECT_COLUMN_NAME = os.getenv('MULTI_SELECT_COLUMN_NAME', 'MultiSelect')
PRIMARY_ID_TRACKER = os.getenv('PRIMARY_ID_TRACKER', 'Primary_Row_ID')
BASEROW_URL = 'https://api.baserow.io'

HEADERS = {
    'Authorization': f'Token {BASEROW_TOKEN}',
    'Content-Type': 'application/json'
}

def make_request(method, url, **kwargs):
    kwargs['headers'] = HEADERS
    response = requests.request(method, url, **kwargs)
    if not response.ok:
        print(f"API Error: {response.status_code} - {response.text}")
        response.raise_for_status()
    return response.json() if response.content else None

def get_all_rows(table_id):
    rows = []
    url = f"{BASEROW_URL}/api/database/rows/table/{table_id}/?user_field_names=true"
    while url:
        data = make_request('GET', url)
        rows.extend(data['results'])
        url = data.get('next')
    return rows

def get_tables():
    """Fetches all tables in the database."""
    url = f"{BASEROW_URL}/api/database/tables/all-tables/"
    return make_request('GET', url)

def get_fields(table_id):
    """Fetches the schema/fields of a table."""
    url = f"{BASEROW_URL}/api/database/fields/table/{table_id}/"
    return make_request('GET', url)

def sync_database():
    print("Fetching database schema...")
    all_tables = get_tables()

    primary_table_meta = next((t for t in all_tables if str(t['id']) == str(PRIMARY_TABLE_ID)), None)
    
    if not primary_table_meta:
        print(f"Error: Could not find a table with ID {PRIMARY_TABLE_ID}")
        return
    
    primary_table_name = primary_table_meta.get('name')

    print("Fetching primary table schema and rows...")
    primary_fields = get_fields(PRIMARY_TABLE_ID)
    primary_rows = get_all_rows(PRIMARY_TABLE_ID)
    
    existing_tables = {t['name']: t['id'] for t in get_tables()}

    # Map out which rows belong to which multi-select
    selected_options = {}
    
    for row in primary_rows:
        multi_select_for_row = row.get(MULTI_SELECT_COLUMN_NAME, [])
        for selection_option in multi_select_for_row:
            selection = selection_option.get('value')
            if selection not in selected_options:
                selected_options[selection] = []
            selected_options[selection].append(row)

    # Process each required "select" table
    for select, target_rows in selected_options.items():
        table_name = f"{primary_table_name}_{select}"
        
        if table_name not in existing_tables:
            print(f"Skipping {select}: Table '{table_name}' not found. Please create it manually.")
            continue
            
        table_id = existing_tables[table_name]
        print(f"Syncing {len(target_rows)} rows to {table_name}...")

        # Get secondary rows to compare
        sec_rows = get_all_rows(table_id)
        sec_rows_by_primary_id = {
            int(r[PRIMARY_ID_TRACKER]): r 
            for r in sec_rows if r.get(PRIMARY_ID_TRACKER) is not None
        }

        # Update or Create
        expected_ids = []
        for p_row in target_rows:
            p_id = p_row['id']
            expected_ids.append(p_id)
            payload = p_row.copy()
            payload[PRIMARY_ID_TRACKER] = p_id
            for key in ['id', 'order']: payload.pop(key, None)

            if p_id in sec_rows_by_primary_id:
                s_id = sec_rows_by_primary_id[p_id]['id']
                make_request('PATCH', f"{BASEROW_URL}/api/database/rows/table/{table_id}/{s_id}/?user_field_names=true", json=payload)
            else:
                make_request('POST', f"{BASEROW_URL}/api/database/rows/table/{table_id}/?user_field_names=true", json=payload)

        # Cleanup
        for s_p_id, s_row in sec_rows_by_primary_id.items():
            if s_p_id not in expected_ids:
                make_request('DELETE', f"{BASEROW_URL}/api/database/rows/table/{table_id}/{s_row['id']}/")

if __name__ == "__main__":
    while True:
        try:
            sync_database()
            print(f"Sync complete. Sleeping {SLEEP_SECONDS}s...")
        except Exception as e:
            print(f"Runtime error: {e}")
        time.sleep(SLEEP_SECONDS)
