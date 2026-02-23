import logging
import os
import requests
import time
import sys
import json

# --- VALIDATION ---
def validate_config():
    required = ['BASEROW_TOKEN', 'PRIMARY_TABLE_ID', 'MULTI_SELECT_COLUMN_ID', 'PRIMARY_ID_TRACKER', 'CLONE_COLUMNS']
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"❌ FATAL ERROR: Missing variables: {', '.join(missing)}")
        sys.exit(1)

# Env Vars
BASEROW_TOKEN = os.getenv('BASEROW_TOKEN')
SLEEP_SECONDS = int(os.getenv('SLEEP_SECONDS', 3600))
PRIMARY_TABLE_ID = os.getenv('PRIMARY_TABLE_ID')
SECONDARY_COLUMN_PREFIX = os.getenv('SECONDARY_COLUMN_PREFIX')
MULTI_SELECT_COLUMN_ID = os.getenv('MULTI_SELECT_COLUMN_ID')
PRIMARY_ID_COLUMN_NAME = os.getenv('PRIMARY_ID_TRACKER') 
BASEROW_URL = os.getenv("BASEROW_URL", "https://api.baserow.io")
CLONE_COLUMNS_LIST = [c.strip() for c in os.getenv("CLONE_COLUMNS", "").split(",")] if os.getenv("CLONE_COLUMNS") else []

HEADERS = {'Authorization': f'Token {BASEROW_TOKEN}', 'Content-Type': 'application/json'}
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Filter Logic: Expected format '{"123": ["Value1", "Value2"], "456": ["Allow"]}'
ROW_FILTERS_RAW = os.getenv('ROW_FILTERS', '{}')
try:
    ROW_FILTERS = json.loads(ROW_FILTERS_RAW)
    # Baserow needs keys formatted as `field_123`, but we don't want to leak that info to the config.
    # Therefore, we need to add the `field_` prefix to each key in the filter config.
    ROW_FILTERS = {f"field_{k}": v for k, v in ROW_FILTERS.items()}
except json.JSONDecodeError:
    logger.error(f"❌ FATAL ERROR: ROW_FILTERS is not valid JSON: {ROW_FILTERS_RAW}")
    sys.exit(1)

def make_request(method, url, **kwargs):
    logger.debug(f"API Request: {method} {url}")
    kwargs['headers'] = HEADERS
    response = requests.request(method, url, **kwargs)
    if not response.ok:
        logger.error(f"API Error: {response.status_code} - {response.text}")
        response.raise_for_status()
    return response.json() if response.content else None

def row_passes_filters(row):
    """Checks if the row contains allowed values for the specified filter columns."""
    for field_key, allowed_values in ROW_FILTERS.items():
        cell_value = row.get(field_key)
        
        # Extract string value from cell (handles strings, select dicts, and multi-select lists)
        current_values = []
        if isinstance(cell_value, list):
            current_values = [i.get('value') if isinstance(i, dict) else str(i) for i in cell_value]
        elif isinstance(cell_value, dict):
            current_values = [cell_value.get('value')]
        else:
            current_values = [str(cell_value)] if cell_value is not None else []

        logger.debug(f"Filtering Row {row.get('id')}: Field {field_key} has values {current_values}, allowed are {allowed_values}")
        # Check if there's any overlap between current cell values and allowed values
        if not any(val in allowed_values for val in current_values):
            logger.debug(f"Row {row.get('id')} blocked by filter on {field_key}. Values {current_values} not in {allowed_values}")
            return False
    return True

def get_field_and_option_map(target_table_id, primary_field_defs):
    target_fields = make_request('GET', f"{BASEROW_URL}/api/database/fields/table/{target_table_id}/")
    target_name_map = {f['name']: f for f in target_fields}
    
    field_mapping = {}
    option_mapping = {}
    
    for pf in primary_field_defs:
        p_id_key = f"field_{pf['id']}"
        p_name = pf['name']
        
        if p_name in target_name_map:
            tf = target_name_map[p_name]
            t_id_key = f"field_{tf['id']}"
            field_mapping[p_id_key] = t_id_key
            if 'select_options' in tf:
                option_mapping[t_id_key] = {opt['value']: opt['id'] for opt in tf['select_options']}
        else:
            logger.warning(f"Field '{p_name}' not found in target table {target_table_id}")

    tracker_field = target_name_map.get(PRIMARY_ID_COLUMN_NAME)
    tracker_key = f"field_{tracker_field['id']}" if tracker_field else None
    return field_mapping, option_mapping, tracker_key

def get_secondary_table_name(primary_meta, label):
    secondary_prefix = SECONDARY_COLUMN_PREFIX or f"{primary_meta['name']}_"
    return f"{secondary_prefix}{label}"

def sync_database():
    logger.info("--- Starting Sync Cycle ---")
    all_tables = make_request('GET', f"{BASEROW_URL}/api/database/tables/all-tables/")
    
    primary_meta = next((t for t in all_tables if str(t['id']) == str(PRIMARY_TABLE_ID)), None)
    if not primary_meta:
        logger.error(f"Primary table {PRIMARY_TABLE_ID} not found.")
        return

    primary_fields = make_request('GET', f"{BASEROW_URL}/api/database/fields/table/{PRIMARY_TABLE_ID}/")
    fields_to_clone = [f for f in primary_fields if str(f['id']) in CLONE_COLUMNS_LIST]
    
    logger.info(f"Targeting {len(fields_to_clone)} columns for cloning: {[f['name'] for f in fields_to_clone]}")

    primary_rows = []
    url = f"{BASEROW_URL}/api/database/rows/table/{PRIMARY_TABLE_ID}/?user_field_names=false"
    while url:
        data = make_request('GET', url)
        primary_rows.extend(data['results'])
        url = data.get('next')
    
    logger.info(f"Fetched {len(primary_rows)} total rows from Primary Table '{primary_meta['name']}'")

    # Apply Allowlist Filtering
    filtered_rows = [r for r in primary_rows if row_passes_filters(r)]
    logger.info(f"Rows passing filters: {len(filtered_rows)} of {len(primary_rows)}")

    table_map = {t['name']: t['id'] for t in all_tables}
    control_key = f"field_{MULTI_SELECT_COLUMN_ID}"

    # Grouping
    categorized = {}
    for row in filtered_rows:
        raw_val = row.get(control_key)
        if not raw_val: continue
        
        # Ensure we are working with a list of values
        items = raw_val if isinstance(raw_val, list) else [raw_val]
        
        for item in items:
            # Extract the actual text label
            if isinstance(item, dict):
                label = item.get('value')
            else:
                label = str(item)
                
            if label:
                categorized.setdefault(label, []).append(row)

    logger.info(f"Identified {len(categorized)} categories: {', '.join(categorized.keys())}")

    for label, target_rows in categorized.items():
        target_name = get_secondary_table_name(primary_meta, label)
        if target_name not in table_map:
            logger.debug(f"Skipping '{label}': Table '{target_name}' does not exist.")
            continue
        
        t_id = table_map[target_name]
        f_map, opt_map, tracker_key = get_field_and_option_map(t_id, fields_to_clone)
        
        if not tracker_key:
            logger.error(f"Tracker column '{PRIMARY_ID_COLUMN_NAME}' missing in '{target_name}'. Skipping table.")
            continue

        # Fetch Target Rows for comparison
        sec_rows = []
        sec_url = f"{BASEROW_URL}/api/database/rows/table/{t_id}/?user_field_names=false"
        while sec_url:
            data = make_request('GET', sec_url)
            sec_rows.extend(data['results'])
            sec_url = data.get('next')
            
        sec_by_origin = {str(r.get(tracker_key)): r for r in sec_rows if r.get(tracker_key)}
        logger.info(f"Syncing {len(target_rows)} rows to '{target_name}'...")

        synced_ids = []
        for p_row in target_rows:
            p_id = str(p_row['id'])
            synced_ids.append(p_id)
            payload = {tracker_key: p_id}

            for p_key, t_key in f_map.items():
                val = p_row.get(p_key)
                if val is None: continue

                if t_key in opt_map:
                    if isinstance(val, list):
                        # Use .get('value') if it's a dict, otherwise use the string itself
                        payload[t_key] = [opt_map[t_key][i['value'] if isinstance(i, dict) else i] 
                                         for i in val if (i['value'] if isinstance(i, dict) else i) in opt_map[t_key]]
                    elif isinstance(val, dict):
                        payload[t_key] = opt_map[t_key].get(val['value'])
                else:
                    payload[t_key] = val

            if p_id in sec_by_origin:
                make_request('PATCH', f"{BASEROW_URL}/api/database/rows/table/{t_id}/{sec_by_origin[p_id]['id']}/?user_field_names=false", json=payload)
            else:
                make_request('POST', f"{BASEROW_URL}/api/database/rows/table/{t_id}/?user_field_names=false", json=payload)

        # Cleanup orphans
        orphans = [oid for oid in sec_by_origin if oid not in synced_ids]
        if orphans:
            logger.info(f"Cleaning up {len(orphans)} orphaned rows from '{target_name}'.")
            for oid in orphans:
                make_request('DELETE', f"{BASEROW_URL}/api/database/rows/table/{t_id}/{sec_by_origin[oid]['id']}/")

if __name__ == "__main__":
    validate_config()
    while True:
        try:
            sync_database()
            logger.info(f"--- Cycle Complete. Waiting {SLEEP_SECONDS}s ---")
        except Exception:
            logger.exception("A critical error occurred during the sync cycle.")
        time.sleep(SLEEP_SECONDS)