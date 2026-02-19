"""
GitHub Actions NetSuite Data Refresh Script
Pulls KPI data and updates the JSON file for the dashboard
"""

import requests
import json
import os
from datetime import datetime
from requests_oauthlib import OAuth1

# Read credentials from environment variables (set by GitHub Actions)
NETSUITE_ACCOUNT_ID = os.environ.get('NETSUITE_ACCOUNT_ID')
CONSUMER_KEY = os.environ.get('CONSUMER_KEY')
CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET')
TOKEN_ID = os.environ.get('TOKEN_ID')
TOKEN_SECRET = os.environ.get('TOKEN_SECRET')

# NetSuite API endpoint
BASE_URL = f"https://{NETSUITE_ACCOUNT_ID}.suitetalk.api.netsuite.com"
SUITEQL_URL = f"{BASE_URL}/services/rest/query/v1/suiteql"

print(f"Connecting to NetSuite account: {NETSUITE_ACCOUNT_ID}")

# SuiteQL Query - USING YOUR WORKING QUERY!
SUITEQL_QUERY = """
SELECT 
    t.id AS transaction_id,
    t.tranid AS sales_order_number,
    c.companyname AS customer_name,
    TO_CHAR(t.trandate, 'YYYY-MM-DD') AS transaction_date,
    TO_CHAR(t.custbody_po_received_date, 'YYYY-MM-DD') AS po_received_date,
    TO_CHAR(t.custbody_order_confirmed_date, 'YYYY-MM-DD') AS order_confirmed_date,
    TO_CHAR(t.shipdate, 'YYYY-MM-DD') AS target_ship_date,
    TO_CHAR(t.actualshipdate, 'YYYY-MM-DD') AS actual_ship_date,
    sm.itemid AS shipping_method_name,
    t.shippingcost AS shipping_cost,
    t.total AS order_total,
    CASE WHEN t.custbody_on_hold = 'T' THEN 'Yes' ELSE 'No' END AS on_hold_status
FROM transaction t
LEFT JOIN customer c ON t.entity = c.id
LEFT JOIN item sm ON t.shipmethod = sm.id
WHERE t.type = 'SalesOrd'
AND t.trandate >= TO_DATE('2024-01-01', 'YYYY-MM-DD')
ORDER BY t.trandate DESC
"""

def create_oauth_session():
    """Create OAuth1 session for NetSuite authentication"""
    oauth = OAuth1(
        client_key=CONSUMER_KEY,
        client_secret=CONSUMER_SECRET,
        resource_owner_key=TOKEN_ID,
        resource_owner_secret=TOKEN_SECRET,
        realm=NETSUITE_ACCOUNT_ID,
        signature_method='HMAC-SHA256'
    )
    return oauth

def fetch_netsuite_data():
    """Fetch data from NetSuite using SuiteQL"""
    print("Fetching data from NetSuite...")
    
    oauth = create_oauth_session()
    headers = {
        "Content-Type": "application/json",
        "Prefer": "transient"
    }
    
    payload = {"q": SUITEQL_QUERY}
    
    try:
        response = requests.post(
            SUITEQL_URL,
            auth=oauth,
            headers=headers,
            json=payload
        )
        
        if response.status_code == 200:
            print("✓ Successfully connected to NetSuite")
            data = response.json()
            return data.get('items', [])
        else:
            print(f"✗ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"✗ Connection error: {str(e)}")
        return None

def calculate_kpi_fields(data):
    """Calculate KPI fields from raw data"""
    print("Calculating KPI fields...")
    
    for row in data:
        # Calculate days for order entry
        if row.get('po_received_date') and row.get('transaction_date'):
            try:
                po_date = datetime.strptime(row['po_received_date'], '%Y-%m-%d')
                trans_date = datetime.strptime(row['transaction_date'], '%Y-%m-%d')
                row['days_order_entry'] = (trans_date - po_date).days
            except:
                row['days_order_entry'] = None
        else:
            row['days_order_entry'] = None
        
        # Calculate days for order confirmation
        if row.get('po_received_date') and row.get('order_confirmed_date'):
            try:
                po_date = datetime.strptime(row['po_received_date'], '%Y-%m-%d')
                conf_date = datetime.strptime(row['order_confirmed_date'], '%Y-%m-%d')
                row['days_order_confirmation'] = (conf_date - po_date).days
            except:
                row['days_order_confirmation'] = None
        else:
            row['days_order_confirmation'] = None
        
        # Calculate fulfillment time
        if row.get('order_confirmed_date') and row.get('actual_ship_date'):
            try:
                conf_date = datetime.strptime(row['order_confirmed_date'], '%Y-%m-%d')
                ship_date = datetime.strptime(row['actual_ship_date'], '%Y-%m-%d')
                row['days_fulfillment'] = (ship_date - conf_date).days
            except:
                row['days_fulfillment'] = None
        else:
            row['days_fulfillment'] = None
        
        # Calculate days late/early
        if row.get('target_ship_date') and row.get('actual_ship_date'):
            try:
                target = datetime.strptime(row['target_ship_date'], '%Y-%m-%d')
                actual = datetime.strptime(row['actual_ship_date'], '%Y-%m-%d')
                row['days_late_early'] = (actual - target).days
            except:
                row['days_late_early'] = None
        else:
            row['days_late_early'] = None
        
        # On-time delivery status
        if row.get('actual_ship_date'):
            if row['days_late_early'] is not None:
                row['on_time_delivery'] = 'On Time' if row['days_late_early'] <= 0 else 'Late'
            else:
                row['on_time_delivery'] = 'Pending'
        else:
            row['on_time_delivery'] = 'Pending'
        
        # Percent shipping cost
        try:
            shipping = float(row.get('shipping_cost', 0) or 0)
            total = float(row.get('order_total', 0) or 0)
            if total > 0:
                row['percent_shipping_cost'] = round((shipping / total) * 100, 2)
            else:
                row['percent_shipping_cost'] = 0
        except:
            row['percent_shipping_cost'] = 0
    
    return data

def save_to_json(data):
    """Save data to JSON file"""
    print("Saving data to JSON...")
    
    if not data:
        print("✗ No data to save")
        return False
    
    output = {
        "metadata": {
            "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "record_count": len(data)
        },
        "data": data
    }
    
    try:
        with open('NetSuite_KPI_Data.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"✓ Successfully saved {len(data)} records")
        return True
        
    except Exception as e:
        print(f"✗ Error saving JSON: {str(e)}")
        return False

def main():
    """Main execution function"""
    print("=" * 60)
    print("NetSuite KPI Data Refresh (GitHub Actions)")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Fetch data
    data = fetch_netsuite_data()
    
    if not data:
        print("\n✗ Failed to fetch data from NetSuite")
        exit(1)
    
    print(f"✓ Retrieved {len(data)} records")
    
    # Calculate KPIs
    data = calculate_kpi_fields(data)
    
    # Save to JSON
    success = save_to_json(data)
    
    print("=" * 60)
    if success:
        print("✓ DATA REFRESH COMPLETE!")
    else:
        print("✗ DATA REFRESH FAILED")
        exit(1)
    print("=" * 60)

if __name__ == "__main__":
    main()
