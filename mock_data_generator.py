import csv
import random
import datetime
import uuid

# Configuration
filename = 'mock_data.csv'
rows = 1000

print(f"Generating {rows} rows with 11 total columns...")

# Lists for random data generation
regions = ['North America', 'Europe', 'APAC', 'LATAM', 'EMEA']
categories = ['Electronics', 'Home & Garden', 'Fashion', 'Automotive', 'Books']
payments = ['Credit Card', 'PayPal', 'Bank Transfer', 'Crypto', 'Apple Pay']
devices = ['Desktop', 'Mobile', 'Tablet', 'Smart Watch']

with open(filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # 1. Header (Original 5 + 6 New)
    writer.writerow([
        'user_id', 'full_name', 'email', 'total_spend', 'signup_date', 
        'region', 'product_category', 'payment_method', 'is_active', 'device_type', 'loyalty_score'
    ])

    for _ in range(rows):
        # Original Logic
        uid = str(uuid.uuid4())[:8]
        fname = random.choice(['Alex', 'Jordan', 'Casey', 'Taylor', 'Morgan', 'Jamie', 'Riley', 'Avery'])
        lname = random.choice(['Smith', 'Doe', 'Lee', 'Wong', 'Garcia', 'Johnson', 'Martinez', 'Brown'])
        email = f"{fname.lower()}.{lname.lower()}.{random.randint(10,99)}@example.com"
        spend = round(random.uniform(10.50, 5000.00), 2)
        
        start_date = datetime.date.today() - datetime.timedelta(days=365)
        random_date = start_date + datetime.timedelta(days=random.randint(0, 365))
        
        # New Logic for 6 additional fields
        region = random.choice(regions)
        category = random.choice(categories)
        payment = random.choice(payments)
        is_active = random.choice([True, False]) # Boolean flag
        device = random.choice(devices)
        loyalty = random.randint(1, 100) # Integer score

        # Write Row
        writer.writerow([
            uid, fname + " " + lname, email, spend, random_date, 
            region, category, payment, is_active, device, loyalty
        ])

print(f"Done! '{filename}' created with extended schema.")