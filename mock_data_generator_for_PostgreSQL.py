import csv
import random
import datetime
import uuid

# Configuración: Nombre distinto para diferenciar el archivo
filename = 'mock_data_v2_new_batch.csv'
rows = 500  # Generaremos 500 filas nuevas

print(f"Generando {rows} filas nuevas (Lote 2 - Futuro)...")

# Listas (Mismas categorías para mantener consistencia)
regions = ['North America', 'Europe', 'APAC', 'LATAM', 'EMEA']
categories = ['Electronics', 'Home & Garden', 'Fashion', 'Automotive', 'Books']
payments = ['Credit Card', 'PayPal', 'Bank Transfer', 'Crypto', 'Apple Pay']
devices = ['Desktop', 'Mobile', 'Tablet', 'Smart Watch']

with open(filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # 1. Header (DEBE ser idéntico al anterior para que BigQuery no de error)
    writer.writerow([
        'user_id', 'full_name', 'email', 'total_spend', 'signup_date', 
        'region', 'product_category', 'payment_method', 'is_active', 'device_type', 'loyalty_score'
    ])

    for _ in range(rows):
        uid = str(uuid.uuid4())[:8]
        fname = random.choice(['Elena', 'Marco', 'Sofia', 'Lucas', 'Ana', 'Diego', 'Valentina', 'Leo'])
        lname = random.choice(['Rossi', 'Silva', 'Müller', 'Schmidt', 'Dubois', 'Kowalski', 'Tanaka', 'Kim'])
        email = f"{fname.lower()}.{lname.lower()}.{random.randint(100,999)}@example.com"
        
        # CAMBIO: Gastos más altos para notar la diferencia
        spend = round(random.uniform(500.00, 10000.00), 2)
        
        # CAMBIO: Fechas en el futuro (2026-2027)
        start_date = datetime.date(2026, 6, 1)
        random_date = start_date + datetime.timedelta(days=random.randint(0, 365))
        
        region = random.choice(regions)
        category = random.choice(categories)
        payment = random.choice(payments)
        is_active = random.choice([True, False])
        device = random.choice(devices)
        loyalty = random.randint(50, 100) # Scores más altos

        writer.writerow([
            uid, fname + " " + lname, email, spend, random_date, 
            region, category, payment, is_active, device, loyalty
        ])

print(f"¡Listo! Se creó '{filename}'.")