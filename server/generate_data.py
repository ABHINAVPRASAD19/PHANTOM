import csv
import random
import datetime
import os

# Target rows to generate (~3.5MB file)
NUM_ROWS = 60000
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "transactions.csv")

print(f"Generating {NUM_ROWS} rows of generic server transaction logs into {OUTPUT_FILE}...")

# Some dummy weights to make errors rare
statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "PENDING", "PENDING", "FAILED", "ERROR"]

with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["TransactionID", "Timestamp", "IP_Address", "Amount", "Status", "Notes"])
    
    start_date = datetime.datetime.now() - datetime.timedelta(days=365)
    
    for i in range(NUM_ROWS):
        t_id = f"TXN-{random.randint(1000000, 9999999)}"
        timestamp = start_date + datetime.timedelta(minutes=i*13)
        ip = f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
        
        # 99% chance normal amount, 1% chance highly anomalous amount
        if random.random() < 0.01:
            amount = round(random.uniform(11000, 55000), 2)
            if random.random() < 0.1:
                status = "FRAUD"
            else:
                status = random.choice(statuses)
        else:
            amount = round(random.uniform(5, 500), 2)
            status = random.choice(statuses)
            
        notes = "Processed by gateway." if status == "SUCCESS" else "Requires review."
        
        # Super rare explicit manual injection for the demo
        if i == 500 or i == 25000 or i == 55000:
            status = "FRAUD"
            amount = 99999.99
            notes = "CRITICAL: Automated fraud detection triggered."
            
        writer.writerow([t_id, timestamp.strftime("%Y-%m-%d %H:%M:%S"), ip, amount, status, notes])

print(f"Finished generating 'transactions.csv'!")
