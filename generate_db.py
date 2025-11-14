import os
import csv
import math
import random
from faker import Faker
from multiprocessing import Pool, cpu_count
from pathlib import Path
from datetime import datetime
import psycopg2
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# config
OUT_DIR = Path("./data_chunks")
OUT_DIR.mkdir(exist_ok=True)
fake = Faker("en_US")

DB_HOST = "localhost"
DB_NAME = "omniship"
DB_USER = "postgres"
DB_PASS = "admin"
DB_PORT = 5432

# dataset sizes
SIZES = {
    "users": 200_000,
    "products": 4_000,
    "productvariants": 20_000,
    "orders": 1_000_000,
    "orderitems": 10_000_000,
    "routestops": 400_000,
    "maintenancelogs": 200_000
}

# chunk sizes: how many rows per CSV chunk file for parallel generation
CHUNK_SIZES = {
    "users": 50_000,            
    "products": 1000,          
    "productvariants": 5_000,  
    "orders": 100_000,         
    "orderitems": 200_000,     
    "routestops": 100_000,     
    "maintenancelogs": 50_000  
}

WORKERS = min(8, cpu_count())  # processes for generating files (threads)
COPY_WORKERS = min(8, cpu_count())  # concurrent COPY commands (threads)

# ---------- HELPERS ----------
def csv_safe(s):
    # replace newlines and ensure no stray tabs (we use CSV)
    if s is None:
        return ""
    return str(s).replace("\n", " ").replace("\r", " ")

def chunk_ranges(total, chunk_size):
    n_chunks = math.ceil(total / chunk_size)
    ranges = []
    for i in range(n_chunks):
        start = i * chunk_size
        end = min(total, (i + 1) * chunk_size)
        ranges.append((start, end, i))
    return ranges

# generators
def gen_users_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"users_{idx:03d}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["username","password_hash","email","full_name","user_type","created_at"])
        for i in range(start, end):
            uname = f"{fake.user_name()}_{i}"  # ensures uniqueness
            email = f"{uname}@example.com"     # also unique

            writer.writerow([
                uname,
                fake.sha256(),
                email,
                fake.name(),
                "customer",
                fake.date_time_this_year().isoformat(sep=' ')
            ])
    return str(fname)


def gen_products_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"products_{idx:03d}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["category_id","product_name","description","brand","created_at"])
        for _ in range(start, end):
            writer.writerow([
                random.randint(1, 50),
                fake.word(),
                fake.sentence(nb_words=6),
                fake.company(),
                fake.date_time_this_year().isoformat(sep=' ')
            ])
    return str(fname)

def gen_productvariants_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"productvariants_{idx:03d}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id","sku","color","size","weight","base_price"])
        for i in range(start, end):
            writer.writerow([
                random.randint(1, SIZES["products"]),
                f"{fake.uuid4()}_{i}",               
                fake.color_name(),
                random.choice(['XS','S','M','L','XL']),
                round(random.uniform(0.1, 10.0), 2),
                round(random.uniform(5, 500), 2)
            ])
    return str(fname)

def gen_orders_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"orders_{idx:03d}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id","warehouse_id","order_date","region","currency_code","subtotal","tax_id","promo_id","total_amount","status"])
        for _ in range(start, end):
            subtotal = round(random.uniform(10, 1000), 2)
            tax = round(subtotal * 0.17, 2)
            writer.writerow([
                random.randint(1, SIZES["users"]),
                random.randint(1, 10),
                fake.date_time_this_year().isoformat(sep=' '),
                fake.country(),
                "USD",
                subtotal,
                "",
                "",
                round(subtotal + tax, 2),
                random.choice(["pending","shipped","delivered"])
            ])
    return str(fname)

def gen_orderitems_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"orderitems_{idx:03d}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id","variant_id","quantity","unit_price"])
        for _ in range(start, end):
            writer.writerow([
                random.randint(1, SIZES["orders"]),
                random.randint(1, SIZES["productvariants"]),
                random.randint(1, 5),
                round(random.uniform(5, 500), 2)
            ])
    return str(fname)

def gen_routestops_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"routestops_{idx:03d}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["route_id","stop_order","address","city","country","arrival_time","departure_time","status"])
        for _ in range(start, end):
            writer.writerow([
                random.randint(1, 10000),
                random.randint(1, 50),
                csv_safe(fake.address()),
                fake.city(),
                fake.country(),
                fake.date_time_this_year().isoformat(sep=' '),
                fake.date_time_this_year().isoformat(sep=' '),
                random.choice(["pending","completed"])
            ])
    return str(fname)

def gen_maint_logs_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"maintenancelogs_{idx:03d}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["vehicle_id","maintenance_date","description","cost","performed_by"])
        for _ in range(start, end):
            writer.writerow([
                random.randint(1, 50),
                fake.date_time_this_year().isoformat(sep=' '),
                fake.sentence(nb_words=12),
                round(random.uniform(20, 1000), 2),
                random.randint(1, SIZES["users"])
            ])
    return str(fname)

# generation driver
GEN_FUNCS = {
    "users": gen_users_chunk,
    "products": gen_products_chunk,
    "productvariants": gen_productvariants_chunk,
    "orders": gen_orders_chunk,
    "orderitems": gen_orderitems_chunk,
    "routestops": gen_routestops_chunk,
    "maintenancelogs": gen_maint_logs_chunk
}

def generate_table(table):
    total = SIZES[table]
    chunk_size = CHUNK_SIZES[table]
    ranges = chunk_ranges(total, chunk_size)
    args = [(r[0], r[1], r[2]) for r in ranges]
    print(f"Generating {table}: {total} rows in {len(args)} chunk files (chunk_size={chunk_size})")
    with Pool(WORKERS) as p:
        results = list(tqdm(p.imap(GEN_FUNCS[table], args), total=len(args)))
    return results

# copy (parallel) to DB
COPY_SQL = {
    "users": "COPY users(username,password_hash,email,full_name,user_type,created_at) FROM STDIN WITH CSV HEADER DELIMITER ','",
    "products": "COPY products(category_id,product_name,description,brand,created_at) FROM STDIN WITH CSV HEADER DELIMITER ','",
    "productvariants": "COPY productvariants(product_id,sku,color,size,weight,base_price) FROM STDIN WITH CSV HEADER DELIMITER ','",
    "orders": "COPY orders(user_id,warehouse_id,order_date,region,currency_code,subtotal,tax_id,promo_id,total_amount,status) FROM STDIN WITH CSV HEADER DELIMITER ','",
    "orderitems": "COPY orderitems(order_id,variant_id,quantity,unit_price) FROM STDIN WITH CSV HEADER DELIMITER ','",
    "routestops": "COPY routestops(route_id,stop_order,address,city,country,arrival_time,departure_time,status) FROM STDIN WITH CSV HEADER DELIMITER ','",
    "maintenancelogs": "COPY maintenancelogs(vehicle_id,maintenance_date,description,cost,performed_by) FROM STDIN WITH CSV HEADER DELIMITER ','"
}

def copy_file_to_db(fname, table):
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
    cur = conn.cursor()
    with open(fname, "r", encoding="utf-8") as f:
        cur.copy_expert(COPY_SQL[table], f)
    conn.commit()
    cur.close()
    conn.close()
    return fname

def parallel_copy(table, files):
    print(f"Starting COPY for {table} ({len(files)} files) with {COPY_WORKERS} workers")
    with ThreadPoolExecutor(max_workers=COPY_WORKERS) as ex:
        list(tqdm(ex.map(lambda fn: copy_file_to_db(fn, table), files), total=len(files)))
    print(f"COPY finished for {table}")

# main
def main():
    start_time = datetime.now()
    print("=== GENERATION PHASE ===")
    generated_files = {}
    # generate each table sequentially (but each table uses multiprocessing to create chunks)
    for table in ["users","products","productvariants","orders","orderitems","routestops","maintenancelogs"]:
        files = generate_table(table)
        generated_files[table] = files

    print("\n=== LOAD/ COPY PHASE ===")
    # perform COPY per table in parallel (parallel across files)
    # do tables in order that satisfy FKs: users -> products/productvariants -> orders -> orderitems ...
    copy_order = ["users","products","productvariants","orders","orderitems","routestops","maintenancelogs"]
    for table in copy_order:
        files = generated_files[table]
        parallel_copy(table, files)

    elapsed = datetime.now() - start_time
    print(f"\nAll done in {elapsed}. Data chunks in: {OUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
