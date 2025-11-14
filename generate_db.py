import os
import csv
import math
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timedelta
import psycopg2
from tqdm import tqdm

# config
OUT_DIR = Path("./data_chunks")
OUT_DIR.mkdir(parents=True, exist_ok=True)

DB_HOST = "localhost"
DB_NAME = "omniship"
DB_USER = "postgres"
DB_PASS = "admin"
DB_PORT = 5432

# table sizes
SIZES = {
    "categories": 50,
    "users": 200000,
    "roles": 5,
    "permissions": 15,
    "rolepermissions": 50,
    "userroles": 200000,
    "warehouses": 10,
    "suppliers": 1000,
    "products": 4000,
    "productvariants": 20000,
    "inventory": 40000,
    "promotions": 200,
    "taxes": 200,
    "orders": 1000000,
    "orderitems": 10000000,
    "payments": 1000000,
    "vehicles": 200,
    "drivers": 500,
    "maintenancelogs": 200000,
    "deliveryshipments": 500000,
    "deliveryroutes": 500000,
    "routestops": 400000,
    "proofofdelivery": 400000,
    "purchaseorders": 200000,
    "purchaseorderitems": 1000000,
    "shipments": 200000,
    "qualitychecks": 200000
}

# chunk sizes (tune them if needed)
CHUNK_SIZES = {
    "categories": 50,
    "users": 50000,
    "roles": 5,
    "permissions": 15,
    "rolepermissions": 50,
    "userroles": 50000,
    "warehouses": 10,
    "suppliers": 250,
    "products": 1000,
    "productvariants": 5000,
    "inventory": 10000,
    "promotions": 200,
    "taxes": 200,
    "orders": 100000,
    "orderitems": 200000,
    "payments": 100000,
    "vehicles": 200,
    "drivers": 250,
    "maintenancelogs": 50000,
    "deliveryshipments": 100000,
    "deliveryroutes": 100000,
    "routestops": 100000,
    "proofofdelivery": 100000,
    "purchaseorders": 50000,
    "purchaseorderitems": 100000,
    "shipments": 50000,
    "qualitychecks": 50000
}

WORKERS = min(8, cpu_count())
COPY_WORKERS = min(8, cpu_count())

BASE_DATE = datetime(2020, 1, 1)

# ---------- HELPERS ----------
def chunk_ranges(total, chunk_size):
    n_chunks = math.ceil(total / chunk_size)
    ranges = []
    for i in range(n_chunks):
        start = i * chunk_size
        end = min(total, (i + 1) * chunk_size)
        ranges.append((start, end, i))
    return ranges

def gi_to_iso(gi):
    dt = BASE_DATE + timedelta(seconds=gi)
    return dt.isoformat(sep=' ')

def deterministic(prefix, gi, width=9):
    return f"{prefix}_{gi:0{width}d}"

def write_csv_rows(fname, header, rows):
    with open(fname, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)

# Generators
# Each function writes CSV chunk file and returns path string.

def gen_categories_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"categories_{idx:03d}.csv"
    header = ["parent_id","category_name","description"]
    rows = []
    for gi in range(start, end):
        parent = (gi % SIZES['categories']) + 1 if (gi % 10 == 0 and gi != 0) else ''
        rows.append([parent, deterministic('category', gi, 4), f"Category desc {gi}"])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_users_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"users_{idx:03d}.csv"
    header = ["username","password_hash","email","full_name","user_type","created_at"]
    rows = []
    for gi in range(start, end):
        uname = deterministic('user', gi)
        email = f"{uname}@example.com"
        pw = f"hash_{gi:012d}"
        fullname = deterministic('UserFull', gi)
        rows.append([uname, pw, email, fullname, 'customer', gi_to_iso(gi)])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_roles_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"roles_{idx:03d}.csv"
    header = ["role_name","description"]
    rows = []
    for gi in range(start, end):
        rows.append([deterministic('role', gi), f"Role {gi}"])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_permissions_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"permissions_{idx:03d}.csv"
    header = ["permission_name","description"]
    rows = []
    for gi in range(start, end):
        rows.append([deterministic('permission', gi), f"Permission {gi}"])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_rolepermissions_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"rolepermissions_{idx:03d}.csv"
    header = ["role_id","permission_id"]
    rows = []
    total_pairs = SIZES['roles'] * SIZES['permissions']
    for gi in range(start, end):
        j = gi % total_pairs
        role_id = (j % SIZES['roles']) + 1
        perm_id = (j // SIZES['roles']) + 1
        rows.append([role_id, perm_id])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_userroles_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"userroles_{idx:03d}.csv"
    header = ["user_id","role_id"]
    rows = []
    for gi in range(start, end):
        user_id = (gi % SIZES['users']) + 1
        role_id = (gi % SIZES['roles']) + 1
        rows.append([user_id, role_id])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_warehouses_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"warehouses_{idx:03d}.csv"
    header = ["warehouse_name","location_country","location_city","address","capacity"]
    rows = []
    for gi in range(start, end):
        rows.append([deterministic('wh', gi), f"Country_{gi%200}", f"City_{gi%1000}", deterministic('Address', gi), 1000 + (gi % 100000)])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_suppliers_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"suppliers_{idx:03d}.csv"
    header = ["company_name","contact_name","contact_email","phone","address","country"]
    rows = []
    for gi in range(start, end):
        comp = deterministic('supplier', gi, 6)
        email = f"{comp}@supplier.example"
        phone = f"+100000{gi:07d}"
        rows.append([comp, deterministic('Contact', gi), email, phone, deterministic('Addr', gi), f"Country_{gi%200}"])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_products_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"products_{idx:03d}.csv"
    header = ["category_id","product_name","description","brand","created_at"]
    rows = []
    for gi in range(start, end):
        cat = (gi % SIZES['categories']) + 1
        pname = deterministic('product', gi)
        brand = deterministic('brand', gi % 1000)
        rows.append([cat, pname, f"Product {gi} desc", brand, gi_to_iso(gi)])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_productvariants_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"productvariants_{idx:03d}.csv"
    header = ["product_id","sku","color","size","weight","base_price"]
    rows = []
    sizes = ['XS','S','M','L','XL']
    for gi in range(start, end):
        pid = (gi % SIZES['products']) + 1
        sku = deterministic('SKU', gi)
        color = f"Color_{gi%140}"
        size = sizes[gi % 5]
        weight = round(0.1 + (gi % 1000) * 0.01, 2)
        price = round(5.0 + (gi % 5000) * 0.1, 2)
        rows.append([pid, sku, color, size, weight, price])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_inventory_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"inventory_{idx:03d}.csv"
    header = ["warehouse_id","variant_id","quantity","last_updated"]
    rows = []
    for gi in range(start, end):
        wid = (gi % SIZES['warehouses']) + 1
        vid = (gi % SIZES['productvariants']) + 1
        qty = gi % 2000
        rows.append([wid, vid, qty, gi_to_iso(gi)])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_promotions_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"promotions_{idx:03d}.csv"
    header = ["promo_name","discount_percent","start_date","end_date"]
    rows = []
    for gi in range(start, end):
        pname = deterministic('promo', gi)
        disc = round((gi % 50) + 1, 2)
        rows.append([pname, disc, gi_to_iso(gi), gi_to_iso(gi+1000)])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_taxes_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"taxes_{idx:03d}.csv"
    header = ["region","tax_rate"]
    rows = []
    for gi in range(start, end):
        rows.append([f"Region_{gi%200}", round((gi % 25) + 0.5, 2)])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_orders_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"orders_{idx:03d}.csv"
    header = ["user_id","warehouse_id","order_date","region","currency_code","subtotal","tax_id","promo_id","total_amount","status"]
    rows = []
    statuses = ['pending','shipped','delivered']
    for gi in range(start, end):
        uid = (gi % SIZES['users']) + 1
        wid = (gi % SIZES['warehouses']) + 1
        subtotal = round(10.0 + (gi % 1000) * 0.5, 2)
        tax = round(subtotal * 0.17, 2)
        total = round(subtotal + tax, 2)
        status = statuses[gi % 3]
        rows.append([uid, wid, gi_to_iso(gi), f"Region_{gi%50}", 'USD', subtotal, '', '', total, status])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_orderitems_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"orderitems_{idx:03d}.csv"
    header = ["order_id","variant_id","quantity","unit_price"]
    rows = []
    for gi in range(start, end):
        oid = (gi % SIZES['orders']) + 1
        vid = (gi % SIZES['productvariants']) + 1
        qty = (gi % 5) + 1
        unit = round(5.0 + (gi % 400) * 0.25, 2)
        rows.append([oid, vid, qty, unit])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_payments_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"payments_{idx:03d}.csv"
    header = ["order_id","payment_method","amount","payment_date","transaction_ref"]
    rows = []
    methods = ['card','paypal','transfer']
    for gi in range(start, end):
        oid = (gi % SIZES['orders']) + 1
        amt = round(5.0 + (gi % 2000) * 0.5, 2)
        tx = deterministic('TX', gi)
        method = methods[gi % 3]
        rows.append([oid, method, amt, gi_to_iso(gi), tx])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_vehicles_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"vehicles_{idx:03d}.csv"
    header = ["vehicle_type","license_plate","capacity_kg","status"]
    rows = []
    types = ['truck','van','bike']
    states = ['available','in_service','maintenance']
    for gi in range(start, end):
        vtype = types[gi % 3]
        plate = deterministic('PLATE', gi)
        cap = 500 + (gi % 20000)
        status = states[gi % 3]
        rows.append([vtype, plate, cap, status])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_drivers_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"drivers_{idx:03d}.csv"
    header = ["user_id","license_number","phone_number","assigned_vehicle"]
    rows = []
    for gi in range(start, end):
        user_id = (gi % SIZES['users']) + 1
        lic = deterministic('LIC', gi)
        phone = f"+200000{gi:07d}"
        av = (gi % SIZES['vehicles']) + 1
        rows.append([user_id, lic, phone, av])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_maintenancelogs_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"maintenancelogs_{idx:03d}.csv"
    header = ["vehicle_id","maintenance_date","description","cost","performed_by"]
    rows = []
    for gi in range(start, end):
        vid = (gi % SIZES['vehicles']) + 1
        desc = f"Maintenance entry {gi}"
        cost = round(20.0 + (gi % 1000) * 0.5, 2)
        perf = (gi % SIZES['users']) + 1
        rows.append([vid, gi_to_iso(gi), desc, cost, perf])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_deliveryshipments_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"deliveryshipments_{idx:03d}.csv"
    header = ["order_id","vehicle_id","driver_id","start_time","end_time","status"]
    rows = []
    for gi in range(start, end):
        oid = (gi % SIZES['orders']) + 1
        vid = (gi % SIZES['vehicles']) + 1
        did = (gi % SIZES['drivers']) + 1
        st = gi_to_iso(gi)
        et = gi_to_iso(gi+3600)
        status = ['pending','in_transit','delivered'][gi % 3]
        rows.append([oid, vid, did, st, et, status])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_deliveryroutes_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"deliveryroutes_{idx:03d}.csv"
    header = ["delivery_id","route_name","distance_km","estimated_time_min"]
    rows = []
    for gi in range(start, end):
        did = (gi % SIZES['deliveryshipments']) + 1
        rn = deterministic('route', gi)
        dist = round(1.0 + (gi % 1000) * 0.1, 2)
        et = (gi % 1440) + 10
        rows.append([did, rn, dist, et])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_routestops_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"routestops_{idx:03d}.csv"
    header = ["route_id","stop_order","address","city","country","arrival_time","departure_time","status"]
    rows = []
    for gi in range(start, end):
        rid = (gi % SIZES['deliveryroutes']) + 1
        stop_order = (gi % 100) + 1
        addr = deterministic('Addr', gi)
        city = f"City_{gi%1000}"
        country = f"Country_{gi%200}"
        at = gi_to_iso(gi)
        dt = gi_to_iso(gi+600)
        status = ['pending','completed'][gi % 2]
        rows.append([rid, stop_order, addr, city, country, at, dt, status])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_proofofdelivery_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"proofofdelivery_{idx:03d}.csv"
    header = ["stop_id","signature_url","photo_url","delivered_at"]
    rows = []
    for gi in range(start, end):
        sid = (gi % SIZES['routestops']) + 1
        sig = f"http://cdn.example/sign_{gi}.png"
        photo = f"http://cdn.example/photo_{gi}.jpg"
        rows.append([sid, sig, photo, gi_to_iso(gi)])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_purchaseorders_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"purchaseorders_{idx:03d}.csv"
    header = ["supplier_id","warehouse_id","order_date","status","total_amount"]
    rows = []
    for gi in range(start, end):
        sup = (gi % SIZES['suppliers']) + 1
        wh = (gi % SIZES['warehouses']) + 1
        total = round(100.0 + (gi % 10000) * 1.5, 2)
        status = ['pending','received'][gi % 2]
        rows.append([sup, wh, gi_to_iso(gi), status, total])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_purchaseorderitems_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"purchaseorderitems_{idx:03d}.csv"
    header = ["po_id","variant_id","quantity_ordered","unit_price"]
    rows = []
    for gi in range(start, end):
        po = (gi % SIZES['purchaseorders']) + 1
        vid = (gi % SIZES['productvariants']) + 1
        qty = (gi % 500) + 1
        price = round(1.0 + (gi % 2000) * 0.5, 2)
        rows.append([po, vid, qty, price])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_shipments_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"shipments_{idx:03d}.csv"
    header = ["po_id","shipped_date","received_date","tracking_number","status"]
    rows = []
    for gi in range(start, end):
        po = (gi % SIZES['purchaseorders']) + 1
        trk = deterministic('TRK', gi)
        status = ['in_transit','received'][gi % 2]
        rows.append([po, gi_to_iso(gi), gi_to_iso(gi+10000), trk, status])
    write_csv_rows(fname, header, rows)
    return str(fname)

def gen_qualitychecks_chunk(args):
    start, end, idx = args
    fname = OUT_DIR / f"qualitychecks_{idx:03d}.csv"
    header = ["shipment_id","checked_by","qc_date","result","remarks"]
    rows = []
    for gi in range(start, end):
        sh = (gi % SIZES['shipments']) + 1
        cb = (gi % SIZES['users']) + 1
        res = ['pass','fail'][gi % 2]
        rows.append([sh, cb, gi_to_iso(gi), res, f"QC remark {gi}"])
    write_csv_rows(fname, header, rows)
    return str(fname)

# GEN_FUNCS mapping
GEN_FUNCS = {
    'categories': gen_categories_chunk,
    'users': gen_users_chunk,
    'roles': gen_roles_chunk,
    'permissions': gen_permissions_chunk,
    'rolepermissions': gen_rolepermissions_chunk,
    'userroles': gen_userroles_chunk,
    'warehouses': gen_warehouses_chunk,
    'suppliers': gen_suppliers_chunk,
    'products': gen_products_chunk,
    'productvariants': gen_productvariants_chunk,
    'inventory': gen_inventory_chunk,
    'promotions': gen_promotions_chunk,
    'taxes': gen_taxes_chunk,
    'orders': gen_orders_chunk,
    'orderitems': gen_orderitems_chunk,
    'payments': gen_payments_chunk,
    'vehicles': gen_vehicles_chunk,
    'drivers': gen_drivers_chunk,
    'maintenancelogs': gen_maintenancelogs_chunk,
    'deliveryshipments': gen_deliveryshipments_chunk,
    'deliveryroutes': gen_deliveryroutes_chunk,
    'routestops': gen_routestops_chunk,
    'proofofdelivery': gen_proofofdelivery_chunk,
    'purchaseorders': gen_purchaseorders_chunk,
    'purchaseorderitems': gen_purchaseorderitems_chunk,
    'shipments': gen_shipments_chunk,
    'qualitychecks': gen_qualitychecks_chunk
}

# generation driver
def generate_table(table):
    total = SIZES[table]
    chunk_size = CHUNK_SIZES.get(table, 1000)
    ranges = chunk_ranges(total, chunk_size)
    args = [(r[0], r[1], r[2]) for r in ranges]
    print(f"Generating {table}: {total} rows in {len(args)} chunks (chunk_size={chunk_size})")
    with Pool(WORKERS) as p:
        results = list(tqdm(p.imap(GEN_FUNCS[table], args), total=len(args)))
    return results

# COPY SQL map
COPY_SQL = {
    'categories': "COPY categories(parent_id,category_name,description) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'users': "COPY users(username,password_hash,email,full_name,user_type,created_at) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'roles': "COPY roles(role_name,description) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'permissions': "COPY permissions(permission_name,description) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'rolepermissions': "COPY rolepermissions(role_id,permission_id) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'userroles': "COPY userroles(user_id,role_id) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'warehouses': "COPY warehouses(warehouse_name,location_country,location_city,address,capacity) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'suppliers': "COPY suppliers(company_name,contact_name,contact_email,phone,address,country) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'products': "COPY products(category_id,product_name,description,brand,created_at) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'productvariants': "COPY productvariants(product_id,sku,color,size,weight,base_price) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'inventory': "COPY inventory(warehouse_id,variant_id,quantity,last_updated) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'promotions': "COPY promotions(promo_name,discount_percent,start_date,end_date) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'taxes': "COPY taxes(region,tax_rate) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'orders': "COPY orders(user_id,warehouse_id,order_date,region,currency_code,subtotal,tax_id,promo_id,total_amount,status) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'orderitems': "COPY orderitems(order_id,variant_id,quantity,unit_price) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'payments': "COPY payments(order_id,payment_method,amount,payment_date,transaction_ref) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'vehicles': "COPY vehicles(vehicle_type,license_plate,capacity_kg,status) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'drivers': "COPY drivers(user_id,license_number,phone_number,assigned_vehicle) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'maintenancelogs': "COPY maintenancelogs(vehicle_id,maintenance_date,description,cost,performed_by) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'deliveryshipments': "COPY deliveryshipments(order_id,vehicle_id,driver_id,start_time,end_time,status) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'deliveryroutes': "COPY deliveryroutes(delivery_id,route_name,distance_km,estimated_time_min) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'routestops': "COPY routestops(route_id,stop_order,address,city,country,arrival_time,departure_time,status) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'proofofdelivery': "COPY proofofdelivery(stop_id,signature_url,photo_url,delivered_at) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'purchaseorders': "COPY purchaseorders(supplier_id,warehouse_id,order_date,status,total_amount) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'purchaseorderitems': "COPY purchaseorderitems(po_id,variant_id,quantity_ordered,unit_price) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'shipments': "COPY shipments(po_id,shipped_date,received_date,tracking_number,status) FROM STDIN WITH CSV HEADER DELIMITER ','",
    'qualitychecks': "COPY qualitychecks(shipment_id,checked_by,qc_date,result,remarks) FROM STDIN WITH CSV HEADER DELIMITER ','"
}

def copy_file_to_db(fname, table):
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
    cur = conn.cursor()
    with open(fname, 'r', encoding='utf-8') as f:
        cur.copy_expert(COPY_SQL[table], f)
    conn.commit()
    cur.close()
    conn.close()
    try:
        os.remove(fname)
    except Exception:
        pass
    return fname

def parallel_copy(table, files):
    print(f"Starting COPY for {table} ({len(files)} files) with {COPY_WORKERS} workers")
    with ThreadPoolExecutor(max_workers=COPY_WORKERS) as ex:
        list(tqdm(ex.map(lambda fn: copy_file_to_db(fn, table), files), total=len(files)))
    print(f"COPY finished for {table}")

def truncate_all_tables():
    """Truncate all tables in dependency-reverse order to clear existing data"""
    print("=== TRUNCATING EXISTING DATA ===")
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
    cur = conn.cursor()
    
    # Truncate in reverse dependency order
    tables = [
        "qualitychecks", "shipments", "purchaseorderitems", "purchaseorders",
        "proofofdelivery", "routestops", "deliveryroutes", "deliveryshipments",
        "maintenancelogs", "drivers", "vehicles",
        "payments", "orderitems", "orders",
        "inventory", "productvariants", "products",
        "taxes", "promotions",
        "suppliers", "warehouses",
        "userroles", "rolepermissions", "permissions", "roles",
        "users", "categories"
    ]
    
    for table in tables:
        try:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
            print(f"Truncated {table}")
        except Exception as e:
            print(f"Warning truncating {table}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    print("Truncation complete\n")

def main():
    import time
    t0 = time.time()
    truncate_all_tables()
    generated = {}
    tables_order = [
        'categories','users','roles','permissions','rolepermissions','userroles',
        'warehouses','suppliers','products','productvariants','inventory',
        'promotions','taxes','orders','payments','orderitems',
        'vehicles','drivers','maintenancelogs',
        'deliveryshipments','deliveryroutes','routestops','proofofdelivery',
        'purchaseorders','purchaseorderitems','shipments','qualitychecks'
    ]
    print("=== GENERATION PHASE ===")
    for table in tables_order:
        generated[table] = generate_table(table)
    print("=== COPY PHASE ===")
    for table in tables_order:
        files = generated[table]
        parallel_copy(table, files)
    dt = time.time() - t0
    print(f"All done in {dt:.2f} seconds. Chunk dir: {OUT_DIR.resolve()}")
    print("Finished.")

if __name__ == '__main__':
    main()
