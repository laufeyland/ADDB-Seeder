# OmniShip Data Generation

## 1. Install Requirements
```bash
python -m venv venv
.\venv\scripts\activate
pip install -r requirements.txt
```

## 2. Prepare PostgreSQL
Run your schema:
```bash
psql -U postgres -d omniship -f SQLOmniship.sql
```

Optionally disable triggers for faster loading:
```sql
ALTER TABLE users DISABLE TRIGGER ALL;
ALTER TABLE products DISABLE TRIGGER ALL;
ALTER TABLE productvariants DISABLE TRIGGER ALL;
ALTER TABLE orders DISABLE TRIGGER ALL;
ALTER TABLE orderitems DISABLE TRIGGER ALL;
ALTER TABLE routestops DISABLE TRIGGER ALL;
ALTER TABLE maintenancelogs DISABLE TRIGGER ALL;
```

## 3. Run the Data Generator
```bash
python generate_db.py
```
This generates CSV chunks and loads them into PostgreSQL using parallel COPY.

## 4. Re-Enable Triggers After Loading
```sql
ALTER TABLE users ENABLE TRIGGER ALL;
ALTER TABLE products ENABLE TRIGGER ALL;
ALTER TABLE productvariants ENABLE TRIGGER ALL;
ALTER TABLE orders ENABLE TRIGGER ALL;
ALTER TABLE orderitems ENABLE TRIGGER ALL;
ALTER TABLE routestops ENABLE TRIGGER ALL;
ALTER TABLE maintenancelogs ENABLE TRIGGER ALL;
```
