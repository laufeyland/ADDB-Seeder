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
SET session_replication_role = 'replica';
```

## 3. Run the Data Generator
```bash
python generate_db.py
```
This generates CSV chunks and loads them into PostgreSQL using parallel COPY.

## 4. Re-Enable Triggers After Loading
```sql
SET session_replication_role = 'origin';
```
