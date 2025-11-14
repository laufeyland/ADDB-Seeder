-- TABLE: categories
CREATE TABLE categories (
  category_id BIGSERIAL PRIMARY KEY,
  parent_id BIGINT NULL,
  category_name VARCHAR(150) NOT NULL,
  description TEXT,
  CONSTRAINT fk_categories_parent
    FOREIGN KEY (parent_id) REFERENCES categories(category_id) 
      ON DELETE SET NULL ON UPDATE CASCADE
);

-- TABLE: users
CREATE TABLE users (
  user_id BIGSERIAL PRIMARY KEY,
  username VARCHAR(100) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  email VARCHAR(150) NOT NULL UNIQUE,
  full_name VARCHAR(150),
  user_type VARCHAR(50) DEFAULT 'customer',  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TABLE: roles and permissions 
CREATE TABLE roles (
  role_id SERIAL PRIMARY KEY,
  role_name VARCHAR(100) UNIQUE NOT NULL,
  description TEXT
);

CREATE TABLE permissions (
  permission_id SERIAL PRIMARY KEY,
  permission_name VARCHAR(100) UNIQUE NOT NULL,
  description TEXT
);

CREATE TABLE rolepermissions (
  role_id INT NOT NULL,
  permission_id INT NOT NULL,
  PRIMARY KEY (role_id, permission_id),
  FOREIGN KEY (role_id) REFERENCES roles(role_id) ON DELETE CASCADE,
  FOREIGN KEY (permission_id) REFERENCES permissions(permission_id) ON DELETE CASCADE
);

CREATE TABLE userroles (
  user_id BIGINT NOT NULL,
  role_id INT NOT NULL,
  PRIMARY KEY (user_id, role_id),
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
  FOREIGN KEY (role_id) REFERENCES roles(role_id) ON DELETE CASCADE
);

-- TABLE: warehouses
CREATE TABLE warehouses (
  warehouse_id SERIAL PRIMARY KEY,
  warehouse_name VARCHAR(150) NOT NULL,
  location_country VARCHAR(100),
  location_city VARCHAR(100),
  address VARCHAR(255),
  capacity INT DEFAULT 0
);

-- TABLE: suppliers
CREATE TABLE suppliers (
  supplier_id BIGSERIAL PRIMARY KEY,
  company_name VARCHAR(150) NOT NULL,
  contact_name VARCHAR(150),
  contact_email VARCHAR(150),
  phone VARCHAR(50),
  address VARCHAR(255),
  country VARCHAR(100)
);

-- TABLE: products and variants
CREATE TABLE products (
  product_id BIGSERIAL PRIMARY KEY,
  category_id BIGINT,
  product_name VARCHAR(200) NOT NULL,
  description TEXT,
  brand VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE SET NULL
);

CREATE TABLE productvariants (
  variant_id BIGSERIAL PRIMARY KEY,
  product_id BIGINT,
  sku VARCHAR(100) UNIQUE,
  color VARCHAR(50),
  size VARCHAR(50),
  weight DECIMAL(10,2),
  base_price DECIMAL(10,2),
  FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

-- TABLE: inventory
CREATE TABLE inventory (
  inventory_id BIGSERIAL PRIMARY KEY,
  warehouse_id INT,
  variant_id BIGINT,
  quantity INT DEFAULT 0,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id) ON DELETE CASCADE,
  FOREIGN KEY (variant_id) REFERENCES productvariants(variant_id) ON DELETE CASCADE
);

-- TABLE: promotions and taxes
CREATE TABLE promotions (
  promo_id SERIAL PRIMARY KEY,
  promo_name VARCHAR(150),
  discount_percent DECIMAL(5,2) DEFAULT 0.00,
  start_date DATE,
  end_date DATE
);

CREATE TABLE taxes (
  tax_id SERIAL PRIMARY KEY,
  region VARCHAR(100),
  tax_rate DECIMAL(5,2) DEFAULT 0.00
);

-- TABLE: orders and items
CREATE TABLE orders (
  order_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT,
  warehouse_id INT,
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  region VARCHAR(100),
  currency_code CHAR(3) DEFAULT 'USD',
  subtotal DECIMAL(12,2),
  tax_id INT,
  promo_id INT,
  total_amount DECIMAL(12,2),
  status VARCHAR(50) DEFAULT 'pending',  
  FOREIGN KEY (user_id) REFERENCES users(user_id),
  FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id),
  FOREIGN KEY (tax_id) REFERENCES taxes(tax_id),
  FOREIGN KEY (promo_id) REFERENCES promotions(promo_id)
);

CREATE TABLE orderitems (
  order_item_id BIGSERIAL PRIMARY KEY,
  order_id BIGINT,
  variant_id BIGINT,
  quantity INT,
  unit_price DECIMAL(10,2),
  FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
  FOREIGN KEY (variant_id) REFERENCES productvariants(variant_id) ON DELETE SET NULL
);

-- TABLE: payments
CREATE TABLE payments (
  payment_id BIGSERIAL PRIMARY KEY,
  order_id BIGINT,
  payment_method VARCHAR(50) DEFAULT 'card',  
  amount DECIMAL(12,2),
  payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  transaction_ref VARCHAR(150),
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- TABLE: drivers, vehicles and maintenance
CREATE TABLE vehicles (
  vehicle_id SERIAL PRIMARY KEY,
  vehicle_type VARCHAR(50),
  license_plate VARCHAR(50) UNIQUE,
  capacity_kg DECIMAL(10,2),
  status VARCHAR(50) DEFAULT 'available'  
);
CREATE TABLE drivers (
  driver_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT,
  license_number VARCHAR(100),
  phone_number VARCHAR(50),
  assigned_vehicle INT,
  FOREIGN KEY (user_id) REFERENCES users(user_id),
  FOREIGN KEY (assigned_vehicle) REFERENCES vehicles(vehicle_id)
);

CREATE TABLE maintenancelogs (
  log_id BIGSERIAL PRIMARY KEY,
  vehicle_id INT,
  maintenance_date TIMESTAMP,
  description TEXT,
  cost DECIMAL(10,2),
  performed_by BIGINT,
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id),
  FOREIGN KEY (performed_by) REFERENCES users(user_id)
);

-- TABLE: deliveries and routes
CREATE TABLE deliveryshipments (
  delivery_id BIGSERIAL PRIMARY KEY,
  order_id BIGINT,
  vehicle_id INT,
  driver_id BIGINT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status VARCHAR(50) DEFAULT 'pending',  
  FOREIGN KEY (order_id) REFERENCES orders(order_id),
  FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id),
  FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

CREATE TABLE deliveryroutes (
  route_id BIGSERIAL PRIMARY KEY,
  delivery_id BIGINT,
  route_name VARCHAR(150),
  distance_km DECIMAL(8,2),
  estimated_time_min INT,
  FOREIGN KEY (delivery_id) REFERENCES deliveryshipments(delivery_id)
);

CREATE TABLE routestops (
  stop_id BIGSERIAL PRIMARY KEY,
  route_id BIGINT,
  stop_order INT,
  address VARCHAR(255),
  city VARCHAR(100),
  country VARCHAR(100),
  arrival_time TIMESTAMP,
  departure_time TIMESTAMP,
  status VARCHAR(50) DEFAULT 'pending',  
  FOREIGN KEY (route_id) REFERENCES deliveryroutes(route_id)
);

CREATE TABLE proofofdelivery (
  pod_id BIGSERIAL PRIMARY KEY,
  stop_id BIGINT,
  signature_url VARCHAR(255),
  photo_url VARCHAR(255),
  delivered_at TIMESTAMP,
  FOREIGN KEY (stop_id) REFERENCES routestops(stop_id)
);

-- 
-- TABLE: purchasing and quality control
CREATE TABLE purchaseorders (
  po_id BIGSERIAL PRIMARY KEY,
  supplier_id BIGINT,
  warehouse_id INT,
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(50) DEFAULT 'pending',  
  total_amount DECIMAL(12,2),
  FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
  FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);

CREATE TABLE purchaseorderitems (
  poi_id BIGSERIAL PRIMARY KEY,
  po_id BIGINT,
  variant_id BIGINT,
  quantity_ordered INT,
  unit_price DECIMAL(10,2),
  FOREIGN KEY (po_id) REFERENCES purchaseorders(po_id),
  FOREIGN KEY (variant_id) REFERENCES productvariants(variant_id)
);

CREATE TABLE shipments (
  shipment_id BIGSERIAL PRIMARY KEY,
  po_id BIGINT,
  shipped_date TIMESTAMP,
  received_date TIMESTAMP,
  tracking_number VARCHAR(100),
  status VARCHAR(50) DEFAULT 'in_transit',  
  FOREIGN KEY (po_id) REFERENCES purchaseorders(po_id)
);

CREATE TABLE qualitychecks (
  qc_id BIGSERIAL PRIMARY KEY,
  shipment_id BIGINT,
  checked_by BIGINT,
  qc_date TIMESTAMP,
  result VARCHAR(50) DEFAULT 'pending',  
  remarks TEXT,
  FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id),
  FOREIGN KEY (checked_by) REFERENCES users(user_id)
);
