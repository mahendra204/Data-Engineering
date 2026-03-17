"""
Sample Data Generator for E-Commerce Pipeline
Generates realistic sample data for testing pipelines
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path
from typing import Dict


class SampleDataGenerator:
    """Generate realistic e-commerce sample data."""
    
    def __init__(self, num_customers: int = 1000, num_products: int = 500, 
                 num_orders: int = 5000, seed: int = 42):
        """Initialize data generator."""
        np.random.seed(seed)
        random.seed(seed)
        
        self.num_customers = num_customers
        self.num_products = num_products
        self.num_orders = num_orders
        
        self.first_names = [
            'John', 'Mary', 'James', 'Patricia', 'Robert', 'Jennifer',
            'Michael', 'Linda', 'William', 'Barbara', 'David', 'Elizabeth',
            'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah',
            'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa'
        ]
        
        self.last_names = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
            'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez',
            'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore',
            'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White'
        ]
        
        self.cities = [
            'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
            'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
            'Austin', 'Jacksonville', 'Seattle', 'Denver', 'Boston'
        ]
        
        self.countries = [
            'USA', 'UK', 'Canada', 'Australia', 'India', 'Germany',
            'France', 'Japan', 'Mexico', 'Brazil'
        ]
        
        self.categories = [
            'Electronics', 'Clothing', 'Home & Garden', 'Sports',
            'Books', 'Toys', 'Food & Beverage', 'Beauty', 'Furniture'
        ]
        
        self.product_names = {
            'Electronics': [
                'Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smart Watch',
                'Camera', 'Monitor', 'Keyboard', 'Mouse', 'Charger'
            ],
            'Clothing': [
                'T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Shoes',
                'Sweater', 'Shorts', 'Socks', 'Hat', 'Scarf'
            ],
            'Home & Garden': [
                'Pillow', 'Blanket', 'Lamp', 'Rug', 'Plant',
                'Picture Frame', 'Curtains', 'Bedsheet', 'Towel', 'Mirror'
            ],
            'Sports': [
                'Basketball', 'Soccer Ball', 'Tennis Racket', 'Yoga Mat',
                'Dumbbells', 'Running Shoes', 'Bicycle', 'Skateboard', 'Helmet', 'Gloves'
            ],
            'Books': [
                'Fiction Novel', 'Mystery Book', 'Self-Help Book', 'Biography',
                'Science Fiction', 'History Book', 'Cookbook', 'Travel Guide'
            ]
        }
        
        self.statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
    
    def generate_customers(self) -> pd.DataFrame:
        """Generate customer data."""
        customers = []
        registration_start = datetime.now() - timedelta(days=730)
        
        for i in range(1, self.num_customers + 1):
            customer = {
                'customer_id': i,
                'first_name': random.choice(self.first_names),
                'last_name': random.choice(self.last_names),
                'email': f'customer{i}@example.com',
                'phone': f'+1{random.randint(2000000000, 9999999999)}',
                'city': random.choice(self.cities),
                'country': random.choice(self.countries),
                'registration_date': (registration_start + 
                                     timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')
            }
            customers.append(customer)
        
        return pd.DataFrame(customers)
    
    def generate_products(self) -> pd.DataFrame:
        """Generate product data."""
        products = []
        product_id = 1
        created_start = datetime.now() - timedelta(days=365)
        
        for category in self.categories:
            category_products = self.product_names.get(category, ['Product'])
            
            for product_name in category_products:
                for variant in range(1, self.num_products // len(self.categories) // 
                                    len(category_products) + 2):
                    product = {
                        'product_id': product_id,
                        'product_name': f'{product_name} - Variant {variant}',
                        'category': category,
                        'price': round(np.random.uniform(10, 1000), 2),
                        'stock_quantity': random.randint(0, 1000),
                        'supplier_id': random.randint(1, 50),
                        'created_date': (created_start + 
                                       timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
                    }
                    products.append(product)
                    product_id += 1
                    
                    if product_id > self.num_products:
                        break
                
                if product_id > self.num_products:
                    break
            
            if product_id > self.num_products:
                break
        
        return pd.DataFrame(products).head(self.num_products)
    
    def generate_orders(self, customers_df: pd.DataFrame, 
                       products_df: pd.DataFrame) -> pd.DataFrame:
        """Generate order data."""
        orders = []
        order_id = 1
        order_start = datetime.now() - timedelta(days=180)
        
        for _ in range(self.num_orders):
            customer_id = random.choice(customers_df['customer_id'].values)
            product_id = random.choice(products_df['product_id'].values)
            quantity = random.randint(1, 10)
            
            # Get product price
            product_price = float(
                products_df[products_df['product_id'] == product_id]['price'].iloc[0]
            )
            
            order_amount = round(product_price * quantity * 
                               random.uniform(0.8, 1.2), 2)  # Add some variance
            
            order = {
                'order_id': order_id,
                'customer_id': customer_id,
                'product_id': product_id,
                'order_date': (order_start + 
                             timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d'),
                'order_amount': order_amount,
                'quantity': quantity,
                'status': random.choice(self.statuses)
            }
            orders.append(order)
            order_id += 1
        
        return pd.DataFrame(orders)
    
    def generate_all(self, output_dir: str = ".\\data") -> Dict[str, pd.DataFrame]:
        """Generate all data and save to CSV."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        print("Generating sample data...")
        print(f"  - Customers: {self.num_customers}")
        
        customers_df = self.generate_customers()
        customers_df.to_csv(output_path / 'customers.csv', index=False)
        print(f"    ✓ Saved to {output_path / 'customers.csv'}")
        
        print(f"  - Products: {self.num_products}")
        products_df = self.generate_products()
        products_df.to_csv(output_path / 'products.csv', index=False)
        print(f"    ✓ Saved to {output_path / 'products.csv'}")
        
        print(f"  - Orders: {self.num_orders}")
        orders_df = self.generate_orders(customers_df, products_df)
        orders_df.to_csv(output_path / 'orders.csv', index=False)
        print(f"    ✓ Saved to {output_path / 'orders.csv'}")
        
        return {
            'customers': customers_df,
            'products': products_df,
            'orders': orders_df
        }


def main():
    """Main entry point."""
    generator = SampleDataGenerator(
        num_customers=1000,
        num_products=500,
        num_orders=5000
    )
    
    data = generator.generate_all()
    
    print("\n" + "=" * 60)
    print("Data Generation Summary")
    print("=" * 60)
    print(f"Customers: {len(data['customers'])} records")
    print(f"Products: {len(data['products'])} records")
    print(f"Orders: {len(data['orders'])} records")
    print("\nSample data generated successfully!")


if __name__ == "__main__":
    main()
