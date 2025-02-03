import os
import pandas as pd
import pyodbc
class DataProcessor:
    def __init__(self, input_file="data/interim/OrderWithReturns.csv", output_dir="data/processed", delimiter="|"):
        self.input_file = input_file
        self.output_dir = output_dir
        self.delimiter = delimiter
        self.df = self.load_data()

        self.db_connection_string = (
            "Driver={ODBC Driver 18 for SQL Server};"
                    "Server=OMEGA-PC-10498;"  
                    "Database=Orders_Teodora;"     
                    "Trusted_Connection=yes;"
                    "Encrypt=no;"
                    "TrustServerCertificate=yes;")
        
        self.column_mappings = {
            "Customer":{
                "Customer ID": "RowID",
                "Customer Name": "CustomerName",
                "Customer Segment": "Segment"
            },
            "Locations":{
                "Postal Code": "PostalCode",
                "Country": "Country",
                "State or Province": "StateOrProvince",
                "City": "City"
            },
            "Orders":{
                "Row ID": "RowID",
                "Order ID":"OrderID",
                "Customer ID": "id_customer",
                "Product ID": "id_product",
                "Region ID": "id_region",
                "Postal Code":"id_location",
                "Product Base Margin": "product_base_margin",
                "Unit Price":"unit_price",
                "Shipping Cost": "shipping_cost",
                "Ship Mode":"ship_mode",
                "Order Date": "order_date",
                "Ship Date": "shipping_date",
                "Discount":"discount",
                "Order Priority": "order_priority",
                "Profit":"profit",
                "Quantity ordered new":"quantity",
                "Status":"returned",
                "Sales": "sales",
            
            },
            "Products":{
                "Product ID": "RowID",
                "Product Name": "ProductName",
                "Product Container": "ProductContainer",
                "Subcategory ID": "Subcategory"
            }
            
        }
    def load_data(self):
        """Loads the merged CSV file into a DataFrame."""
        if not os.path.exists(self.input_file):
            raise FileNotFoundError(f"❌ Input file not found: {self.input_file}")

        try:
            df = pd.read_csv(self.input_file, delimiter=self.delimiter, dtype=str, na_values=[' '])
            print(f"✅ Successfully loaded data from {self.input_file}. Shape: {df.shape}")
            
            df.replace(r'^\s*$', None, regex=True, inplace=True)
            
            return df
        except Exception as e:
            raise Exception(f"❌ Error loading data: {e}")

    def extract_customers(self):
        """Extracts customer-related data and saves it."""
        customer_columns = ["Customer ID", "Customer Name", "Customer Segment"]
        self._extract_and_save(customer_columns, "customers.csv", unique_columns=["Customer ID"])

    def extract_category(self):
        """Extracts category-related data and saves it."""
        category_columns = ["Category ID", "Product Category"]
        self._extract_and_save(category_columns, "category.csv", unique_columns=["Category ID"])
        
    def extract_subcategory(self):
        """Extracts subcategory-related data and saves it."""
        subcategory_columns = ["Subcategory ID", "Category ID" ,"Product Sub-Category"]
        self._extract_and_save(subcategory_columns, "subcategory.csv", unique_columns=["Subcategory ID"])
    
    def extract_products(self):
        """Extracts product-related data and saves it."""
        product_columns = ["Product ID", "Subcategory ID", "Product Name", "Product Container"]
        self._extract_and_save(product_columns, "products.csv", unique_columns=["Product ID"])

    def extract_locations(self):
        """Extracts location-related data and saves it."""
        location_columns = ["Country", "State or Province", "City", "Postal Code"]
        self._extract_and_save(location_columns, "locations.csv", unique_columns=["Postal Code"])
    
    def extract_orders(self):
        """Extracts orders-related data and saves it."""
        orders_columns = ["Row ID","Order ID", "Customer ID", "Product ID", "Postal Code", "Region ID", "Order Priority", "Discount","Unit Price","Shipping Cost","Ship Mode","Product Base Margin","Order Date","Ship Date","Profit","Quantity ordered new","Sales", "Status"]
        self._extract_and_save(orders_columns, "orders.csv", unique_columns=["Row ID"])

    def _extract_and_save(self, columns, output_filename, unique_columns):
        """Extracts specified columns, removes duplicates, and saves the data."""
        extracted_df = self.df[columns].drop_duplicates(subset=unique_columns)
        output_path = os.path.join(self.output_dir, output_filename)
        os.makedirs(self.output_dir, exist_ok=True)
        extracted_df.to_csv(output_path, index=False, sep=self.delimiter)
        print(f"✅ Extracted and saved to {output_path}")
    
    def import_to_database(self, table_name, csv_file):
        """Imports the extracted CSV data into the specified database table."""
        # Read the CSV file to import into the database
        data_df = pd.read_csv(csv_file, delimiter=self.delimiter, dtype=str, na_values=[' '])
        data_df.replace(r'^\s*$', None, regex=True, inplace=True)
        
        data_df = data_df.where(pd.notnull(data_df), None)

        # Get the column mapping for the current table
        if table_name not in self.column_mappings:
            print(f"❌ No column mapping found for table {table_name}")
            return
        
        column_mapping = self.column_mappings[table_name]
        
        # Apply the column mapping to rename the DataFrame columns
        data_df = data_df.rename(columns=column_mapping)

        # Establish database connection
        conn = pyodbc.connect(self.db_connection_string)
        cursor = conn.cursor()

        # Prepare insert statement based on columns of the DataFrame
        columns = ", ".join(data_df.columns)
        placeholders = ", ".join(["?" for _ in data_df.columns])

        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        try:
            # Insert data into the table
            for index, row in data_df.iterrows():
                cursor.execute(insert_query, tuple(row))
            conn.commit()
            print(f"✅ Successfully imported data into {table_name}")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error importing data into {table_name}: {e}")
        finally:
            cursor.close()
            conn.close()
            print(f"Database connection closed")
            
if __name__ == "__main__":
    processor = DataProcessor()
    processor.extract_customers()
    processor.extract_category()
    processor.extract_subcategory()
    processor.extract_products()    
    processor.extract_locations()
    processor.extract_orders()   
# Import data to the database 
    processor.import_to_database("Customer", "data/processed/customers.csv")
    #processor.import_to_database("Category", "data/processed/category.csv")
    #processor.import_to_database("Subcategory", "data/processed/subcategory.csv")
    processor.import_to_database("Products", "data/processed/products.csv")
    processor.import_to_database("Locations", "data/processed/locations.csv")
    processor.import_to_database("Orders", "data/processed/orders.csv")
    #processor.import_to_database("Returned", "data/processed/Returns.csv")