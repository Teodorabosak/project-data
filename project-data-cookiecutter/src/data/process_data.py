import os
import pandas as pd

class DataProcessor:
    def __init__(self, input_file="data/interim/merged.csv", output_dir="data/processed", delimiter="|"):
        self.input_file = input_file
        self.output_dir = output_dir
        self.delimiter = delimiter
        self.df = self.load_data()

    def load_data(self):
        """Loads the merged CSV file into a DataFrame."""
        if not os.path.exists(self.input_file):
            raise FileNotFoundError(f"❌ Input file not found: {self.input_file}")

        try:
            df = pd.read_csv(self.input_file, delimiter=self.delimiter, dtype=str)
            print(f"✅ Successfully loaded data from {self.input_file}. Shape: {df.shape}")
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
        subcategory_columns = ["Subcategory ID", "Product Sub-Category"]
        self._extract_and_save(subcategory_columns, "subcategory.csv", unique_columns=["Subcategory ID"])
    
    def extract_products(self):
        """Extracts product-related data and saves it."""
        product_columns = ["Product ID", "Subcategory ID", "Product Name", "Product Container"]
        self._extract_and_save(product_columns, "products.csv", unique_columns=["Customer ID"])

    def extract_locations(self):
        """Extracts location-related data and saves it."""
        location_columns = ["Country", "State or Province", "City", "Postal Code"]
        self._extract_and_save(location_columns, "locations.csv", unique_columns=["Postal Code"])
    
    def extract_orders(self):
        """Extracts orders-related data and saves it."""
        orders_columns = ["Order ID", "Customer ID", "Product ID", "Postal Code", "Region ID", "Order Priority", "Discount","Unit Price","Shipping Cost","Ship Mode","Product Base Margin","Order Date","Ship Date","Profit","Quantity ordered new","Sales"]
        self._extract_and_save(orders_columns, "orders.csv", unique_columns=["Order ID"])

    def _extract_and_save(self, columns, output_filename):
        
        

        output_path = os.path.join(self.output_dir, output_filename)
        os.makedirs(self.output_dir, exist_ok=True)
        extracted_df.to_csv(output_path, index=False, sep=self.delimiter)

        print(f"✅ Extracted and saved to {output_path}")

if __name__ == "__main__":
    processor = DataProcessor()
    processor.extract_customers()
    processor.extract_category()
    processor.extract_subcategory()
    processor.extract_products()    
    processor.extract_locations()
    processor.extract_locations()   
