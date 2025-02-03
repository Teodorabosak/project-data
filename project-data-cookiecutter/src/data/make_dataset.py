# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

#MERGE DATA FILES
import os
import pandas as pd

def merge_raw_csv_files(input_dir="data/raw", output_file="data/interim/merged.csv", delimiter="|", id_column="Row ID"):
    """Merge all CSV files from input_dir into a single file, remove duplicate row_id, and save to output_file."""
    
    all_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    if not all_files:
        print("❌ No CSV files found in", input_dir)
        return
    
    df_list = []
    
    for file in all_files:
        file_path = os.path.join(input_dir, file)
        try:
            df = pd.read_csv(file_path, delimiter=delimiter, dtype=str)  # Čitamo sve kao stringove da izbegnemo greške
            df_list.append(df)
            print(f"✅ Successfully loaded: {file}")
        except Exception as e:
            print(f"❌ Error reading {file}: {e}")

    merged_df = pd.concat(df_list, ignore_index=True)
    
    
    if id_column in merged_df.columns:
        merged_df.drop_duplicates(subset=[id_column], inplace=True)

     # Add Region ID column based on the Region name
    region_mapping = {
        "Central": 1,
        "East": 2,
        "South": 3,
        "West": 4
    }
    category_mapping = {
        "Furniture": 1,
        "Office Supplies": 2,
        "Technology":3
    }
    
    subcategory_mapping = {
        "Office Furnishings": 1,
        "Chairs & Chairmats": 2,
        "Bookcases": 3,
        "Tables": 4,
        "Paper": 5,
        "Rubber Bands": 6,
        "Envelopes": 7,
        "Scissors, Rulers and Trimmers": 8,
        "Binders and Binder Accessories": 9,
        "Labels": 10,
        "Storage & Organization": 11,
        "Computer Peripherals": 12,
        "Telephones and Communication": 13,
        "Office Machines": 14,
        "Copiers and Fax": 15,
        "Appliances": 16,
        "Pens & Art Supplies": 17 
    }
    
    merged_df['Region ID'] = merged_df['Region'].map(region_mapping)
    merged_df['Category ID'] = merged_df['Product Category'].map(category_mapping)
    merged_df['Subcategory ID'] = merged_df['Product Sub-Category'].map(subcategory_mapping)
    merged_df['Order ID']
    merged_df['Product ID'] = pd.factorize(merged_df['Product Name'])[0]

    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    merged_df.to_csv(output_file, index=False, sep=delimiter)

    print(f"✅ Merged {len(all_files)} CSV files into {output_file}. Total rows: {len(merged_df)}")


#########################################################################

def merge_return(orders, returns, output, delimiter='|'):
    
    orders = r'C:\Users\teodora.bosak\Desktop\project-data\project-data-cookiecutter\data\interim\merged.csv'
    returns = r'C:\Users\teodora.bosak\Desktop\project-data\project-data-cookiecutter\data\interim\Returns.csv'
    output = r'C:\Users\teodora.bosak\Desktop\project-data\project-data-cookiecutter\data\interim\OrderWithReturns.csv'
    
    try: 
        df_orders =pd.read_csv(orders, sep=delimiter)
        df_returns= pd.read_csv(returns, sep=delimiter)
        
        merged_df = pd.merge(df_orders,df_returns, on='Order ID', how='left')
        merged_df['Status']=merged_df["Status"].fillna('')
        
        os.makedirs(os.path.dirname(output), exist_ok=True)
        merged_df.to_csv(output, sep=delimiter, index=False)
        
        print(f'✅ Merged data saved to {output}. Total rows: {len(merged_df)}')
    except Exception as e:
        print(f'❌ Error: {e}')


if __name__ == "__main__":
    merge_raw_csv_files()  # Prvo spajanje 
    
    # dodavanje return informacija
    merge_return(
        orders="C:\\Users\\teodora.bosak\\Desktop\\project-data\\project-data-cookiecutter\\data\\interim\\merged.csv",
        returns="C:\\Users\\teodora.bosak\\Desktop\\project-data\\project-data-cookiecutter\\data\\interim\\Returns.csv",
        output="C:\\Users\\teodora.bosak\\Desktop\\project-data\\project-data-cookiecutter\\data\\interim\\OrderWithReturns.csv"
    )
    

@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
