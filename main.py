import pandas as pd
import os
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

# Supabase


url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

COLUMNS = ["Doc. Date", "FORETV\n Doc. #", "Business Partner", "Agency", "Sales \nExec.", "Product", "Order", "Total Foreing Currency"]
FOLDER = "invoice/"

def get_invoice_data(file):
    try:
        df = pd.read_excel(FOLDER+file, sheet_name="Sheet1")
        return df
    except Exception as e:
        print(e)

def filter_columns(df, columns):
    try:
        df = df[columns]
        return df
    except Exception as e:
        print(e)

def get_csv(df, file):
    try:
        df.to_csv(FOLDER+file+".csv", index=False)
    except Exception as e:
        print(e)

def upload_file(file):
    print(supabase)
    print('///')
    try:
        
        # res = supabase.storage.create_bucket('invoice')

        res = supabase.storage.get_bucket('invoice-dev')

        res = supabase.storage.from_('invoice-dev').upload('/invoices/invoice.csv', FOLDER+file+".csv")
        print(res)
    except Exception as e:
        print(e)
    
    


def main():
    invoice_data = get_invoice_data("invoice.xls")
  

    invoice_data = filter_columns(invoice_data, COLUMNS)
    invoice_data = invoice_data.dropna()

    get_csv(invoice_data, "invoice")
    upload_file("invoice")
    



if __name__ == "__main__":
    # load_dotenv()
    main()
    