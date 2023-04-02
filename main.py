import pandas as pd
import os
import asyncio
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

# Supabase


url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

COLUMNS = ["Doc. Date", "FORETV\n Doc. #", "Business Partner", "Agency", "Sales \nExec.", "Product", "Order", "Total Foreing Currency"]
FOLDER = "invoice/"

CHANNEL = "A&E"
TIPO_VENTA = "ON AIR"

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
    try:
       supabase.storage.from_('invoice-dev').upload('/invoices/invoice.csv', FOLDER+file+".csv")
    except Exception as e:
        print(e)
    
def add_channel(df, channel):
    try:
        df["Channel"] = channel
        return df
    except Exception as e:
        print(e)

def add_tipoVenta(df, tipoVenta):
    try:
        df["Tipo Venta"] = tipoVenta
        return df
    except Exception as e:
        print(e)


async def invoke_function(loop):
    try:
        res = await supabase.functions().invoke('invoice_ETL', invoke_options={'body':{}})
        
        print(res)
        return res
    except Exception as e:
        print(e)

def main():
    invoice_data = get_invoice_data("invoice.xls")
  

    invoice_data = filter_columns(invoice_data, COLUMNS)
    invoice_data = invoice_data.dropna()

    invoice_data = add_channel(invoice_data, CHANNEL)
    invoice_data = add_tipoVenta(invoice_data, TIPO_VENTA)

    get_csv(invoice_data, "invoice")
    upload_file("invoice")

    loop = asyncio.get_event_loop()
    resp = loop.run_until_complete(invoke_function(loop))
    loop.close()
    



if __name__ == "__main__":
    # load_dotenv()
    main()
    