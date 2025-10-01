from processing import process_data
from config import engine
import pandas as pd

try:
    df = pd.read_csv(r"D:\Natalia\Trading_quotation_collection\input_data\profit\WINFUT_F_0_Trade_17-09-2025.csv")
except:
    df = pd.read_csv(r"D:\Natalia\Trading_quotation_collection\input_data\profit\WINFUT_F_0_Trade_17-09-2025.csv", sep=";", encoding="latin1")    

is_updated = process_data.is_new_data_updated(df, "all_time_ticks")

print(is_updated)