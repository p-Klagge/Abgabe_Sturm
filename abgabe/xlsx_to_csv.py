import pandas as pd
from pathlib import Path

path = Path().absolute()
excel_path = path / 'data/Online Retail.xlsx'
csv_path = path / 'data/Online_Retail.csv'
data = pd.read_excel(excel_path)
data.to_csv(csv_path, index=True)