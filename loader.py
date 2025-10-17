import kagglehub
import pandas as pd
import hashlib


#path = kagglehub.dataset_download("roopacalistus/superstore")
#print("Path to dataset files:", path)
path = "SampleSuperstore.csv"


def _normalize_series(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
         .str.strip()
         .str.upper()
    )


def make_bk_hash(df: pd.DataFrame, cols, algo: str = "sha256") -> pd.Series:
    norm = df[cols].apply(_normalize_series).fillna("")
    concat = norm.agg("|".join, axis=1)
    h = getattr(hashlib, algo)
    return concat.map(lambda x: h(x.encode("utf-8")).hexdigest())


df = pd.read_csv(path)
df['Sale_HK'] = make_bk_hash(df, ["City", "State", "Postal Code"])
df['Customer_HK'] = make_bk_hash(df, ['Segment', 'City', 'State', 'Postal Code'])
df['Product_HK'] = make_bk_hash(df, ['Category', 'Sub-Category'])
df['Shipment_ID'] = make_bk_hash(df, ['Customer_HK', 'Sale_HK', 'Ship Mode'])
print(df.head())
df.to_csv('buisness_data.csv', index=False)

