import pandas as pd
from sqlalchemy import create_engine
import hashlib


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


engine = create_engine(
    'postgresql+psycopg2://student30:dn2in8aapq92l018jfdh@rc1a-q1h1bect4ltivtlp.mdb.yandexcloud.net,rc1a-st22qvh84iqkso52.mdb.yandexcloud.net:6432/hse',
    connect_args={
        'application_name': 'etl',
        'connect_timeout': 20
    },
    pool_pre_ping=True
)

df = pd.read_csv('buisness_data.csv')

# H_SALE
df_h_sale = df[['Sale_HK']].copy()
df_h_sale['H_Load_Source'] = 'buisness_data.csv'
df_h_sale['H_Load_Date'] = pd.Timestamp.now()

df_h_sale.to_sql(
    'H_SALE',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)

# H_CUSTOMER
df_h_customer = df[['Customer_HK']].copy()
df_h_cusotmer['H_Load_Source'] = 'buisness_data.csv'
df_h_customer['H_Load_Date'] = pd.Timestamp.now()

df_h_customer.to_sql(
    'H_CUSTOMER',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)

# H_SHIPMENT
df_h_shipment = df[['Shipment_HK']].copy()
df_h_shipment['H_Load_Source'] = 'buisness_data.csv'
df_h_shipment['H_Load_Date'] = pd.Timestamp.now()

df_h_shipment.to_sql(
    'H_SHIPMENT',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)

# H_PRODUCT
df_h_product = df[['Product_HK']].copy()
df_h_product['H_Load_Source'] = 'buisness_data.csv'
df_h_product['H_Load_Date'] = pd.Timestamp.now()

df_h_product.to_sql(
    'H_PRODUCT',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)

# L_SALE
df_l_sale = df[['Sale_HK', 'Customer_HK']].copy()

df_l_sale['H_Load_Source'] = 'buisness_data.csv'
df_l_sale['H_Load_Date'] = pd.Timestamp.now()

df_l_sale.to_sql(
    'L_SALE',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)

# L_PRODUCT
df_l_product = df[['Sale_HK', 'Product_HK']].copy()

df_l_product['H_Load_Source'] = 'buisness_data.csv'
df_l_product['H_Load_Date'] = pd.Timestamp.now()

df_l_product.to_sql(
    'L_PRODUCT',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)

# L_SHIPMENT
df_l_shipment = df[['Sale_HK', 'Shipment_HK']].copy()

df_l_shipment['H_Load_Source'] = 'buisness_data.csv'
df_l_shipment['H_Load_Date'] = pd.Timestamp.now()

df_l_shipment.to_sql(
    'L_SHIPMENT',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)

# S_DETAILS
df_s_details = df[['Product_HK', 'Segment']].copy()
df_s_details['S_Hash'] = make_bk_hash(df, ["Segmnet"])
df_s_details['H_Load_Source'] = 'buisness_data.csv'
df_s_details['H_Load_Date'] = pd.Timestamp.now()

df_s_details.to_sql(
    'S_DETAILS',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)

# S_CUSTOMER
df_s_details = df[['Customer_HK', 'Postal_Code', 'Country', 'City', 'State', 'Region']].copy()
df_s_details['S_Hash'] = make_bk_hash(df, ['Postal_Code', 'Country', 'City', 'State', 'Region'])
df_s_details['H_Load_Source'] = 'buisness_data.csv'
df_s_details['H_Load_Date'] = pd.Timestamp.now()

df_s_details.to_sql(
    'S_CUSTOMER',
    con=engine,
    schema='student30',
    if_exists='append',
    index=False,
    method='multi'
)