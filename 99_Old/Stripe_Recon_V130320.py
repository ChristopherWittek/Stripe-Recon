# Step 1 - create main data frame from Statement
import pandas as pd
import re
import os
pd.options.display.float_format = '{:,.2f}'.format

input = input('Copy & paste file-path to desired reconciliation folder:')
main_path = input
data_path = input+r'\Data'
output_path = input+r'\Output Files'
# path_parent = os.path.dirname(os.getcwd())
ns_data = main_path+r"\NetSuite Data"


stripe_statement = pd.read_csv(data_path+r'\Stripe_Statement.csv',encoding='iso8859_15')
stripe_statement['Web Shop Order No'] = ''
for index,row in stripe_statement.iterrows() :
    try :
        order_id = re.search('\#([^ ]+) .*',row['description'])
        stripe_statement['Web Shop Order No'].loc[index] = order_id[1]
    except :
        continue

#           -> Add customer name from Orders
netsuite_orders = pd.read_csv(ns_data+r'\Stripe_Orders.csv',encoding='iso8859_15',usecols=['Web Shop Order No','Name','Sales Channel','BRAND','SALES REGION'])#.rename(columns={'Webshop Payment Token':'payment_intent_id'})
stripe_statement = stripe_statement.merge(netsuite_orders,how='left',on='Web Shop Order No')
for index,row in stripe_statement.iterrows() :
    sales_channel = str(row['Sales Channel'])
    if sales_channel[4:6] == 'WM' :
        stripe_statement['BRAND'].loc[index] = 'Womanizer'
    elif sales_channel[4:7] == 'Arc' :
        stripe_statement['BRAND'].loc[index] = 'Arcwave'
    else :
        stripe_statement['BRAND'].loc[index] = 'We-Vibe'

#           -> Add Currency, FX Amount, EUR Amount from Payments
netsuite_payments = pd.read_csv(ns_data+r'\Stripe_Payments.csv',dtype=object,encoding='iso8859_15',header=6,usecols=['Currency: Name','Amount (Foreign Currency)','Amount','Web Shop Order No','Type']).rename(columns={'Currency: Name':'NS Currency','Amount (Foreign Currency)':'NS FX Amount','Amount':'NS EUR Amount'})
netsuite_refunds = pd.read_csv(ns_data+r'\Stripe_Refunds.csv',dtype=object,encoding='iso8859_15',header=6,usecols=['Currency: Name','Amount (Foreign Currency)','Amount','Memo','Type']).rename(columns={'Memo':'Web Shop Order No','Currency: Name':'NS Currency','Amount (Foreign Currency)':'NS FX Amount','Amount':'NS EUR Amount'})

stripe_statement_charges = stripe_statement[stripe_statement['reporting_category'] == 'charge']
stripe_statement_refunds = stripe_statement[stripe_statement['reporting_category'] == 'refund']
stripe_statement_disputes = stripe_statement[stripe_statement['reporting_category'] == 'dispute']
stripe_statement_charges = stripe_statement_charges.merge(netsuite_payments,how='left',on='Web Shop Order No')
stripe_statement_refunds = stripe_statement_refunds.merge(netsuite_refunds,how='left',on='Web Shop Order No')
stripe_statement = stripe_statement_charges.append([stripe_statement_refunds,stripe_statement_disputes],ignore_index=True)

def floatCleanser(dirty_series) :
    dirty_series = dirty_series.str.replace(',','').str.replace('€','').str.replace('$','').str.replace('£','').str.replace(r'â\x82','').str.replace('Â','').str.rstrip(')').str.replace('Can','').str.replace('--','-').str.replace('¬','').str.replace('(','-').astype(float)
    global clean_series
    clean_series = dirty_series
    return clean_series

stripe_statement['NS EUR Amount'] = floatCleanser(stripe_statement['NS EUR Amount'])
stripe_statement['NS FX Amount'] = floatCleanser(stripe_statement['NS FX Amount'])

# Step 2 - Add calculations and conditions (S - X)
stripe_statement['gross'] = round(stripe_statement['gross'].astype(float),2)
stripe_statement['Stat vs NS FX'] = 0.00
stripe_statement['Stat vs NS EUR'] = 0.00
stripe_statement['FX G/L'] = 0.00

for index,row in stripe_statement.iterrows() :
    ns_currency = str(row['NS Currency'])
    stripe_currency = str(row['customer_facing_currency'])
    ns_fx_amount = float(row['NS FX Amount'])
    customer_facing_amount = float(row['customer_facing_amount'])
    gross = float(row['gross'])
    ns_eur_amount = float(row['NS EUR Amount'])
    if row['customer_facing_currency'] == 'gbp' :
        stripe_statement['Stat vs NS FX'].iloc[index] = customer_facing_amount - ns_eur_amount
        stripe_statement['FX G/L'].iloc[index] = gross - ns_eur_amount
    else :
        stripe_statement['Stat vs NS FX'].iloc[index] = 'EUR'
        stripe_statement['FX G/L'].iloc[index] = 0.00
    if row['customer_facing_currency'] == 'eur' :
        stripe_statement['Stat vs NS EUR'].iloc[index] = gross - ns_eur_amount
    else :
        stripe_statement['Stat vs NS EUR'].iloc[index] = 'GBP'
    if ns_currency == '' :
        ns_currency.loc[key] = stripe_currency.capitalize()
    else :
        continue

stripe_statement.to_csv(data_path+r'\stripe_statement_extract.csv')

### Journal Entry ###
## 1777 - negative 'FX G/L'
## 1642 - positive 'FX G/L'
## 1641 - positive 'NS EUR Amount'per order
## 117 - negative 'NS EUR Amount' per order (Refunds)
# 680 - 'Fee'
# 742 - sum(CR) - sum(DR)
# 599 - No order in Netsuite (Sales & Refunds) ??

# try :
#     recon_period_market = re.findall('Stripe .*?$',main_path)[0]
# except :
#     print('File path not correct format')

memo = 'Stripe Recon 02-2021'
cols = ['External ID','Account Internal ID','DR','CR','Memo','Name','Region','Channel','Sales Channel','Brand','Product','Technology']
journal_entry = pd.DataFrame(columns=cols)
stripe_statement['ID'] = stripe_statement['Name'].str.split(' ').str[0]
cust_internal_ids = pd.read_excel(ns_data+r'\Cust_Internal_IDs.xlsx',dtype=object,usecols=['ID','Internal ID'])#delimiter=',',encoding='iso8859_15',
stripe_statement = stripe_statement.merge(cust_internal_ids,how='left',on='ID')

for index,row in stripe_statement.iterrows() :
    web_shop_order_no = row['Web Shop Order No']
    gross = row['gross']
    fee = row['fee']
    reporting_cat = row['reporting_category']
    ns_order_id = row['Web Shop Order No']
    cust_internal_id = row['Internal ID']
    transaction_type = row['Type']
    ns_eur_amount = row['NS EUR Amount']
    fx_gl = row['FX G/L']
    sales_channel = str(row['Sales Channel'])
    brand = row['BRAND']
    sales_region = row['SALES REGION']
    if transaction_type in ['Customer Refund'] :
        journal_line = {'Account Internal ID':'117',
        'DR':abs(ns_eur_amount),
        'CR':0,
        'Memo':memo+' '+web_shop_order_no,
        'Name':cust_internal_id,
        'Region':sales_region,
        'Channel':'B2C : Adult WebShop',
        'Sales Channel':sales_channel,
        'Brand':brand,
        'Sales Region':sales_region}
        journal_entry = journal_entry.append(journal_line,ignore_index=True)
    elif transaction_type in ['Payment'] :
        journal_line = {'Account Internal ID':'1641',
        'DR':0,
        'CR':abs(ns_eur_amount),
        'Memo':memo+' '+web_shop_order_no,
        'Name':cust_internal_id,
        'Region':sales_region,
        'Channel':'B2C : Adult WebShop',
        'Sales Channel':sales_channel,
        'Brand':brand,
        'Sales Region':sales_region}
        journal_entry = journal_entry.append(journal_line,ignore_index=True)
    else :
        if gross < 0 :
            journal_line = {'Account Internal ID':'599',
            'DR':abs(gross),
            'CR':0,
            'Memo':memo+' Missing Refund in NS'+' '+web_shop_order_no,
            'Name':cust_internal_id,
            'Region':sales_region,
            'Channel':'B2C : Adult WebShop',
            'Sales Channel':sales_channel,
            'Brand':brand,
            'Sales Region':sales_region}
            journal_entry = journal_entry.append(journal_line,ignore_index=True)
        if gross > 0 :
            journal_line = {'Account Internal ID':'599',
            'DR':0,
            'CR':abs(gross),
            'Memo':memo+' Missing Payment in NS'+' '+web_shop_order_no,
            'Name':cust_internal_id,
            'Region':sales_region,
            'Channel':'B2C : Adult WebShop',
            'Sales Channel':sales_channel,
            'Brand':brand,
            'Sales Region':sales_region}
            journal_entry = journal_entry.append(journal_line,ignore_index=True)

statement_v_ns_diff = stripe_statement['Stat vs NS EUR'][stripe_statement['Stat vs NS EUR'] != 'GBP'].sum()
journal_line = {'Account Internal ID':'599',
'DR':0,
'CR':abs(statement_v_ns_diff),
'Memo':memo+' Minor deltas Statement vs NS EUR',
'Name':cust_internal_id,
'Region':sales_region,
'Channel':'B2C : Adult WebShop',
'Sales Channel':sales_channel,
'Brand':brand,
'Sales Region':sales_region}
journal_entry = journal_entry.append(journal_line,ignore_index=True)

acc_1642 = stripe_statement['FX G/L'][stripe_statement['FX G/L'] < 0].sum()
acc_1777 = stripe_statement['FX G/L'][stripe_statement['FX G/L'] >= 0].sum()

journal_line_acc_1777 = {'Account Internal ID':'1777',
                        'DR':0,
                        'CR':abs(acc_1777),
                        'Memo':'Stripe Payments/Refunds'+' '+memo+' '+'Realized FX Gain',
                        'Region':'EMEA : Western Europe',
                        'Channel':'B2C : Adult WebShop',
                        'Sales Channel':'Corporate',
                        'Brand':'Corporate',
                        'Sales Region':'EMEA'}

journal_line_acc_1642 = {'Account Internal ID':'1642',
                        'DR':abs(acc_1642),
                        'CR':0,
                        'Memo':'Stripe Payments/Refunds'+' '+memo+' '+'Realized FX Loss',
                        'Region':'EMEA : Western Europe',
                        'Channel':'B2C : Adult WebShop',
                        'Sales Channel':'Corporate',
                        'Brand':'Corporate',
                        'Sales Region':'EMEA'}

journal_entry = journal_entry.append([journal_line_acc_1777,journal_line_acc_1642],ignore_index=True)

brands = ['Womanizer','We-Vibe','Arcwave']

for b in brands :
    brand_sum_no_dispute = stripe_statement['fee'][(stripe_statement['BRAND'] == b) & (stripe_statement['reporting_category'] != 'dispute')].sum()
    brand_sum_dispute = stripe_statement['fee'][(stripe_statement['BRAND'] == b) & (stripe_statement['reporting_category'] == 'dispute')].sum()
    journal_line_acc_680_nd = {'Account Internal ID':'680',
                            'DR':abs(brand_sum_no_dispute),
                            'CR':0,
                            'Memo':'Stripe Payment fees'+' '+memo,
                            'Region':'EMEA : Western Europe',
                            'Channel':'B2C : Adult WebShop',
                            'Sales Channel':'Corporate',
                            'Brand':b,
                            'Sales Region':'EMEA'}
    journal_line_acc_680_d = {'Account Internal ID':'680',
                            'DR':abs(brand_sum_dispute),
                            'CR':0,
                            'Memo':'Stripe Payment fees'+' '+memo,
                            'Region':'EMEA : Western Europe',
                            'Channel':'B2C : Adult WebShop',
                            'Sales Channel':'Corporate',
                            'Brand':b,
                            'Region':'EMEA'}

    if brand_sum_dispute == 0 :
        journal_entry = journal_entry.append(journal_line_acc_680_nd,ignore_index=True)
    else :
        journal_entry = journal_entry.append([journal_line_acc_680_nd,journal_line_acc_680_d],ignore_index=True)

acc_742_cash_in = stripe_statement['net'][stripe_statement['reporting_category'] == 'charge'].sum()
acc_742_cash_out = stripe_statement['net'][(stripe_statement['reporting_category'] == 'refund') | (stripe_statement['reporting_category'] == 'dispute')].sum()

#journal_entry['DR'].sum() - journal_entry['CR'].sum()

journal_line_acc_742_cash_in = {'Account Internal ID':'742',
                        'DR':abs(acc_742_cash_in),
                        'CR':0,
                        'Memo':'Cash Income'+' '+memo,
                        'Region':'EMEA : Western Europe',
                        'Channel':'B2C : Adult WebShop',
                        'Sales Channel':'Corporate',
                        'Brand':'Corporate',
                        'Region':'EMEA'}

journal_line_acc_742_cash_out = {'Account Internal ID':'742',
                        'DR':0,
                        'CR':abs(acc_742_cash_out),
                        'Memo':'Cash Out'+' '+memo,
                        'Region':'EMEA : Western Europe',
                        'Channel':'B2C : Adult WebShop',
                        'Sales Channel':'Corporate'
                        'Brand':'Corporate',
                        'Region':'EMEA'}

journal_entry = journal_entry.append([journal_line_acc_742_cash_in,journal_line_acc_742_cash_out],ignore_index=True)
journal_entry['External ID'],journal_entry['Product'],journal_entry['Technology'] = memo,'Corporate','Corporate'
journal_entry['Channel'] = journal_entry['Channel'].fillna()
journal_entry = journal_entry.set_index('External ID')
journal_entry.to_csv(output_path+r'\journal_entry.csv')
