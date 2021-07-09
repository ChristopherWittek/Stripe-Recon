###### IMPORT RELEVANT MODULES AND SET NUMBER FORMATTING ######
import pandas as pd
import numpy as np
import re
import os
pd.options.display.float_format = '{:,.2f}'.format
####### DEFINE ALL FILE PATHS RELEVANT TO RECONCILIATION ######
input_path = input('Copy & paste file-path to desired reconciliation folder:')
recon_path = input_path
data_path = recon_path+r'\Data'
output_path = recon_path+r'\Output Files'
path_parent = os.path.dirname(os.getcwd())
ns_data = recon_path+r"\NetSuite Data"
recon_country = input('Recon Country: ').upper()
###### DEFINE THE NAME OF THE PERIOD AND MARKET TO BE RECONCILED ######
try :
    recon_period_market = str(re.findall('Stripe .*?$',recon_path)[0].replace('\\','').replace(' - ','.'))
except :
    print(r'"/!\ File path not correct format /!\"')
    recon_period_market_input = input('Enter Recon Period Market e.g. Stripe 04.2021:')
    recon_period_market = recon_period_market_input
###### DEFINE STATEMENT DATAFRAME BASED ON ORIGINAL STATEMENT AND PREPARE FOR RECON ######
stripe_statement_cols = ['balance_transaction_id','currency','gross','fee','net','reporting_category','description','customer_facing_amount','customer_facing_currency']
stripe_statement = pd.read_csv(data_path+r'\Stripe_Statement.csv',encoding='iso8859_15',usecols=stripe_statement_cols)
stripe_statement['Web Shop Order No'] = ''
for index,row in stripe_statement.iterrows() :
    try :
        order_id = re.search('\#([^ ]+) .*',row['description'])
        stripe_statement['Web Shop Order No'].loc[index] = order_id[1]
    except :
        continue
###### DEFINE NETSUITE SALES ORDER DATA DATAFRAME, MERGE WITH STATEMENT AND POPULATE MISSING DATA ######
netsuite_orders = pd.read_csv(ns_data+r'\Stripe_Orders.csv',encoding='iso8859_15',usecols=['Web Shop Order No','Name','Sales Channel','BRAND','SALES REGION']).rename(columns={'SALES REGION':'Region'})
stripe_statement = stripe_statement.merge(netsuite_orders,how='left',on='Web Shop Order No')
for index,row in stripe_statement.iterrows() :
    sales_channel = str(row['Sales Channel'])
    if sales_channel[4:6] == 'WM' :
        stripe_statement['BRAND'].loc[index] = 'Womanizer'
    elif sales_channel[4:7] == 'Arc' :
        stripe_statement['BRAND'].loc[index] = 'Arcwave'
    elif sales_channel[4:6] == 'WV' :
        stripe_statement['BRAND'].loc[index] = 'We-Vibe'
    else :
        stripe_statement['BRAND'].loc[index] = 'Corporate'
###### DEFINE NETSUITE PAYMENT AND REFUND DATAFRAMES AND RENAME COLUMNS TO PREPARE FOR MERGE WITH STATEMENT ######
if recon_country in ['US','USA','CA','CAN'] :
    netsuite_payments = pd.read_csv(ns_data+r'\Stripe_Payments.csv',dtype=object,encoding='iso8859_15',header=6,usecols=['Currency: Name','Amount (Foreign Currency)','Amount','Memo','Type']).rename(columns={'Currency: Name':'NS Currency','Amount (Foreign Currency)':'NS FX Amount','Amount':'NS EUR Amount','Memo':'Web Shop Order No'})
else:
    netsuite_payments = pd.read_csv(ns_data+r'\Stripe_Payments.csv',dtype=object,encoding='iso8859_15',header=6,usecols=['Currency: Name','Amount (Foreign Currency)','Amount','Web Shop Order No','Type']).rename(columns={'Currency: Name':'NS Currency','Amount (Foreign Currency)':'NS FX Amount','Amount':'NS EUR Amount'})
netsuite_refunds = pd.read_csv(ns_data+r'\Stripe_Refunds.csv',dtype=object,encoding='iso8859_15',header=6,usecols=['Currency: Name','Amount (Foreign Currency)','Amount','Memo','Type']).rename(columns={'Memo':'Web Shop Order No','Currency: Name':'NS Currency','Amount (Foreign Currency)':'NS FX Amount','Amount':'NS EUR Amount'})
netsuite_refunds.to_csv(output_path+r'\netsuite_refunds.csv')
###### SPLIT THE STATEMENTS INTO TRANSACTION CATEGORIES AND MERGE WITH PAYMENTS / REFUNDS ######
stripe_statement_charges = stripe_statement[stripe_statement['reporting_category'] == 'charge']
stripe_statement_refunds = stripe_statement[stripe_statement['reporting_category'] == 'refund']
stripe_statement_disputes = stripe_statement[stripe_statement['reporting_category'] == 'dispute']
stripe_statement_charges = stripe_statement_charges.merge(netsuite_payments,how='left',on='Web Shop Order No').fillna(0)
stripe_statement_refunds.to_csv(output_path+r'\stripe_statement_refunds.csv')
stripe_statement_refunds = stripe_statement_refunds.merge(netsuite_refunds,how='left',on='Web Shop Order No').fillna(0)
stripe_statement = stripe_statement_charges.append([stripe_statement_refunds,stripe_statement_disputes],ignore_index=True)
###### DEFINE FUNCTION TO REMOVE SYMBOLS RESULTING FROM DECODING ERROR AND CONVERT SERIES TO FLOAT ######
def floatCleanser(dirty_series) :
    try :
        dirty_series = dirty_series.str.replace(' ','',regex=True).str.replace('¬','',regex=True).str.replace('$','',regex=True).str.replace(r'â\x82','',regex=True).str.replace(',','',regex=True).str.replace('€','',regex=True).str.replace('£','',regex=True).str.replace('ý','',regex=True).str.replace('Â','',regex=True).str.rstrip(')').str.rstrip(' ').str.lstrip('ý').str.replace('Can','',regex=True).str.replace('--','-',regex=True).str.replace('¬','',regex=True).str.replace('(','-',regex=True).str.replace('ý','',regex=True).astype(float)#.str.replace('-ï¿œï¿œï¿','',regex=True).str.replace('(ï¿œï¿œï','-',regex=True).str.replace('(ï¿œï¿œï¿','-',regex=True)
        global clean_series
        clean_series = dirty_series
        return clean_series
    except Exception as e :
        print(e)

###### CLEAN SERIES TO MAKE THEM FLOAT ######
# stripe_statement['NS FX Amount'] = floatCleanser(stripe_statement['NS FX Amount'].fillna(0))
stripe_statement['NS EUR Amount'] = floatCleanser(stripe_statement['NS EUR Amount'])
###### COMBINE DUPLICATE STATEMENT ROWS RESULTING FROM MERGING WITH  SALES ORDERS CORRECTED IN NETSUITE (ADDITIONAL LINE ADDED) ######
cols_orders = ['Web Shop Order No','Name','Sales Channel','BRAND','Region','Type','NS Currency']
stripe_statement_cols.extend(cols_orders)
duplicates = list(stripe_statement['balance_transaction_id'][stripe_statement['balance_transaction_id'].duplicated()])
stripe_statement_duplicates = stripe_statement[stripe_statement['balance_transaction_id'].isin(duplicates)]
stripe_statement_duplicates = stripe_statement_duplicates.groupby(stripe_statement_cols,as_index=False).sum(['NS FX Amount','NS EUR Amount'])
stripe_statement_non_duplicates = stripe_statement[~stripe_statement['balance_transaction_id'].isin(duplicates)]
stripe_statement = stripe_statement_non_duplicates.append(stripe_statement_duplicates)
###### SPLIT THE CUSTOMER NAME INTO THE ID AND FIRST + LAST NAME, THEN MERGE WITH CUSTOMER INTERNAL IDs ######
stripe_statement['ID'] = stripe_statement['Name'].str.split(' ').str[0]
cust_internal_ids = pd.read_excel(ns_data+r'\Cust_Internal_IDs.xlsx',dtype=object,usecols=['ID','Internal ID'])#delimiter=',',encoding='iso8859_15',
stripe_statement = stripe_statement.merge(cust_internal_ids,how='left',on='ID')
###### PREPARE / ADD FLOAT COLUMNS FOR SUBSEQUENT CALCULATIONS ######
stripe_statement['gross'] = round(stripe_statement['gross'].astype(float),2)
stripe_statement['Stat vs NS FX'] = 0.00
stripe_statement['Stat vs NS EUR'] = 0.00
stripe_statement['FX G/L'] = 0.00
###### COMPARISON BETWEEN STATEMENT AND NETSUITE DATA (PAYMENT / REFUNDS) ######
for index,row in stripe_statement.iterrows() :
    ns_currency = str(row['NS Currency'])
    stripe_currency = str(row['customer_facing_currency'])
    ns_fx_amount = float(row['NS FX Amount'])
    customer_facing_amount = float(row['customer_facing_amount'])
    gross = float(row['gross'])
    try :
        ns_amount = float(row['NS EUR Amount'])
    except Exception as e :
        print(e,'\n',index,row['NS EUR Amount'],type(row['NS EUR Amount']))
    if row['customer_facing_currency'] == 'gbp' :
        stripe_statement['Stat vs NS FX'].iloc[index] = customer_facing_amount - ns_amount
        stripe_statement['FX G/L'].iloc[index] = gross - ns_amount
    else :
        stripe_statement['Stat vs NS FX'].iloc[index] = 'EUR'
        stripe_statement['FX G/L'].iloc[index] = 0.00
    if recon_country in ['US','USA','CA','CAN'] :
        stripe_statement['Stat vs NS EUR'].iloc[index] = gross - ns_fx_amount
        stripe_statement['FX G/L'].iloc[index] = gross - ns_amount
    else :
        if row['customer_facing_currency'] == 'eur' :
            stripe_statement['Stat vs NS EUR'].iloc[index] = gross - ns_amount
        else :
            stripe_statement['Stat vs NS EUR'].iloc[index] = 'GBP'
    if ns_currency == '' :
        ns_currency.loc[key] = stripe_currency.capitalize()
    else :
        continue
###### DEFINE JOURNAL ENTRY DATAFRAME ######
memo = recon_period_market
cols = ['External ID','Account Internal ID','DR','CR','Memo','Name','Channel','Sales Channel','Brand','Product','Technology','Region']
journal_entry = pd.DataFrame(columns=cols)
###### WRITE REFUNDS (117), PAYMENTS (1641) AND MISSING PAYMENTS / REFUNDS (599) INTO JOURNAL ######
stripe_statement.to_csv(output_path+r'\stripe_statement.csv')
for index,row in stripe_statement.iterrows() :
    if row['Web Shop Order No'] == '' :
        web_shop_order_no = row['description']
    else :
        web_shop_order_no = row['Web Shop Order No']
    gross = row['gross']
    fee = row['fee']
    reporting_cat = row['reporting_category']
    ns_order_id = str(row['Web Shop Order No'])
    cust_internal_id = row['Internal ID']
    transaction_type = row['Type']
    if recon_country in ['US','USA','CA','CAN'] :
        ns_amount = float(row['NS FX Amount'])
    else :
        ns_amount = float(row['NS EUR Amount'])
    fx_gl = row['FX G/L']
    brand = row['BRAND']
    region = row['Region']
    sales_channel = str(row['Sales Channel'])
    if sales_channel == 'nan' :
        sales_channel = 'Corporate'
    if transaction_type in ['Customer Refund'] :
        journal_line = {'Account Internal ID':'117',
        'DR':abs(ns_amount),
        'CR':0,
        'Memo':memo+' '+web_shop_order_no,
        'Name':cust_internal_id,
        'Channel':'B2C : Adult WebShop',
        'Sales Channel':sales_channel,
        'Brand':brand,
        'Region':region}
        journal_entry = journal_entry.append(journal_line,ignore_index=True)
    elif transaction_type in ['Payment'] :
        journal_line = {'Account Internal ID':'1641',
        'DR':0,
        'CR':abs(ns_amount),
        'Memo':memo+' '+web_shop_order_no,
        'Name':cust_internal_id,
        'Channel':'B2C : Adult WebShop',
        'Sales Channel':sales_channel,
        'Brand':brand,
        'Region':region}
        journal_entry = journal_entry.append(journal_line,ignore_index=True)
    else :
        if gross < 0 :
            journal_line = {'Account Internal ID':'599',
            'DR':abs(gross),
            'CR':0,
            'Memo':memo+' Missing Refund in NS'+' '+web_shop_order_no,
            'Name':'',
            'Channel':'B2C : Adult WebShop',
            'Sales Channel':sales_channel,
            'Brand':brand,
            'Region':region}
            journal_entry = journal_entry.append(journal_line,ignore_index=True)
        if gross > 0 :
            journal_line = {'Account Internal ID':'599',
            'DR':0,
            'CR':abs(gross),
            'Memo':memo+' Missing Payment in NS'+' '+web_shop_order_no,
            'Name':'',
            'Channel':'B2C : Adult WebShop',
            'Sales Channel':sales_channel,
            'Brand':brand,
            'Region':region}
            journal_entry = journal_entry.append(journal_line,ignore_index=True)
###### WRITE DELTAS BETWEEN STATEMENT AND NETSUITE INTO JOURNAL ######
statement_v_ns_diff = stripe_statement['Stat vs NS EUR'][ ( stripe_statement['Stat vs NS EUR'] != 'GBP' ) & ( stripe_statement['Web Shop Order No'] != '' ) ].sum()
if statement_v_ns_diff > 0 :
    journal_line = {'Account Internal ID':'599',
    'DR':0,
    'CR':abs(statement_v_ns_diff),
    'Memo':memo+' deltas Statement vs NS EUR',
    'Name':'',
    'Region':region,
    'Channel':'B2C : Adult WebShop',
    'Sales Channel':sales_channel,
    'Brand':brand,
    'Region':region}
    journal_entry = journal_entry.append(journal_line,ignore_index=True)
else :
    journal_line = {'Account Internal ID':'599',
    'DR':abs(statement_v_ns_diff),
    'CR':0,
    'Memo':memo+' deltas Statement vs NS EUR',
    'Name':'',
    'Region':region,
    'Channel':'B2C : Adult WebShop',
    'Sales Channel':sales_channel,
    'Brand':brand,
    'Region':region}
    journal_entry = journal_entry.append(journal_line,ignore_index=True)
###### CALCULATE AND WRITE FX GAINS AND LOSSES TO JOURNAL ######
if recon_country not in ['US','USA','CA','CAN'] :
    acc_1642 = abs(stripe_statement['FX G/L'][stripe_statement['FX G/L'] < 0 ].sum())
    acc_1777 = abs(stripe_statement['FX G/L'][stripe_statement['FX G/L'] >= 0 ].sum())
    if statement_v_ns_diff > 0 :
        acc_1642 = acc_1642 + abs(statement_v_ns_diff)
    else :
        acc_1777 = acc_1777 + abs(statement_v_ns_diff)
    journal_line_acc_1777 = {'Account Internal ID':'1777',
                            'DR':0,
                            'CR':acc_1777,
                            'Memo':'Stripe Payments/Refunds'+' '+memo+' '+'Realized FX Gain',
                            'Channel':'B2C : Adult WebShop',
                            'Sales Channel':'Corporate',
                            'Brand':'Corporate',
                            'Region':region}
    journal_line_acc_1642 = {'Account Internal ID':'1642',
                            'DR':acc_1642,
                            'CR':0,
                            'Memo':'Stripe Payments/Refunds'+' '+memo+' '+'Realized FX Loss',
                            'Channel':'B2C : Adult WebShop',
                            'Sales Channel':'Corporate',
                            'Brand':'Corporate',
                            'Region':region}
    journal_entry = journal_entry.append([journal_line_acc_1777,journal_line_acc_1642],ignore_index=True)
###### WRITE FEES PER BRAND INTO JOURNAL ######
brands = ['Womanizer','We-Vibe','Arcwave','Corporate']
for b in brands :
    brand_sum_no_dispute = stripe_statement['fee'][(stripe_statement['BRAND'] == b) & (stripe_statement['reporting_category'] != 'dispute')].sum()
    brand_sum_dispute = stripe_statement['fee'][(stripe_statement['BRAND'] == b) & (stripe_statement['reporting_category'] == 'dispute')].sum()
    journal_line_acc_680_nd = {'Account Internal ID':'680',
                            'DR':abs(brand_sum_no_dispute),
                            'CR':0,
                            'Memo':'Stripe Payment fees'+' '+memo,
                            'Channel':'B2C : Adult WebShop',
                            'Sales Channel':'Corporate',
                            'Brand':b,
                            'Region':region}
    journal_line_acc_680_d = {'Account Internal ID':'680',
                            'DR':abs(brand_sum_dispute),
                            'CR':0,
                            'Memo':'Stripe Payment fees'+' '+memo,
                            'Channel':'B2C : Adult WebShop',
                            'Sales Channel':'Corporate',
                            'Brand':b,
                            'Region':region}
    if brand_sum_dispute == 0 :
        journal_entry = journal_entry.append(journal_line_acc_680_nd,ignore_index=True)
    else :
        journal_entry = journal_entry.append([journal_line_acc_680_nd,journal_line_acc_680_d],ignore_index=True)
###### CALCULATE AND WRITE CASH IN AND CASH OUT INTO JOURNAL ######
stripe_statement = stripe_statement.iloc[:,[0,1,2,3,4,5,6,7,8,9,10]].drop_duplicates()
acc_742_cash_in = stripe_statement['net'][stripe_statement['reporting_category'] == 'charge'].sum()
acc_742_cash_out = stripe_statement['net'][(stripe_statement['reporting_category'] == 'refund') | (stripe_statement['reporting_category'] == 'dispute')].sum()
journal_line_acc_742_cash_in = {'Account Internal ID':'742',
                        'DR':abs(acc_742_cash_in),
                        'CR':0,
                        'Memo':'Cash Income'+' '+memo,
                        'Channel':'B2C : Adult WebShop',
                        'Sales Channel':'Corporate',
                        'Brand':'Corporate',
                        'Region':region}
journal_line_acc_742_cash_out = {'Account Internal ID':'742',
                        'DR':0,
                        'CR':abs(acc_742_cash_out),
                        'Memo':'Cash Out'+' '+memo,
                        'Channel':'B2C : Adult WebShop',
                        'Sales Channel':'Corporate',
                        'Brand':'Corporate',
                        'Region':region}
journal_entry = journal_entry.append([journal_line_acc_742_cash_in,journal_line_acc_742_cash_out],ignore_index=True)
###### FINALIZE THE JOURNAL AND SAVE IT AS CSV FOR UPLOAD INTO NETSUITE ######
journal_entry['External ID'],journal_entry['Product'],journal_entry['Technology'] = memo,'Corporate','Corporate'
journal_entry['Sales Channel'] = journal_entry['Sales Channel'].str.replace('0','Corporate')#.replace(np.nan,'Corporate')#,inplace=True
# journal_entry['Region'] = journal_entry['Region'].fillna('EMEA : Western Europe',inplace=False)# inplace=True removes regions in all rows
journal_entry['Region'] = journal_entry['Region'].fillna(region)#,inplace=True
journal_entry = journal_entry.set_index('External ID')
journal_entry.to_csv(output_path+r'\Journal Entry '+recon_period_market+'.csv')
