# Step 1 - create main data frame from Statement
import pandas as pd
import re
stripe_statement = pd.read_csv('Stripe_Statement.csv',encoding='iso8859_15')
stripe_statement['NS Order ID'] = ''
for index,row in stripe_statement.iterrows() :
    try :
        order_id = re.search('\#([^ ]+) .*',row['description'])
        stripe_statement['NS Order ID'].loc[index] = order_id[1]
    except :
        continue

#           -> Add customer name from Orders
netsuite_orders = pd.read_csv('Netsuite_Stripe_Orders.csv',usecols=['Name','Webshop Payment Token','Sales Channel','BRAND','SALES REGION']).rename(columns={'Webshop Payment Token':'payment_intent_id'})
stripe_statement = stripe_statement.merge(netsuite_orders,how='left',on='payment_intent_id')
for index,row in stripe_statement.iterrows() :
    sales_channel = str(row['Sales Channel'])
    if row['BRAND'] == '' :
        continue
    else :
        if sales_channel[4:6] == 'WM' :
            stripe_statement['BRAND'].loc[index] = 'Womanizer'
        else :
            stripe_statement['BRAND'].loc[index] = 'We-Vibe'
#           -> Add Currency, FX Amount, EUR Amount from Payments
netsuite_payments = pd.read_csv('Netsuite_Stripe_Payments.csv',encoding='iso8859_15',usecols=['Currency','Amount (Foreign Currency)','Amount','Web Shop Order No','Internal ID','Type','Subsidiary','Account']).rename(columns={'Currency':'NS Currency','Amount (Foreign Currency)':'NS FX Amount','Amount':'NS EUR Amount','Web Shop Order No':'NS Order ID'})
netsuite_payments['NS EUR Amount'] = netsuite_payments['NS EUR Amount'].str.replace('(','-').str.replace(')','').str.replace(',','').astype(float)
netsuite_payments['NS FX Amount'] = netsuite_payments['NS FX Amount'].str.replace(',','').str.replace('(','-').str.replace('€','').str.strip().str.replace('$','').str.replace('£','').str.replace(r'-â\x82','').str.replace('Â','').str.rstrip(')').str.replace('Can','').str.replace('--','-').str.replace('¬','').astype(float)

netsuite_payments_charges = netsuite_payments[netsuite_payments['Type'] == 'Invoice']
netsuite_payments_refunds = netsuite_payments[netsuite_payments['Type'] == 'Credit Memo']
stripe_statement_charges = stripe_statement[stripe_statement['reporting_category'] == 'charge']
stripe_statement_refunds = stripe_statement[stripe_statement['reporting_category'] == 'refund']
stripe_statement_disputes = stripe_statement[stripe_statement['reporting_category'] == 'dispute']
stripe_statement_charges = stripe_statement_charges.merge(netsuite_payments_charges,how='left',on='NS Order ID')
stripe_statement_refunds = stripe_statement_refunds.merge(netsuite_payments_refunds,how='left',on='NS Order ID')
stripe_statement = stripe_statement_charges.append([stripe_statement_refunds,stripe_statement_disputes])

# Step 2 - Add calculations and conditions (S - X)
stripe_statement['Stat vs NS FX'] = 0.0
stripe_statement['Stat vs NS EUR'] = 0.0
stripe_statement['FX G/L'] = 0.0
for index,row in stripe_statement.iterrows() :
    if row['customer_facing_currency'] == 'gbp' :
        stripe_statement['Stat vs NS FX'].iloc[index] = row['FX Amount'] - row['NS FX Amount']
        stripe_statement['FX G/L'].iloc[index] = row['NS EUR Amount'] - row['gross']
    else :
        stripe_statement['Stat vs NS FX'].iloc[index] = 'EUR'
        stripe_statement['FX G/L'].iloc[index] = 0.0
    if row['customer_facing_currency'] == 'eur' :
        stripe_statement['Stat vs NS EUR'].iloc[index] = row['NS FX Amount'] - row['gross']
    else :
        stripe_statement['Stat vs NS EUR'].iloc[index] = 'GBP'

stripe_statement.to_csv('stripe_statement_extract.csv')

### Journal Entry ###
## 1777 - negative 'FX G/L'
## 1642 - positive 'FX G/L'
## 1641 - positive 'NS EUR Amount'per order
## 117 - negative 'NS EUR Amount' per order (Refunds)
# 680 - 'Fee'
# 742 - sum(CR) - sum(DR)
# 599 - No order in Netsuite (Sales & Refunds) ??

memo = 'Stripe 01-09.2020'
cols = ['External ID','Account Internal ID','DR','CR','Memo','Name','Region','Channel','Brand','Product','Technology']
journal_entry = pd.DataFrame(columns=cols)



for index,row in stripe_statement.iterrows() :
    payment_intent_id = row['payment_intent_id']
    gross = row['gross']
    fee = row['fee']
    reporting_cat = row['reporting_category']
    ns_order_id = row['NS Order ID']
    cust_internal_id = row['Internal ID']
    transaction_type = row['Type']
    ns_eur_amount = row['NS EUR Amount']
    fx_gl = row['FX G/L']
    sales_channel = str(row['Sales Channel'])
    brand = row['BRAND']
    sales_region = row['SALES REGION']
    if transaction_type in ['refund','dispute'] :
        journal_line = {'Account Internal ID':'117',
        'DR':abs(ns_eur_amount),
        'CR':0,
        'Memo':memo+' '+payment_intent_id,
        'Name':cust_internal_id,
        'Region':sales_region,
        'Channel':sales_channel,
        'Brand':brand}
        journal_entry = journal_entry.append(journal_line,ignore_index=True)
    else :
        journal_line = {'Account Internal ID':'1641',
        'DR':0,
        'CR':abs(ns_eur_amount),
        'Memo':memo+' '+payment_intent_id,
        'Name':cust_internal_id,
        'Region':sales_region,
        'Channel':sales_channel,
        'Brand':brand}
        journal_entry = journal_entry.append(journal_line,ignore_index=True)

acc_1777 = stripe_statement['FX G/L'][stripe_statement['FX G/L'] < 0].sum()
acc_1642 = stripe_statement['FX G/L'][stripe_statement['FX G/L'] >= 0].sum()
acc_680 = stripe_statement['fee'].sum()

journal_line_acc_1777 = {'Account Internal ID':'1777',
                        'DR':0,
                        'CR':abs(acc_1777),
                        'Memo':'Stripe Payments/Refunds'+' '+memo+' '+'Realized FX Gain',
                        'Region':'EMEA : Western Europe',
                        'Channel':'B2C : WebShop',
                        'Brand':'Corporate'}

journal_line_acc_1642 = {'Account Internal ID':'1642',
                        'DR':abs(acc_1642),
                        'CR':0,
                        'Memo':'Stripe Payments/Refunds'+' '+memo+' '+'Realized FX Loss',
                        'Region':'EMEA : Western Europe',
                        'Channel':'B2C : WebShop',
                        'Brand':'Corporate'}

journal_line_acc_680 = {'Account Internal ID':'680',
                        'DR':abs(acc_680),
                        'CR':0,
                        'Memo':'Stripe Payment fees'+' '+memo,
                        'Region':'EMEA : Western Europe',
                        'Channel':'B2C : WebShop',
                        'Brand':'Corporate'}

journal_entry = journal_entry.append([journal_line_acc_1777,journal_line_acc_1642,journal_line_acc_680],ignore_index=True)

acc_742 = journal_entry['DR'].sum() - journal_entry['CR'].sum()

journal_line_acc_742 = {'Account Internal ID':'742',
                        'DR':abs(acc_742),
                        'CR':0,
                        'Memo':'Cash Income'+' '+memo,
                        'Region':'EMEA : Western Europe',
                        'Channel':'B2C : WebShop',
                        'Brand':'Corporate'}

journal_entry = journal_entry.append(journal_line_acc_742,ignore_index=True)
journal_entry['External ID'],journal_entry['Product'],journal_entry['Technology'] = memo,'Corporate','Corporate'
journal_entry = journal_entry.set_index('External ID')
journal_entry.to_csv('journal_entry.csv')
