# Step 1 - create main data frame from Statement
import pandas as pd
stripe_statement = pd.read_csv('Stripe_Statement.csv',encoding='iso8859_15')
#           -> Add customer name from Orders
netsuite_orders = pd.read_csv('Netsuite_Stripe_Orders.csv',usecols=['Name','Webshop Payment Token']).rename(columns={'Webshop Payment Token':'payment_intent_id'})
stripe_statement = stripe_statement.merge(netsuite_orders,how='left',on='payment_intent_id')
stripe_statement['created'] = pd.to_datetime(stripe_statement.created,dayfirst=True)


#           -> Add Currency, FX Amount, EUR Amount from Payments
netsuite_payments = pd.read_csv('Netsuite_Stripe_Payments.csv',usecols=['Name','Currency: Name','Amount (Foreign Currency)','Amount']).rename(columns={'Currency: Name':'NS Currency','Amount (Foreign Currency)':'NS FX Amount','Amount':'NS EUR Amount'})

netsuite_refunds = pd.read_csv('Netsuite_Stripe_Refunds.csv',usecols=['Name','Currency: Name','Amount (Foreign Currency)','Amount']).rename(columns={'Currency: Name':'NS Currency','Amount (Foreign Currency)':'NS FX Amount','Amount':'NS EUR Amount'})

# Statement: sort by Name
# Payments: sort by Name
# if statement len name xy = payment len name xy
#   name xy 1 = payment xy 1
#   name xy 2 = payment xy 2 

# for index,row in stripe_statement.iterrows() :
#     if row['reporting_category'] == 'charge' :
#         for index2,row2 in netsuite_payments.iterrows() :
#             if row['Name'] == row2['Name'] :
#                 stripe_statement['NS FX Amount'].loc[index] = row2['NS FX Amount']

# stripe_statement_charge = stripe_statement[stripe_statement['reporting_category'] == 'charge']
# stripe_statement_refund = stripe_statement[stripe_statement['reporting_category'] == 'refund']
#
# stripe_statement_charge = stripe_statement_charge.merge(netsuite_payments,how='left',on='Name',validate='1:1')
# # stripe_statement_charge = stripe_statement_charge.groupby(['Name'])
#
# stripe_statement_refund = stripe_statement_refund.merge(netsuite_refunds,how='left',on='Name')
#
# stripe_statement = stripe_statement_charge.append(stripe_statement_refund)

# Step 2 - Add calculations and conditions (S - X)
stripe_statement['Stat vs NS FX'] = 0
stripe_statement['Stat vs NS EUR'] = 0
for index,row in stripe_statement.iterrows() :
    if row['customer_facing_currency'] == 'gbp' :
        stripe_statement['Stat vs NS FX'].loc[index] = row['FX Amount'] - row['NS FX Amount']
        stripe_statement['Stat vs NS EUR'].loc[index] = row['gross'] - row['NS FX Amount']
    else :
        stripe_statement['Stat vs NS FX'].loc[index] = 'EUR'
        stripe_statement['Stat vs NS EUR'].loc[index] = 'GBP'
