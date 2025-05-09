import json
import gemini_api as gemini

public_key = 'account-MZdSIrTlCgMdt2278U8H'
private_key = '4LPajVrWCb7iopoz4JqZ8CQXrvbq'

#This function converts all your GUSD to USD
def _convertGUSDtoUSD(pub_key, priv_key):
    gusd_balance = 0
    trader = gemini.PrivateClient(pub_key, priv_key)
    if(list((type['available'] for type in  trader.get_balance() if type['currency'] == 'GUSD'))):
        gusd_balance = str(list((type['available'] for type in  trader.get_balance() if type['currency'] == 'GUSD'))[0])
    #use "buy" to convert USD to GUSD
    #use "sell" to convert GUSD into USD
    #replace gusd_balance below to transfer a static amount, use gusd_balance to transfer all your GUSD to USD
    results = trader.wrap_order(gusd_balance, "sell")
    print(results)


def lambda_handler(event, context):
    _convertGUSDtoUSD(public_key, private_key)
    return {
        'statusCode': 200,
        'body': json.dumps('End of script')
    }