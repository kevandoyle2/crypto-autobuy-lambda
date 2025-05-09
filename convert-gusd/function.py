import json
import os
import boto3
import gemini

# Initialize Secrets Manager client
secrets_client = boto3.client('secretsmanager')

# Retrieve API keys from Secrets Manager
def get_api_keys():
    secret_name = "GeminiApiKeys"
    region_name = "us-east-1"

    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        public_key = secret['GEMINI_PUBLIC_KEY']
        private_key = secret['GEMINI_PRIVATE_KEY']
        return public_key, private_key
    except Exception as e:
        raise ValueError(f"Error retrieving secrets from AWS Secrets Manager: {str(e)}")

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

    try:
        # Retrieve API keys from Secrets Manager
        public_key, private_key = get_api_keys()
        
        _convertGUSDtoUSD(public_key, private_key)
        return {
            'statusCode': 200,
            'body': json.dumps('End of script')
            }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }