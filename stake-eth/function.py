import json
import os
import boto3
from shared.gemini_client import GeminiClient

# Initialize SSM client
ssm_client = boto3.client('ssm')

def get_api_keys():
    try:
        response = ssm_client.get_parameter(
            Name='GeminiApiKeys',
            WithDecryption=True
        )
        secret = json.loads(response['Parameter']['Value'])
        public_key = secret['API key']
        private_key = secret['API Secret']
        return public_key, private_key
    except Exception as e:
        raise ValueError(f"Error retrieving API keys from AWS SSM Parameter Store: {str(e)}")

def _stakeEthereum():
    public_key, private_key = get_api_keys()
    gemini = GeminiClient(public_key, private_key)

    # Step 1: Get available ETH balance
    try:
        balances = gemini.get_balance()
    except Exception as e:
        error_message = f"Failed to retrieve balances: {str(e)}"
        print(error_message)
        return {"error": error_message}

    eth_balance = 0.0
    for asset in balances:
        if asset['currency'] == 'ETH':
            eth_balance = float(asset['available'])
            break
    print(f"ETH Available Balance: {eth_balance}")

    if eth_balance <= 0:
        return {"message": "There is no ETH available to stake in your account"}

    # Step 2: Fetch staking rates to get providerId for ETH
    try:
        staking_rates = gemini.get_staking_rates()
    except Exception as e:
        error_message = f"Failed to retrieve staking rates: {str(e)}"
        print(error_message)
        return {"error": error_message}

    eth_provider_id = None
    # The response is a dict with a UUID key mapping to another dict with currency details
    if staking_rates:
        # Get the first (and likely only) UUID key
        uuid_key = next(iter(staking_rates), None)
        if uuid_key:
            rates_by_currency = staking_rates[uuid_key]
            # Look for ETH details
            if "ETH" in rates_by_currency:
                eth_provider_id = rates_by_currency["ETH"].get('providerId')

    if not eth_provider_id:
        return {"error": "Could not find providerId for ETH staking"}

    print(f"ETH Staking Provider ID: {eth_provider_id}")

    # Step 3: Stake all available ETH
    staking_payload = {
        "currency": "ETH",
        "amount": str(eth_balance),
        "providerId": eth_provider_id
    }

    try:
        result = gemini.stake_assets(staking_payload)
        print(f"Staking Result: {result}")
        return result
    except Exception as e:
        print(f"Error staking ETH: {str(e)}")
        return {"error": str(e)}

def lambda_handler(event, context):
    try:
        result = _stakeEthereum()
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }