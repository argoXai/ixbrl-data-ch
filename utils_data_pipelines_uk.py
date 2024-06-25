from document_retrieval import *
from utils import *
import time
from datetime import datetime
import boto3

# Initialize the S3 client
s3 = boto3.client('s3')
# Specify the DynamoDB table for company_xhtml_data
table_xhtml_data = dynamodb.Table('company_xhtml_data')
table_profile = dynamodb.Table('company_profile')

def add_delay(delay):
    time.sleep(delay)

def check_for_xhtml_pipe(item):
    """
    Check if the given item contains an XHTML document.

    Parameters:
    item (dict): The item to check for XHTML document.

    Returns:
    bool: True if the item contains an XHTML document, False otherwise.
    """
    link_to_file = item["links"]["document_metadata"]
    response = make_get_request(link_to_file, headers=None)
    # print(response.json()['resources'])
    if "application/xhtml+xml" in response.json()["resources"]:
        # print(response.json())
        return True
    else:
        return False
    

def get_company_profile_pipe(company_number):
    """
    Retrieves the company information from the provided company number.

    Parameters
    ----------
    company_number : str
        The company number to retrieve the company information.

    Returns
    -------
    dict
        A dictionary containing the company information.
        Returns None if an exception occurs.
    """
    try:
        url = f"https://api.company-information.service.gov.uk/company/{company_number}"
        response = make_get_request(url, headers=None)
        company_info = response.json()
        return company_info

    except Exception as e:
        print(f"Error occurred while retrieving company profile information (get_company_profile_pipe): {e}")
        return None

def update_profile_table(company_id):
    try:
        human_readable_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        unix_timestamp = int(time.time())
        company_profile = get_company_profile_pipe(company_id)

        # Update the DynamoDB table with the new profile data and additional attributes
        response = table_profile.update_item(
            Key={'companyID': company_id},
            UpdateExpression="set profile=:p, last_updated=:lu, timestamp_unix=:ts",
            ExpressionAttributeValues={
                ':p': company_profile,
                ':lu': human_readable_date,
                ':ts': unix_timestamp
            },
            ReturnValues="UPDATED_NEW"
        )
        print(f"Updated profile for company ID: {company_id} with last_updated: {human_readable_date} and timestamp: {unix_timestamp}")
    except Exception as e:
        raise Exception(f"Error in update_profile_table for company ID {company_id}: {str(e)}") from e

def request_filling_history_pipe(company_number):
    """
    Makes a GET request to the Companies House API to retrieve the filing history of a company.

    Parameters
    ----------
    company_number : str, optional
        The company number for which the filing history is requested.

    Returns
    -------
    dict
        A JSON object containing a list of filings for the specified company.
    """
    try:
        # Construct the API endpoint URL with the specified company number FILLING HISTORY
        url = f"https://api.company-information.service.gov.uk/company/{company_number}/filing-history"

        response = make_get_request(url, headers=None)

        if response.status_code != 200:
            raise Exception(f"Failed to retrieve filing history for company number {company_number}. Status code: {response.status_code}")

        return response.json()
    except Exception as e:
        raise Exception(f"Error in request_filling_history_pipe: {str(e)}") from e

def exists_in_profile_table(companyID):
    try:
        response = table_profile.get_item(Key={'companyID': companyID})
        if 'Item' in response:
            return True
        else:
            return False
    except Exception as e:
        raise Exception(f"Error in exists_in_profile_table: {str(e)}") from e

def update_xhtml_table(company_id, history):
    try:
        print(f'Processing: {company_id}')
        add_delay(0.2)  # Introduce delay before each API call to avoid server overload
        filtered_history = [item for item in history['items'] if item.get('type') == 'AA']

        if not filtered_history:
            error_message = f"No 'AA' type items found in history for company ID: {company_id}"
            print(error_message)
            raise ValueError(error_message)

        for item in filtered_history:

            divider2()

            if check_for_xhtml_pipe(item):
                add_delay(0.2)
                # print(f'after check for xhtml')

                # Retrieve the URL of the documents
                url = retrieve_documents_url(item)
                add_delay(0.2)

                # Retrieve the json object containing the documents (pdf + xhtml possibly)
                response = make_get_request(url, headers=None)
                add_delay(0.2)
                url = response.json()["links"]["document"]

                # Retrieve the desired document
                headers = {"Accept": "application/xhtml+xml"}
                response = make_get_request(url, headers)
                add_delay(0.2)
                
                # The response should be a binary of the xhtml file
                xhtml_content = response.content.decode("utf-8")
                
                # print(f'before save to s3')
                # Save xhtml content to S3
                try:
                    made_up_date = item["description_values"]["made_up_date"]
                    print(f'Processing filing: {made_up_date}')

                    file_name = f"{made_up_date}"
                    s3_key = f'xhtml_data/{company_id}/{file_name}'
                    s3_url = f's3://company-house-uk/{s3_key}'
                    # print(f'after s3_url')
                    
                    s3.put_object(Bucket='company-house-uk', Key=s3_key, Body=xhtml_content.encode(), ContentType='application/xhtml+xml')
                    # print(f'after s3 put')

                except Exception as e:
                    print(f"Failed to upload XHTML for company ID: {company_id}. Error in S3 upload: {str(e)}")
                    continue
                
                # Save pointers to DynamoDB in company_xhtml_data
                try:
                    timestamp_unix = int(time.time())
                    timestamp_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    table_xhtml_data.put_item(
                        Item={
                            'companyID': company_id,
                            'year': made_up_date,
                            's3key': s3_key,
                            's3url': s3_url,
                            'timestamp_date': timestamp_date,
                            'timestamp_unix': timestamp_unix,
                        }
                    )
                except Exception as e:
                    print(f"Failed to update company_xhtml_data for company ID: {company_id}. Error in DynamoDB update: {str(e)}")
                    continue
            else:
                print(f"No XHTML data available for this item")

    except Exception as e:
        raise Exception(f"Error in update_xhtml_table for company ID {company_id}: {str(e)}") from e

def divider():
    print("*"*150)

def divider2():
    print("-"*100)

def identify_latest_aa_filing_micro_status(company_number, history):
    try:
        aa_items = [item for item in history['items'] if item.get('type') == 'AA']
        if not aa_items:
            return None

        aa_items_sorted = sorted(aa_items, key=lambda x: x.get('action_date', ''), reverse=True)
        latest_aa_item = aa_items_sorted[0]
        return 'micro-entity' in latest_aa_item.get('description', '')
    except Exception as e:
        raise Exception(f"Error in identify_latest_aa_filing_micro_status for company number {company_number}: {str(e)}") from e

def collect_last_accounts_info(dynamodb_resource):
    try:
        table = dynamodb_resource.Table('company_profile')
        last_accounts_info = {}
        response = table.scan()
        items = response.get('Items', [])
        for item in items:
            company_id = item.get('companyID')
            profile = item.get('profile', {})
            last_acc_made_up_to = profile.get('accounts', {}).get('last_accounts', {}).get('made_up_to', None)
            if last_acc_made_up_to:
                last_accounts_info[company_id] = last_acc_made_up_to

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items = response.get('Items', [])
            for item in items:
                company_id = item.get('companyID')
                profile = item.get('profile', {})
                last_acc_made_up_to = profile.get('accounts', {}).get('last_accounts', {}).get('made_up_to', None)
                if last_acc_made_up_to:
                    last_accounts_info[company_id] = last_acc_made_up_to
        return last_accounts_info
    except Exception as e:
        raise Exception(f"Error in collect_last_accounts_info: {str(e)}") from e

def collect_company_ids(dynamodb_resource):
    try:
        table = dynamodb_resource.Table('company_profile')
        company_ids = []
        response = table.scan()
        items = response.get('Items', [])
        for item in items:
            company_ids.append(item['companyID'])

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items = response.get('Items', [])
            for item in items:
                company_ids.append(item['companyID'])
        
        return company_ids
    except Exception as e:
        raise Exception(f"Error in collect_company_ids: {str(e)}") from e

def get_single_last_accounts_info(dynamodb_resource, company_id):
    try:
        table = dynamodb_resource.Table('company_profile')
        response = table.get_item(
            Key={
                'companyID': company_id
            }
        )
        item = response.get('Item', None)
        if item:
            profile = item.get('profile', {})
            last_acc_made_up_to = profile.get('accounts', {}).get('last_accounts', {}).get('made_up_to', None)
            return last_acc_made_up_to
    except Exception as e:
        raise Exception(f"Error in get_single_last_accounts_info for company ID {company_id}: {str(e)}") from e

    return None
