import re
import singer
from tap_linkedin_ads.streams import STREAMS, write_bookmark
from tap_linkedin_ads.transform import transform_json

LOGGER = singer.get_logger()

LOOKBACK_WINDOW = 7
DATE_WINDOW_SIZE = 30 # days
PAGE_SIZE = 100

def update_currently_syncing(state, stream_name):
    """
    Currently syncing sets the stream currently being delivered in the state.
    If the integration is interrupted, this state property is used to identify the
    starting point to continue from.

    Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
    """
    if (stream_name is None) and ('currently_syncing' in state):
        # Remove the existing currently_syncing stream from the state for the complete sync
        del state['currently_syncing']
    else:
        # Set currently_syncing stream
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

def get_streams_to_sync(selected_streams):
    """
    Get lists of streams to call the sync method.
    For children, ensure that dependent parent_stream is included even if it is not selected.
    """
    streams_to_sync = []

    # Loop thru all selected streams
    for stream_name in selected_streams:
        stream_obj = STREAMS[stream_name]
        # If the stream has a parent_stream, then it is a child stream
        parent_stream = hasattr(stream_obj, 'parent') and stream_obj.parent

        # Append selected parent streams
        if not parent_stream:
            streams_to_sync.append(stream_name)
        else:
            # Append un-selected parent streams of selected children
            if parent_stream not in selected_streams and parent_stream not in streams_to_sync:
                streams_to_sync.append(parent_stream)

    return streams_to_sync

def get_page_size(config):
    """
    Get page size from config.
    Return the default value if an empty string is given and raise an exception if an invalid value is given.
    """
    page_size = config.get('page_size', PAGE_SIZE)
    if page_size == "":
        return PAGE_SIZE
    try:
        if isinstance(page_size, float):
            raise Exception

        page_size = int(page_size)
        if page_size <= 0:
            # Raise an exception if negative page_size is given in the config.
            raise Exception
        return page_size
    except Exception:
        raise Exception("The entered page size ({}) is invalid".format(page_size))

def fetch_all_accounts(client):
    """
    Fetch all ad accounts from LinkedIn API when accounts are not configured.
    Returns a list of account IDs.
    """
    LOGGER.info('No accounts configured, fetching all available ad accounts')
    account_list = []
    

    url = 'https://api.linkedin.com/rest/adAccounts?q=search&pageSize=1000'
    next_url = url

    while next_url:
        LOGGER.info('Fetching accounts from: %s', next_url)
        data = client.get(url=next_url, endpoint='accounts')
        
        # Transform the data using the same transform_json function used in normal sync
        # This ensures URNs are converted to numeric IDs, consistent with configured accounts
        if 'elements' in data:
            transformed_data = transform_json(data, 'accounts')
            for account in transformed_data.get('elements', []):
                account_id = account.get('id')
                if account_id:
                    # account_id should now be numeric after transform_json processing
                    account_list.append(str(account_id))
        
        # Check for next page
        next_page_token = data.get('metadata', {}).get('nextPageToken', None)
        if next_page_token:
            if 'pageToken=' in next_url:
                next_url = re.sub(r'pageToken=[^&]+', 'pageToken={}'.format(next_page_token), next_url)
            else:
                next_url = next_url + "&pageToken={}".format(next_page_token)
        else:
            next_url = None
    
    LOGGER.info('Fetched %s ad accounts', len(account_list))
    return account_list

def sync(client, config, catalog, state):
    """
    sync selected streams.
    """
    start_date = config['start_date']
    page_size = get_page_size(config)

    if config.get('date_window_size'):
        LOGGER.info('Using non-standard date window size of %s', config.get('date_window_size'))
        date_window_size = int(config.get('date_window_size'))
    else:
        date_window_size = DATE_WINDOW_SIZE
        LOGGER.info('Using standard date window size of %s', DATE_WINDOW_SIZE)

    # Get ALL selected streams from catalog
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: %s', selected_streams)

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: %s', last_stream)


    stream_to_sync = get_streams_to_sync(selected_streams)

    if config.get("accounts"):
        account_list = config['accounts'].replace(" ", "").split(",")
        LOGGER.info('Using configured accounts: %s', account_list)
    else:
        account_list = fetch_all_accounts(client)
        if not account_list:
            LOGGER.warning('No ad accounts found for this LinkedIn Authorization.')

    # Loop through all `stream_to_sync` streams
    for stream_name in stream_to_sync:
        stream_obj = STREAMS[stream_name]()

        # Add appropriate account_filter query parameters based on account_filter type
        account_filter = stream_obj.account_filter
        if account_list and account_filter is not None:
            params = stream_obj.params
            if account_filter == 'search_id_values_param':
                # Convert account IDs to URN format
                urn_list = ["urn%3Ali%3AsponsoredAccount%3A{}".format(account_id) for account_id in account_list]
                # Create the query parameter string
                param_value = "(id:(values:List({})))".format(','.join(urn_list))
                params['search'] = param_value
            elif account_filter == 'accounts_param':
                for idx, account in enumerate(account_list):
                    params['accounts[{}]'.format(idx)] = \
                        'urn:li:sponsoredAccount:{}'.format(account)
            stream_obj.params = params
        elif account_filter == "accounts_param" and not account_list:
            raise Exception("No ad accounts found for this LinkedIn Authorization. Cannot sync stream {}.".format(stream_name))
        # Update params of specific stream


        LOGGER.info('START Syncing: %s', stream_name)
        update_currently_syncing(state, stream_name)

        # Write schema for parent streams
        if stream_name in selected_streams:
            stream_obj.write_schema(catalog)

        total_records, max_bookmark_value = stream_obj.sync_endpoint(
            client=client, catalog=catalog,
            state=state, page_size=page_size,
            start_date=start_date,
            selected_streams=selected_streams,
            date_window_size=date_window_size,
            account_list=account_list)

        # Write parent stream's bookmarks
        if stream_obj.replication_keys and stream_name in selected_streams:
            write_bookmark(state, max_bookmark_value, stream_name)

        update_currently_syncing(state, None)
        LOGGER.info('Synced: %s, total_records: %s', stream_name, total_records)
        LOGGER.info('FINISHED Syncing: %s', stream_name)
