import pandas as pd
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection
import time
from tqdm import tqdm


class EasyEbayData:

    def __init__(self, apiId, keywords, max_pages=False):
        self.api = Connection(appid=apiId, config_file=None)
        assert isinstance(keywords, str), "'keywords' must be a string"
        self.keywords = keywords
        if max_pages is not False:
            assert isinstance(max_pages, int), "'max_pages' must be an integer if specified"
        self.max_pages = max_pages

    def unembed_ebay_item_data(self, list_of_item_dics):
        unembedded = []
        for ebay_item in list_of_item_dics:
            assert isinstance(ebay_item, dict), "The data should be a list of dictionaries, if it isn't the API has changed."
            unembedded_dict = dict()
            for key, val in ebay_item.items():
                if isinstance(val, dict):
                    for key2, val2 in val.items():
                        if isinstance(val2, dict):
                            for key3, val3 in val2.items():
                                unembedded_dict[key2 + '_' + key3] = val3
                        else:
                            unembedded_dict[key + '_' + key2] = val2
                else:
                    unembedded_dict[key] = val
            unembedded.append(unembedded_dict)
        return unembedded

    def test_connection(self):
        """Tests that an initial API connection is successful"""
        try:
            response = self.api.execute('findItemsAdvanced', {'keywords': self.keywords,
                                                              'paginationInput': {'pageNumber': 1}})
            assert response.reply.ack == 'Success'
            print('Successfully Connected to API!')
            return response
        except ConnectionError:
            print('Connection Error! Ensure that your API key was correctly entered.')
            return False

    def get_max_pages(self, response):
        """response comes from test_connection to access total pages without making another API call"""
        if not self.max_pages:  # Getting the total number of pages with listings
            pages2pull = int(response.reply.paginationOutput.totalPages)
        else:
            pages2pull = self.max_pages
        return pages2pull + 1

    def get_ebay_item_info(self):

        response = self.test_connection()

        if not response:
            return

        pages2pull = self.get_max_pages(response)

        all_items = []

        total_errors = 0

        for page in tqdm(range(1, pages2pull + 1)):
            time.sleep(1)
            response = self.api.execute('findItemsAdvanced', {'keywords': self.keywords,
                                                              'paginationInput': {'pageNumber': page}})
            if response.reply.ack == 'Success':
                data = response.dict()['searchResult']['item']
                all_items.extend(self.unembed_ebay_item_data(data))

            else:
                print('Unable to connect to page #: ', page)
                total_errors += 1
                if total_errors == 3:
                    print('API limit reached or pull finished. Pulled {} pages'.format(page - 3))
                    return pd.DataFrame(all_items)

        return pd.DataFrame(all_items)