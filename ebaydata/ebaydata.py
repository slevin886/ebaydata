import pandas as pd
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection as Finding
from tqdm import tqdm


# TODO:
# TO Support In Future:
# findItemsByCategory
# findItemsByKeywords
# getHistograms
# getSearchKeywordsRecommendation

# Include itemFilter as dict like 'itemFilter':{'name':'SoldItemsOnly', 'value': 'true'},

# search variation:
# baseball card  (both words) baseball,card (exact phrase baseball card)
# (baseball,card) (items with either baseball or card)  baseball -card (baseball but NOT card)
# baseball -(card,star) (baseball but NOT card or star)


class EasyEbayData:

    def __init__(self, apiId, keywords, wanted_pages=False, searchType="findItemsAdvanced",
                 categoryId=None, itemFilter=None):
        self.api = Finding(appid=apiId, config_file=None)
        self.totalEntries = 0
        self.searchType = searchType
        self.keywords = keywords
        self.wanted_pages = wanted_pages
        # TO INCLUDE LATER
        # below will be a list for findItemsByCategory (max: 3, will need to be specified separately for each one)
        self.categoryId = categoryId
        self.itemFilter = itemFilter

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
            response = self.api.execute(self.searchType, {'keywords': self.keywords,
                                                          'paginationInput': {'pageNumber': 1,
                                                                              'entriesPerPage': 100},
                                                          'itemFilter': {}})
            assert response.reply.ack == 'Success'
            print('Successfully Connected to API!')
            return response
        except ConnectionError:
            print('Connection Error! Ensure that your API key was correctly entered.')
            return False

    def get_wanted_pages(self, response):
        """response comes from test_connection to access total pages without making another API call"""
        totalPages = int(response.reply.paginationOutput.totalPages)
        self.totalEntries = int(response.reply.paginationOutput.entriesPerPage)
        if self.wanted_pages:
            # can't pull more than max pages
            pages2pull = min([totalPages, self.wanted_pages])
        else:
            pages2pull = totalPages
        return pages2pull + 1

    def get_ebay_item_info(self):

        response = self.test_connection()

        if not response:
            return

        pages2pull = self.get_wanted_pages(response)

        all_items = []

        total_errors = 0

        for page in tqdm(range(1, pages2pull)):
            response = self.api.execute(self.searchType, {'keywords': self.keywords,
                                                          'paginationInput': {'pageNumber': page,
                                                                              'entriesPerPage': 100},
                                                          'itemFilter': {}})
            if response.reply.ack == 'Success':
                data = response.dict()['searchResult']['item']
                all_items.extend(self.unembed_ebay_item_data(data))

            else:
                print('Unable to connect to page #: ', page)
                total_errors += 1
                if total_errors == 2:
                    print('API limit reached or pull finished. Pulled {} pages'.format(page - 2))
                    return pd.DataFrame(all_items)

        return pd.DataFrame(all_items)
