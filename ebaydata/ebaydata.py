import pandas as pd
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection as Finding


# TODO:
# TO Support In Future:
# findItemsByCategory
# findItemsByKeywords
# getHistograms
# getSearchKeywordsRecommendation
# Eventually use this: 'GetCategoryInfo' to get valid category ids
# Include itemFilter as dict like 'itemFilter':[{'name':'SoldItemsOnly', 'value': 'true'}],

# search variation:
# baseball card  (both words) baseball,card (exact phrase baseball card)
# (baseball,card) (items with either baseball or card)  baseball -card (baseball but NOT card)
# baseball -(card,star) (baseball but NOT card or star)


class EasyEbayData:

    def __init__(self, api_id, keywords, wanted_pages=False, search_type="findItemsByKeywords",
                 category_id=None, item_filter=False):
        self.api = Finding(appid=api_id, config_file=None)
        self.search_type = search_type
        self.keywords = keywords
        self.total_pages = 0
        self.total_entries = 0
        self.wanted_pages = wanted_pages  # must be at least 1 & integer
        self.item_filter = item_filter  # should be a list of dictionaries
        self.search_url = ""
        # TO INCLUDE LATER
        # below will be a list for findItemsByCategory (max: 3, will need to be specified separately for each one)
        self.category_id = category_id

    def unembed_ebay_item_data(self, list_of_item_dics):
        unembedded = []
        for ebay_item in list_of_item_dics:
            assert isinstance(ebay_item, dict), "The data should be returning a list of dictionaries."
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
            # Might simplify this
            response = self.api.execute(self.search_type, {'keywords': self.keywords,
                                                           'paginationInput': {'pageNumber': 1,
                                                                               'entriesPerPage': 100},
                                                           'itemFilter': self.item_filter})
            assert response.reply.ack == 'Success'
            print('Successfully Connected to API!')
            self.search_url = response.dict()['itemSearchURL']
            return response
        except ConnectionError:
            print('Connection Error! Ensure that your API key was correctly entered.')
            return False

    def get_wanted_pages(self, response):
        """response comes from test_connection to access total pages without making another API call"""
        self.total_pages = int(response.reply.paginationOutput.totalPages)
        self.total_entries = int(response.reply.paginationOutput.totalEntries)
        if self.wanted_pages:
            # can't pull more than max pages
            pages2pull = min([self.total_pages, self.wanted_pages])
        else:
            pages2pull = self.total_pages
        return pages2pull

    def get_ebay_item_info(self):
        all_items = []

        response = self.test_connection()

        if not response:
            return None

        # Add initial items from test
        data = response.dict()['searchResult']['item']
        all_items.extend(self.unembed_ebay_item_data(data))

        pages2pull = self.get_wanted_pages(response)

        if pages2pull < 2:  # stop if only pulling one page or only one page exists
            return pd.DataFrame(all_items)

        total_errors = 0

        for page in range(2, pages2pull + 1):
            response = self.api.execute(self.search_type, {'keywords': self.keywords,
                                                           'paginationInput': {'pageNumber': page,
                                                                               'entriesPerPage': 100},
                                                           'itemFilter': self.item_filter,
                                                           })
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
