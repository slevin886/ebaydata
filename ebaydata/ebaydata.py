import pandas as pd
from typing import List
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection as Finding


# TODO:
# TO Support In Future:
# findItemsByCategory
# findItemsByKeywords
# getHistograms
# getSearchKeywordsRecommendation
# Eventually use this: 'GetCategoryInfo' to get valid category ids
# findItemsByCategory (max: 3, will need to be specified separately for each one i)
# search variation:
# baseball card  (both words) baseball,card (exact phrase baseball card)
# (baseball,card) (items with either baseball or card)  baseball -card (baseball but NOT card)
# baseball -(card,star) (baseball but NOT card or star)


class EasyEbayData:

    def __init__(self, api_id: str, keywords: str, excluded_words: str, sort_order: str,
                 search_type: str = "findItemsByKeywords", wanted_pages: int = False,
                 usa_only: bool = True, min_price: float = 0.0, max_price: float = None):
        """
        A class that returns a clean data set of items for sale based on a keyword search from ebay
        :param api_id: eBay developer app's ID
        :param keywords: Keywords should be between 2 & 350 characters, not case sensitive
        :param wanted_pages: The number of desired pages to return w/ 100 items per page
        :param search_type: Search type, for now only findItemsbyKeywords accepted
        """
        self.api = Finding(appid=api_id, config_file=None)
        self.search_type = search_type
        self.keywords = keywords  # keywords only search item titles
        self.exclude_words = excluded_words
        self.wanted_pages = wanted_pages  # must be at least 1 & integer
        self.usa_only = True if usa_only else False
        self.min_price = min_price if min_price else 0.0
        self.max_price = max_price
        self.sort_order = sort_order
        self.search_url = ""  # will be the result url of the first searched page
        self.total_pages = 0  # the total number of available pages
        self.total_entries = 0  # the total number of items available given keywords
        if len(excluded_words) > 2:
            excluded_words = ",".join(word for word in excluded_words.split(" "))
            self.full_query = keywords + " -(" + excluded_words + ")"
        else:
            self.full_query = keywords
        self.item_filter = self._create_item_filter()

    def _create_item_filter(self):
        item_filter = list()
        item_filter.append({'name': 'MinPrice', 'value': self.min_price})
        if self.max_price and self.max_price > self.min_price:
            item_filter.append({'name': 'MaxPrice', 'value': self.max_price})
        if self.usa_only:
            item_filter.append({'name': 'LocatedIn', 'value': 'US'})
        return item_filter

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
            response = self.api.execute(self.search_type, {'keywords': self.full_query,
                                                           'paginationInput': {'pageNumber': 1,
                                                                               'entriesPerPage': 100},
                                                           'itemFilter': self.item_filter,
                                                           'sortOrder': self.sort_order})
            assert response.reply.ack == 'Success'
            print('Successfully Connected to API!')
            self.search_url = response.dict()['itemSearchURL']
            return response
        except ConnectionError:
            print('Connection Error! Ensure that your API key was correctly entered.')
            return "connection_error"
        except AssertionError:
            print('There are no results for that search!')
            return "no_results_error"

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

        if response in ["connection_error", "no_results_error"]:
            return response

        # Add initial items from test
        data = response.dict()['searchResult']['item']
        all_items.extend(self.unembed_ebay_item_data(data))

        pages2pull = self.get_wanted_pages(response)

        if pages2pull < 2:  # stop if only pulling one page or only one page exists
            return pd.DataFrame(all_items)

        total_errors = 0

        for page in range(2, pages2pull + 1):
            response = self.api.execute(self.search_type, {'keywords': self.full_query,
                                                           'paginationInput': {'pageNumber': page,
                                                                               'entriesPerPage': 100},
                                                           'itemFilter': self.item_filter,
                                                           'sortOrder': self.sort_order
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
