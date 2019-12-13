import pandas as pd
from typing import List
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection as Finding
from concurrent.futures import ThreadPoolExecutor


# search variation:
# baseball card  (both words) baseball,card (exact phrase baseball card)
# (baseball,card) (items with either baseball or card)  baseball -card (baseball but NOT card)
# baseball -(card,star) (baseball but NOT card or star)

# TODO: implement exact match search
# TODO: separate into data class and search class

class EasyEbayData:
    def __init__(
        self,
        api_id: str,
        keywords: str,
        excluded_words: str = None,
        sort_order: str = "BestMatch",
        get_category_info: bool = True,
        listing_type: str = None,
        min_price: float = 0.0,
        max_price: float = None,
        category_id: str = None,
        item_condition: str = None,
        *args,
        **kwargs,
    ):
        """
        A class that returns a clean data set of items for sale based on a keyword search from ebay. After
        instantiation, call 'full_data_pull' method with the number of pages wanted to collect data and return it
        as a pandas dataframe.
        :param api_id: eBay developer app's ID
        :param keywords: Keywords should be between 2 & 350 characters, not case sensitive
        :param listing_type: A string for listing type (Auction, etc.) or None to search all
        :param item_condition: A string representing the item condition code
        :param get_category_info: A bool, if true, collects item aspects and category information
        """
        self.api_id = api_id
        self.keywords = keywords  # keywords only search item titles
        self.exclude_words = excluded_words
        self.min_price = min_price if min_price else 0.0
        self.max_price = max_price
        self.category_id = category_id
        self.sort_order = sort_order
        self.listing_type = listing_type
        self.item_condition = item_condition
        self.get_category_info = get_category_info
        self.search_url = ""  # will be the result url of the first searched page
        self.item_aspects = dict()  # dictionary of item features
        self.category_info = dict()  # dictionary of category id and subcategories
        self.largest_sub_category = ""
        self.largest_category = ""
        self.total_pages: int = 0  # the total number of available pages
        self.total_entries: int = 0  # the total number of items available given keywords (all categories)
        if excluded_words and len(excluded_words) > 2:
            excluded_words = ",".join(word for word in excluded_words.split(" "))
            self.full_query = keywords + " -(" + excluded_words + ")"
        else:
            self.full_query = keywords
        self.item_filter = self._create_item_filter()

    def __repr__(self):
        return f"[EasyEbayData] query: {self.full_query}"

    def _create_item_filter(self) -> List[dict]:
        """
        Sets the search filters into the appropriate format where necessary
        :return: List of dictionaries where each dictionary is a filter
        """
        if self.sort_order in ["BidCountMost", "BidCountFewest"]:
            if self.listing_type not in ["Auction", "AuctionWithBIN"]:
                print(
                    "Changing listing type to auction to support a sort order using bid count"
                )
                self.listing_type = (
                    "Auction"  # sort order without that listing type returns nothing
                )
        item_filter = list()
        item_filter.extend(
            [
                {"name": "MinPrice", "value": self.min_price},
                {"name": "LocatedIn", "value": "US"},
            ]  # only looks at US based sellers
        )
        if self.max_price and self.max_price > self.min_price:
            item_filter.append({"name": "MaxPrice", "value": self.max_price})
        if self.listing_type and self.listing_type != "All":
            item_filter.append({"name": "ListingType", "value": self.listing_type})
        if self.item_condition:
            item_filter.append({"name": "Condition", "value": self.item_condition})
        return item_filter

    def flatten_dict(self, item, acc=None, parent_key="", sep="_"):
        """
        The ebay API returns items as nested dictionaries, this recursive function flattens them
        :param item: dictionary for individual item
        :param acc: a dictionary that is used to pass through keys
        :param parent_key: nested parent key in dictionary
        :param sep: separates parent & nested keys if necessary
        :return: flat item dictionary
        """
        double_keys = (
            "_currencyId",
            "value",
        )  # known doubles where always show parent key
        final = dict() if acc is None else acc
        for key, val in item.items():
            if isinstance(val, dict):
                self.flatten_dict(val, final, parent_key=key)
            else:
                if key in final or key in double_keys:
                    final[parent_key + sep + key] = val
                else:
                    final[key] = val
        return final

    def clean_category_info(self, category):
        """
        Executes once from the test connection function to retrieve the categories that returned items
        belong to. Also sets attributes that reveal largest category and sub category.
        :param category: response['categoryHistogramContainer'] from the response dictionary object
        :return: Dictionary of categories and their counts
        """
        try:
            largest = category["categoryHistogram"][0]
            self.largest_category = [largest["categoryName"], largest["count"]]
            sub = largest["childCategoryHistogram"][0]
            self.largest_sub_category = [sub["categoryName"], sub["count"]]
        except (IndexError, KeyError):
            print("No subcategories for search")
            pass
        clean_categories = {}
        for cat in category["categoryHistogram"]:
            clean_categories[cat["categoryName"]] = {
                "categoryId": cat["categoryId"],
                "count": cat["count"],
            }
        return clean_categories

    @staticmethod
    def clean_aspect_dictionary(aspects):
        """
        There is also a second key 'domainDisplayName' for these aspects
        :param aspects: dictionary of item aspects
        """
        all_aspects = {}
        for asp in aspects["aspect"]:
            sub_aspect = {}
            for name in asp["valueHistogram"]:
                sub_aspect[name["_valueName"]] = int(name["count"])
            all_aspects[asp["_name"]] = sub_aspect
        return all_aspects

    def create_search_parameters(self, page_number, include_meta_data):
        parameters = dict(
            keywords=self.full_query,
            paginationInput=dict(pageNumber=page_number, entriesPerPage=100),
            itemFilter=self.item_filter,
            sortOrder=self.sort_order,
            outputSelector=["SellerInfo", "StoreInfo"],
        )
        if include_meta_data:
            parameters["outputSelector"].extend(
                ["AspectHistogram", "CategoryHistogram"]
            )

        if self.category_id:
            parameters["categoryId"] = self.category_id
        return parameters

    def single_page_query(self, page_number=1, include_meta_data=True, return_df=False):
        """
        Tests that an initial API connection is successful and returns a list of unnested ebay item dictionaries .
        If unsuccessful returns a string of the error that occurred.
        """
        parameters = self.create_search_parameters(page_number, include_meta_data)
        api = Finding(appid=self.api_id, config_file=None, https=True)
        try:
            response = api.execute("findItemsAdvanced", parameters)
            assert response.reply.ack == "Success"
        except ConnectionError:
            message = "Connection Error! Ensure that your API key was correctly and you have web connectivity."
            print(message)
            return message
        except AssertionError:
            try:
                message = response.dict()["errorMessage"]["error"]["message"]
            except KeyError:
                message = (
                    "There is an API error, check your rate limit or search parameters"
                )
            print(message)
            return message

        response = response.dict()

        if response["paginationOutput"]["totalPages"] == "0":
            message = f"There are no results for a search of: {self.full_query}"
            print(message)
            return message

        if include_meta_data:
            self._clean_category_data(response)

        # Eventually don't want to run these each time... need to check follow through
        self.total_entries = int(response["paginationOutput"]["totalEntries"])
        self.total_pages = int(response["paginationOutput"]["totalPages"])
        self.search_url = response["itemSearchURL"]

        response = [self.flatten_dict(i) for i in response["searchResult"]["item"]]
        if return_df:
            return pd.DataFrame(response)
        return response

    def _clean_category_data(self, response):
        try:
            self.category_info = self.clean_category_info(
                response["categoryHistogramContainer"]
            )
        except KeyError:
            print(f"There are no categories for a search of: {self.full_query}")
        try:
            self.item_aspects = self.clean_aspect_dictionary(
                response["aspectHistogramContainer"]
            )
        except KeyError:
            print(f"There are no aspects for a search of: {self.full_query}")

    def _get_pages_to_pull(self, pages_wanted: int = None):
        """
        A helper function if using full_data_pull that returns the number of pages available to pull
        up to the ebay API limit or the max pages wanted by the user.
        :param pages_wanted: the total pages wanted by the user
        :return: the number of pages to pull as integer
        """
        if pages_wanted:
            # can't pull more than max pages or 100 total pages
            return min([self.total_pages, pages_wanted, 100])
        else:
            return min([self.total_pages, 100])

    def full_data_pull(self, pages_wanted: int = None, include_meta_data=False):
        response = self.single_page_query(include_meta_data=include_meta_data)

        if isinstance(response, str):
            raise RuntimeError(response)

        all_items = []

        all_items.extend(response)

        pages2pull = self._get_pages_to_pull(pages_wanted)

        if pages2pull < 2:
            return pd.DataFrame(all_items)

        for page in range(2, pages2pull + 1):
            response = self.single_page_query(page_number=page, include_meta_data=False)
            if isinstance(response, str):
                print(
                    f"Unable to connect to page #: {page}\bPulled { page - 1 } pages."
                )
                return pd.DataFrame(all_items)
            else:
                all_items.extend(response)

        return pd.DataFrame(all_items)

    def _async_pull(self, page_number):
        parameters = self.create_search_parameters(
            page_number=page_number, include_meta_data=False
        )
        api = Finding(appid=self.api_id, config_file=None, https=True)
        try:
            result = api.execute("findItemsAdvanced", parameters)
            if result.reply.ack == "Success":
                return [
                    self.flatten_dict(i) for i in result.dict()["searchResult"]["item"]
                ]
            return list()
        except ConnectionError as error:
            print(error)
            return list()

    def run_async(self, pages_wanted=1, max_workers=10, return_df=True, start_page=1):
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = pool.map(
                self._async_pull, [i for i in range(start_page, pages_wanted + 1)]
            )
        data = [item for pull in results for item in pull if pull]
        if return_df:
            return pd.DataFrame(data)
        return data
