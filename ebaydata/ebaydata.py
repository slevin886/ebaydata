import pandas as pd
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection as Finding


# TODO: Reformat into base class with subclasses for keyword and category searches
# TODO: Add complete docstrings


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
        item_condition: str = None,
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
        self.pages_wanted: int = None  # must be at least 1 & integer
        self.usa_only = (
            True  # for now, only support us sellers and removing kwarg from init
        )
        self.min_price = min_price if min_price else 0.0
        self.max_price = max_price
        self.sort_order = sort_order
        self.listing_type = listing_type
        self.item_condition = item_condition
        self.get_category_info = get_category_info
        self.search_url = None  # will be the result url of the first searched page
        self.item_aspects = None  # dictionary of item features
        self.category_info = None  # dictionary of category id and subcategories
        self.largest_sub_category = None
        self.largest_category = None
        self.total_pages: int = None  # the total number of available pages
        self.total_entries: int = None  # the total number of items available given keywords (all categories)
        if excluded_words and len(excluded_words) > 2:
            excluded_words = ",".join(word for word in excluded_words.split(" "))
            self.full_query = keywords + " -(" + excluded_words + ")"
        else:
            self.full_query = keywords
        self.item_filter = self._create_item_filter()

    def __repr__(self):
        return f"[EasyEbayData] query: {self.full_query}"

    def _create_item_filter(self):
        if self.sort_order in ["BidCountMost", "BidCountFewest"]:
            if self.listing_type not in ["Auction", "AuctionWithBIN"]:
                print(
                    "Changing listing type to auction to support a sort order using bid count"
                )
                self.listing_type = (
                    "Auction"  # sort order without that listing type returns nothing
                )
        item_filter = list()
        item_filter.append({"name": "MinPrice", "value": self.min_price})
        if self.max_price and self.max_price > self.min_price:
            item_filter.append({"name": "MaxPrice", "value": self.max_price})
        if self.listing_type and self.listing_type != "All":
            item_filter.append({"name": "ListingType", "value": self.listing_type})
        if self.item_condition:
            item_filter.append({"name": "Condition", "value": self.item_condition})
        if self.usa_only:
            item_filter.append({"name": "LocatedIn", "value": "US"})
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
        )  # known doubles where want always show parent key
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

    def single_page_query(self, page_number=1, include_meta_data=True):
        """
        Tests that an initial API connection is successful and returns the initial raw response as a dictionary if
        successful else returns a string of the error that occurred. Should generally be the first search used.
        """
        output_selection = ["SellerInfo", "StoreInfo"]

        if include_meta_data:
            output_selection.extend(["AspectHistogram", "CategoryHistogram"])

        try:
            api = Finding(appid=self.api_id, config_file=None)
            response = api.execute(
                "findItemsByKeywords",
                {
                    "keywords": self.full_query,
                    "paginationInput": {
                        "pageNumber": page_number,
                        "entriesPerPage": 100,
                    },
                    "itemFilter": self.item_filter,
                    "sortOrder": self.sort_order,
                    "outputSelector": output_selection,
                },
            )
        except ConnectionError:
            print(
                "Connection Error! Ensure that your API key was correctly and you have web connectivity."
            )
            return "connection_error"
        try:
            assert response.reply.ack == "Success"
        except AssertionError:
            print(
                f"There is an API error, please check the parameters of your search or rate limit: {self.full_query}"
            )
            return "api_error"

        response = response.dict()

        if response["paginationOutput"]["totalPages"] == "0":
            print(f"There are no results for a search of: {self.full_query}")
            return "no_results_error"

        if include_meta_data:
            self.total_pages = int(response["paginationOutput"]["totalPages"])
            self.total_entries = int(response["paginationOutput"]["totalEntries"])
            self._clean_category_data(response)
        self.search_url = response["itemSearchURL"]
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

    def _get_pages_to_pull(self, response: dict, pages_wanted: int):
        """response comes from test_connection to access total pages without making another API call"""
        self.total_pages = int(response["paginationOutput"]["totalPages"])
        self.total_entries = int(response["paginationOutput"]["totalEntries"])
        if pages_wanted:
            # can't pull more than max pages or 100 total pages
            pages2pull = min([self.total_pages, pages_wanted, 100])
        else:
            pages2pull = min([self.total_pages, 100])
        return pages2pull

    def full_data_pull(self, pages_wanted: int = None):
        response = self.single_page_query()

        if isinstance(response, str):
            return response

        if self.get_category_info:
            self._clean_category_data(response)

        data = response["searchResult"]["item"]

        all_items = []

        all_items.extend([self.flatten_dict(i) for i in data])

        pages2pull = self._get_pages_to_pull(response, pages_wanted)

        if (
            pages2pull < 2
        ):  # stop if only pulling one/zero pages or only one page exists
            return pd.DataFrame(all_items)

        for page in range(2, pages2pull + 1):
            response = self.single_page_query(page_number=page, include_meta_data=False)
            if isinstance(response, str):
                print(f"Unable to connect to page #: {page}\bPulled {page -1 } pages.")
                return pd.DataFrame(all_items)
            else:
                data = response["searchResult"]["item"]
                all_items.extend([self.flatten_dict(i) for i in data])

        return pd.DataFrame(all_items)
