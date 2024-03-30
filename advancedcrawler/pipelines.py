# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import json
import aiohttp
import asyncio
from itemadapter import ItemAdapter
from scrapy.utils.project import get_project_settings


class SolrPipeline:
    def __init__(self):
        self.items = []
        self.solr_url = get_project_settings().get('SOLR_URL')

    def process_item(self, item, spider):
        # Convert the Scrapy item to a dictionary
        item_dict = dict(item)

        # Extracting necessary fields from the dictionary
        url = item_dict.get("url")
        title = item_dict.get("title")
        meta_description = item_dict.get("meta_description")
        entities = item_dict.get("entities")
        sentiment = item_dict.get("sentiment")
        site_name = item_dict.get("site_name")
        language = item_dict.get("language")
        publication_date = item_dict.get("publication_date")
        author = item_dict.get("author")
        internal_links = item_dict.get("internal_links")
        heading_tags = item_dict.get("heading_tags")
        security_trustworthiness = item_dict.get("security_trustworthiness")
        semantic_markup = item_dict.get("semantic_markup")
        keywords = item_dict.get("keywords")
        named_entities = item_dict.get("named_entities")
        summary = item_dict.get("summary")
        readability_scores = item_dict.get("readability_scores")
        page_load_time = item_dict.get("page_load_time")
        num_outbound_links = item_dict.get("num_outbound_links")
        page_size = item_dict.get("page_size")
        response_headers = item_dict.get("response_headers")
        user_interaction_elements = item_dict.get("user_interaction_elements")
        ad_networks = item_dict.get("ad_networks")
        structured_markup_errors = item_dict.get("structured_markup_errors")
        page_rank = item_dict.get("page_rank")

        # Create a JSON object for the item
        solr_item = {
            "url": url,
            "title": title,
            "meta_description": meta_description,
            "entities": entities,
            "sentiment": sentiment,
            "site_name": site_name,
            "language": language,
            "publication_date": publication_date,
            "author": author,
            "internal_links": internal_links,
            "heading_tags": heading_tags,
            "security_trustworthiness": security_trustworthiness,
            "semantic_markup": semantic_markup,
            "keywords": keywords,
            "named_entities": named_entities,
            "summary": summary,
            "readability_scores": readability_scores,
            "page_load_time": page_load_time,
            "num_outbound_links": num_outbound_links,
            "page_size": page_size,
            "response_headers": response_headers,
            "user_interaction_elements": user_interaction_elements,
            "ad_networks": ad_networks,
            "structured_markup_errors": structured_markup_errors,
            "page_rank": page_rank
        }

        # Append the JSON object to the list of items
        self.items.append(solr_item)

        # Index to Solr asynchronously
        asyncio.ensure_future(self.index_to_solr())

        return item

    async def index_to_solr(self):
        if self.items:
            async with aiohttp.ClientSession() as session:
                headers = {'Content-type': 'application/json'}
                payload = json.dumps(self.items)
                async with session.post(f'{self.solr_url}/update/json/docs?commit=true', data=payload,
                                        headers=headers) as response:
                    if response.status != 200:
                        # Handle error or retry logic
                        pass

            # Clear items after indexing
            self.items = []

    async def close_spider(self, spider):
        # Index any remaining items before closing spider
        await self.index_to_solr()

