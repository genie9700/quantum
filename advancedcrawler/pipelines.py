import json
import aiohttp
import asyncio
import logging
from scrapy.utils.project import get_project_settings

class SolrPipeline:
    def __init__(self, batch_size=10, solr_url=None, logger=None):
        self.items = []
        self.batch_size = batch_size
        self.solr_url = solr_url or get_project_settings().get('SOLR_URL')
        self.logger = logger or logging.getLogger(__name__)

    def process_item(self, item, spider):
        # Convert the Scrapy item to a dictionary
        item_dict = dict(item)

        # Append the item to the list of items
        self.items.append(item_dict)

        # If the batch size is reached, index to Solr asynchronously
        if len(self.items) >= self.batch_size:
            asyncio.ensure_future(self.index_to_solr())

        return item

    async def index_to_solr(self):
        if self.items:
            async with aiohttp.ClientSession() as session:
                headers = {'Content-type': 'application/json'}
                payload = json.dumps(self.items)
                try:
                    async with session.post(f'{self.solr_url}/update/json/docs?commit=true', data=payload,
                                            headers=headers) as response:
                        if response.status != 200:
                            # Handle error or retry logic
                            self.logger.error(f'Failed to index to Solr: HTTP {response.status}')
                        else:
                            # Clear items after successful indexing
                            self.items = []
                except aiohttp.ClientError as e:
                    # Handle client-side errors
                    self.logger.error(f'Aiohttp ClientError: {e}')
                except asyncio.TimeoutError:
                    # Handle timeout errors
                    self.logger.error('Timeout error occurred while indexing to Solr')
                except Exception as e:
                    # Handle other unexpected errors
                    self.logger.error(f'An unexpected error occurred: {e}')

    async def close_spider(self, spider):
        # Index any remaining items before closing spider
        await self.index_to_solr()
