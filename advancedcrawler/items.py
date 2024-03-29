# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AdvancedcrawlerItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    meta_description = scrapy.Field()
    main_content = scrapy.Field()
    favicon_url = scrapy.Field()
    entities = scrapy.Field()
    sentiment = scrapy.Field()
    site_name = scrapy.Field()
    language = scrapy.Field()
    publication_date = scrapy.Field()
    internal_links = scrapy.Field()
    heading_tags = scrapy.Field()
    security_trustworthiness = scrapy.Field()
    semantic_markup = scrapy.Field()
    keywords = scrapy.Field()
    named_entities = scrapy.Field()
    summary = scrapy.Field()
    readability_scores = scrapy.Field()
    page_load_time = scrapy.Field()
    num_outbound_links = scrapy.Field()
    page_size = scrapy.Field()
    response_headers = scrapy.Field()
    user_interaction_elements = scrapy.Field()
    ad_networks = scrapy.Field()
    structured_markup_errors = scrapy.Field()
    page_rank = scrapy.Field()
    text = scrapy.Field()
    author = scrapy.Field()
    tags = scrapy.Field()
    # Add more fields as needed