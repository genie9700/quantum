import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from w3lib.html import get_base_url
# from langdetect import detect_langs
from datetime import datetime
from urllib.parse import urlparse
from urllib.parse import urljoin
from scrapy.exceptions import CloseSpider
from scrapy.http import HtmlResponse
import ssl
import socket
import random
import time
import re
import langid
from rake_nltk import Rake
import spacy
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.text_rank import TextRankSummarizer

from sumy.parsers.html import HtmlParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words
from sumy.summarizers.lsa import LsaSummarizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from readability import Readability
from readability.exceptions import ReadabilityException
# import nltk
from textblob import TextBlob
import logging
import json
import networkx as nx
from scrapy.crawler import CrawlerProcess
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from transformers import pipeline
from nltk.corpus import stopwords
from w3lib.html import remove_tags
from nltk.tokenize import word_tokenize
from nltk.tokenize import sent_tokenize
from nltk.cluster.util import cosine_distance
import numpy as np
from bs4 import BeautifulSoup
from bs4 import Comment
# from collections import Counter
# from heapq import nlargest
# import heapq
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from advancedcrawler.items import AdvancedcrawlerItem
import wordninja
# from readability import Document




# from playwright.async_api import async_playwright
# import asyncio

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# nltk.download('stopwords') #download and comment
# nltk.download('punkt') #download and comment
# Initialize BART summarization model



class DynamicSpider(CrawlSpider):
    name = 'myspider'
    allowed_domains = ['www.pcdl.co']
    start_urls = ['https://www.pcdl.co/']

    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        # Add more user-agents as needed
    ]

    rules = (
        Rule(LinkExtractor(allow=()), callback='parse_item', follow=True),
    )

    def __init__(self, *args, **kwargs):
        super(DynamicSpider, self).__init__(*args, **kwargs)
        self.visited_urls = set()
        self.rake = Rake()  # Initialize Rake for keyword extraction
        self.nlp = spacy.load("en_core_web_sm")  # Load spaCy model for NER
        self.page_rank_cache = {}  # Dictionary to store page rank results
        
        


    

    async def parse_item(self, response):
        try:
            if isinstance(response, scrapy.http.HtmlResponse):

                

                # # Remove script tags from the response body
                # cleaned_body = response.text.replace('<script>', '').replace('</script>', '')

                # Extract entities from the main content
                entities = self.extract_entities(response)

                # Perform sentiment analysis on the main content
                sentiment = self.analyze_sentiment(response)

                # Extract favicon
                favicon_url = self.extract_favicon(response)
                 
                # Calculate page rank
                page_rank = self.calculate_page_rank(response)

                # Determine the method to use for this URL
                crawler_method = self.determine_crawler_method(response)
                
                print(f"Crawler method for {response.url}: {crawler_method}")

                # Determine the method to use for this URL
                crawler_method = self.determine_crawler_method(response)

                print(f"Crawler method for {response.url}: {crawler_method}")

                if crawler_method == 'playwright':
                    # Use Playwright for dynamic websites
                    await self.parse_item_with_playwright(response, entities, sentiment, favicon_url, page_rank)
                else:
                    # Use Scrapy for static websites
                    # Create an instance of YourItem and populate it with data
                    item = AdvancedcrawlerItem(
                        url=response.url,
                        title=self.extract_title(response),
                        meta_description=self.extract_meta_description(response),
                        favicon_url=favicon_url,
                        # entities=entities,
                        sentiment=sentiment,
                        site_name=self.extract_site_name(response),
                        language=self.extract_language(response),
                        publication_date=self.extract_publication_date(response),
                        author=self.extract_author(response),
                        # internal_links=self.extract_internal_links(response),
                        # heading_tags=self.extract_heading_tags(response),
                        security_trustworthiness=self.extract_security_trustworthiness(response),
                        semantic_markup=self.extract_semantic_markup(response),
                        # keywords=self.extract_keywords(response),
                        # named_entities=self.extract_named_entities(response),
                        summary=self.generate_summary(response),
                        readability_scores=self.calculate_readability(response),
                        page_load_time=self.measure_page_load_time(response),
                        num_outbound_links=self.count_outbound_links(response),
                        page_size=self.measure_page_size(response),
                        response_headers=self.extract_response_headers(response),
                        # user_interaction_elements=self.extract_user_interaction_elements(response),
                        # ad_networks=self.extract_ad_networks(response),
                        structured_markup_errors=self.detect_structured_markup_errors(response),
                        page_rank=page_rank
                    )

                    yield item

        except Exception as e:
            # Log the exception
            logger.error(f"Error processing URL: {response.url}. Error message: {str(e)}")

    async def parse_item_with_playwright(self, response, entities, sentiment, favicon_url, page_rank):
        with sync_playwright() as p:
            browser = p.firefox()
            page = browser.new_page()
            await page.goto(response.url)

            # Perform Playwright actions here
            title = await page.title()
            content = await page.content()

            # ... (add other Playwright actions as needed)

            # Create an instance of YourItem and populate it with data
            item = WebPages(
                url=response.url,
                title=self.extract_title(response),
                meta_description=self.extract_meta_description(response),
                favicon_url=favicon_url,
                entities=entities,
                sentiment=sentiment,
                site_name=self.extract_site_name(response),
                language=self.extract_language(response),
                publication_date=self.extract_publication_date(response),
                author=self.extract_author(response),
                internal_links=self.extract_internal_links(response),
                heading_tags=self.extract_heading_tags(response),
                security_trustworthiness=self.extract_security_trustworthiness(response),
                semantic_markup=self.extract_semantic_markup(response),
                keywords=self.extract_keywords(response),
                named_entities=self.extract_named_entities(response),
                summary=self.generate_summary(response),
                readability_scores=self.calculate_readability(response),
                page_load_time=self.measure_page_load_time(response),
                num_outbound_links=self.count_outbound_links(response),
                page_size=self.measure_page_size(response),
                response_headers=self.extract_response_headers(response),
                user_interaction_elements=self.extract_user_interaction_elements(response),
                ad_networks=self.extract_ad_networks(response),
                structured_markup_errors=self.detect_structured_markup_errors(response),
                page_rank=page_rank
            )

            yield item

            browser.close()

    def determine_crawler_method(self, response):
        # Decode the bytes to string before comparison
        content_type = response.headers.get('Content-Type', b'').decode('utf-8').lower()

        # Implement your heuristic to determine the method to use
        # Example: Check for the presence of JavaScript or analyze HTML structure
        if 'text/javascript' in content_type:
            return 'playwright'
        else:
            return 'scrapy'

    async def parse(self, response):
        # If you need additional parsing logic, you can add it here
        pass

    def extract_title(self, response):
        try:
            title = response.xpath('//title/text()').get()
            return title.strip() if title else None
        except Exception as e:
            logger.warning(f"Error extracting title from URL: {response.url}. Error message: {str(e)}")
            return None

    def extract_meta_description(self, response):
        try:
            # List of XPath expressions or CSS selectors where meta descriptions might be found
            meta_description_selectors = [
                '//meta[@name="description"]/@content',  # Example: <meta name="description" content="...">
                '//meta[@property="og:description"]/@content',  # Open Graph Protocol meta tag
                '//meta[@name="twitter:description"]/@content',  # Twitter meta tag
                # Add more XPath expressions or CSS selectors as needed...
            ]

            # Iterate over the list of selectors and attempt to extract the meta description
            for selector in meta_description_selectors:
                meta_description = response.xpath(selector).get()
                if meta_description:
                    return meta_description.strip()

            # If no meta description is found in any of the specified locations, return None
            return None
        except Exception as e:
            logger.warning(f"Error extracting meta description from URL: {response.url}. Error message: {str(e)}")
            return None

   

    def extract_main_content(self, response):
        try:
            # Parse the response body using BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove specific tags
            for tag in soup(['nav', 'header', 'script']):
                tag.extract()

            # Remove inline style attributes
            for tag in soup():
                del tag['style']

            # Remove email addresses
            email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            main_content = re.sub(email_regex, '', soup.get_text(separator=' ', strip=True))

            # Select specific tags for content extraction
            relevant_tags = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div', 'span'])  # Add relevant tags

            # Extract text from selected tags
            main_content += ' '.join(tag.get_text(separator=' ', strip=True) for tag in relevant_tags)

            # Preprocess the main content
            main_content = self.preprocess_text(main_content)

            return main_content
        except Exception as e:
            logger.warning(f"Error extracting main content from URL: {response.url}. Error message: {str(e)}")
            return None


   



    def extract_site_name(self, response):
        try:
            # Check if there's a specific tag that holds the site name
            site_name_tag = response.xpath('//meta[@property="og:site_name"]/@content').get()
            if site_name_tag:
                return site_name_tag.strip()

            # Check other meta tags commonly used for site name
            meta_tags = [
                ('application-name', '//meta[@name="application-name"]/@content'),
                ('apple-mobile-web-app-title', '//meta[@name="apple-mobile-web-app-title"]/@content'),
                ('msapplication-tooltip', '//meta[@name="msapplication-tooltip"]/@content')
            ]

            for tag_name, xpath_expr in meta_tags:
                site_name = response.xpath(xpath_expr).get()
                if site_name:
                    return site_name.strip()

            # Extract site name from the URL
            url = response.url
            if url:
                # Remove protocol (https:// or http://) and www prefix from the URL
                domain = urlparse(url).netloc.replace("www.", "")

                # Remove extension (.com, .org, etc.)
                domain_without_extension = domain.split(".")[0]

                # Split the domain into meaningful words using wordninja
                words = wordninja.split(domain_without_extension)

                # Capitalize each word
                capitalized_words = [word.capitalize() for word in words]

                # Join the capitalized words with spaces
                site_name = ' '.join(capitalized_words)

                # Return the extracted site name
                return site_name.strip() if site_name else None
            else:
                return None
        except Exception as e:
            self.logger.warning(f"Error extracting site name from URL: {response.url}. Error message: {str(e)}")
            return None



    def extract_favicon(self, response):
        try:
            # Extract favicon from HTML if available
            favicon_url = response.xpath('//link[@rel="icon"]/@href').get()
            if not favicon_url:
                favicon_url = response.xpath('//link[@rel="shortcut icon"]/@href').get()

            if favicon_url:
                return urljoin(response.url, favicon_url)
            else:
                # If favicon is not found in HTML, construct default location
                return urljoin(response.url, '/favicon.ico')
        except Exception as e:
            logger.warning(f"Error extracting favicon from URL: {response.url}. Error message: {str(e)}")
            return None

    def extract_heading_tags(self, response):
        try:
            heading_tags = {}
            for level in range(1, 7):  # Check heading tags from h1 to h6
                heading_xpath = f'//h{level}/text()'
                headings = response.xpath(heading_xpath).getall()
                if headings:
                    # Remove newline characters and add <br> tags
                    cleaned_headings = [heading.replace('\n', '<br>').replace('\r', '') for heading in headings]
                    heading_tags[f'h{level}'] = cleaned_headings
            return heading_tags
        except Exception as e:
            logger.warning(f"Error extracting heading tags from URL: {response.url}. Error message: {str(e)}")
            return None


    def preprocess_text(self, text):
        try:
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', '', text)

            # Remove special characters, punctuation, and digits
            text = re.sub(r'[^a-zA-Z\s]', '', text)

            # Convert to lowercase
            text = text.lower()
            return text
        except Exception as e:
            logger.warning(f"Error processing text from URL: {self.response.url}. Error message: {str(e)}")
            return None

    def extract_language(self, response):
        try:
            lang = response.xpath('//html/@lang').get()
            if lang:
                return lang.split('-')[0]
            else:
                main_content = self.extract_main_content(response)
                if main_content:
                    main_content = ' '.join(main_content)
                    main_content = self.preprocess_text(main_content)
                    if len(main_content) > 100:  # Adjust the threshold as needed
                        # Detect language using langid library as a fallback
                        lang, _ = langid.classify(main_content)
                        return lang if lang != 'unknown' else None
                return None
        except Exception as e:
            logger.warning(f"Error extracting language from URL: {response.url}. Error message: {str(e)}")
            return None

    def extract_publication_date(self, response):
        try:
            # List of XPath expressions or CSS selectors where publication dates might be found
            date_selectors = [
                '//meta[@name="pubdate"]/@content',  # Example: <meta name="pubdate" content="2024-02-12">
                '//span[@class="published-date"]/text()',  # Example: <span class="published-date">2024-02-12</span>
                '//div[@class="post-meta"]/time/@datetime',  # Example: <div class="post-meta"><time datetime="2024-02-12">...</time></div>
                # Add more XPath expressions or CSS selectors as needed...
            ]

            # Iterate over the list of selectors and attempt to extract the publication date
            for selector in date_selectors:
                date = response.xpath(selector).get()
                if date:
                    # If a date is found, attempt to parse it into a datetime object
                    try:
                        parsed_date = datetime.strptime(date, '%Y-%m-%d')
                        return parsed_date.strftime('%Y-%m-%d')
                    except ValueError:
                        # Handle cases where the date string cannot be parsed
                        logger.warning(f"Failed to parse publication date '{date}' using format '%Y-%m-%d'")
                        continue

            # If no publication date is found in any of the specified locations, return None
            return None

        except Exception as e:
            logger.warning(f"Error extracting publication date from URL: {response.url}. Error message: {str(e)}")
            return None

    def extract_author(self, response):
        try:
            author = response.xpath('//meta[@name="author"]/@content').get()
            return author.strip() if author else None
        except Exception as e:
            logger.warning(f"Error extracting author from URL: {response.url}. Error message: {str(e)}")
            return None

    def extract_internal_links(self, response):
        """
        Extract internal links (links within the same domain) from a webpage.

        Args:
        - response (scrapy.http.HtmlResponse): The response object representing the webpage.

        Returns:
        - list: A list of internal links found on the webpage.
        """
        try:
            base_url = response.url
            internal_links = set()  # Use set to avoid duplicates

            # Iterate over all <a> elements on the webpage
            for link in response.css('a::attr(href)').getall():
                # Join the link with the base URL to get the absolute URL
                absolute_url = response.urljoin(link.strip())
                parsed_url = urlparse(absolute_url)

                # Check if the parsed URL's netloc (domain) matches the base URL's netloc
                if parsed_url.netloc == urlparse(base_url).netloc:
                    # Normalize and filter links
                    normalized_url = parsed_url._replace(fragment='', query='').geturl()
                    internal_links.add(normalized_url)

            # Convert the set of internal links to a list and return it
            return list(internal_links)
        except Exception as e:
            logger.warning(f"Error extracting internal links from URL: {response.url}. Error message: {str(e)}")
            return None


    def extract_security_trustworthiness(self, response):
        return {
            'ssl': self.check_ssl_presence(response.url),
        }

    def extract_semantic_markup(self, response):
        try:
            schema_markup = {}
            # Extract schema.org markup
            schema_elements = response.xpath('//*[@itemscope]')
            for element in schema_elements:
                item_type = element.xpath('@itemtype').get()
                if item_type:
                    item_props = {}
                    properties = element.xpath('.//*[@itemprop]')
                    for prop in properties:
                        prop_name = prop.xpath('@itemprop').get()
                        prop_value = prop.xpath('string()').get()
                        if prop_name and prop_value:  # Ensure both name and value are present
                            item_props[prop_name] = prop_value.strip()  # Remove leading/trailing whitespaces
                    if item_props:  # Only add non-empty properties
                        schema_markup[item_type] = item_props

            return schema_markup if schema_markup else None  # Return None if no schema markup found
        except Exception as e:
            logger.warning(f"Error extracting semantic markup from URL: {response.url}. Error message: {str(e)}")
            return None

    def _filter_link(self, link):
        url = urlparse(link.url)
        return url.scheme in ['http', 'https'] and url.netloc == self.allowed_domains[0] and link.url not in self.visited_urls

    def _request(self, link):
        headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Referer': 'https://www.google.com/',
        }
        return scrapy.Request(link.url, headers=headers, callback=self._response_downloaded)

    def _response_downloaded(self, response):
        self.visited_urls.add(response.url)
        return super()._response_downloaded(response)

    def closed(self, reason):
        if reason == 'finished':
            self.logger.info('Spider finished scraping successfully.')
        else:
            raise CloseSpider(f"Spider closed unexpectedly: {reason}")

    def check_ssl_presence(self, url):
        try:
            hostname = urlparse(url).hostname
            context = ssl.create_default_context()
            with socket.create_connection((hostname, 443)) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                    cert = ssock.getpeercert()
                    return bool(cert)
        except Exception as e:
            return False

    def extract_entities(self, response):
        try:
            main_content = ' '.join(response.xpath('//body//text()').getall())
            doc = self.nlp(main_content)
            entities = {}
            for ent in doc.ents:
                if ent.label_ not in entities:
                    entities[ent.label_] = []
                entities[ent.label_].append(ent.text)
            return entities
        except Exception as e:
            logger.warning(f"Error extracting entities from URL: {response.url}. Error message: {str(e)}")
            return None

    def analyze_sentiment(self, response):
        try:
            main_content = ' '.join(response.xpath('//body//text()').getall())
            blob = TextBlob(main_content)
            sentiment_score = blob.sentiment.polarity
            if sentiment_score > 0:
                return 'positive'
            elif sentiment_score < 0:
                return 'negative'
            else:
                return 'neutral'
        except Exception as e:
            logger.warning(f"Error analyzing sentiment from URL: {response.url}. Error message: {str(e)}")
            return None

    def extract_keywords(self, response):
        try:
            main_content = ' '.join(response.xpath('//body//text()').getall())
            self.rake.extract_keywords_from_text(main_content)
            return self.rake.get_ranked_phrases()
        except Exception as e:
            logger.warning(f"Error extracting keywords from URL: {response.url}. Error message: {str(e)}")
            return None

    def extract_named_entities(self, response):
        try:
            main_content = ' '.join(response.xpath('//body//text()').getall())
            doc = self.nlp(main_content)
            return list(set([ent.text for ent in doc.ents]))
        except Exception as e:
            logger.warning(f"Error extracting named entities from URL: {response.url}. Error message: {str(e)}")
            return None


    def calculate_readability(self, response):
        try:
            main_content = ' '.join(response.xpath('//body//text()').getall())

            # Check if main_content has at least 100 words
            if len(main_content.split()) >= 100:
                readability = Readability(main_content)
                # Extract desired readability metrics
                flesch_kincaid_grade = readability.flesch_kincaid().grade_level
                coleman_liau_index = readability.coleman_liau().grade_level
                # Return the metrics
                return {
                    'flesch_kincaid_grade': flesch_kincaid_grade,
                    'coleman_liau_index': coleman_liau_index,
                }
            else:
                # Handle cases where content doesn't meet the minimum word count requirement
                logger.warning(f"Error: Content must have at least 100 words for readability analysis. URL: {response.url}")
                return None
        except ReadabilityException as e:
            # Handle ReadabilityException
            logger.warning(f"ReadabilityException: {e}. URL: {response.url}")
            return None
        except Exception as e:
            logger.warning(f"Error calculating readability from URL: {response.url}. Error message: {str(e)}")
            return None
    
    def measure_page_load_time(self, response):
        start_time = response.meta.get('download_latency', 0)
        end_time = time.time()
        return end_time - start_time

 
    def count_outbound_links(self, response):
        """
        Count the number of outbound links (links to external domains) on a webpage.
        
        Args:
        - response (scrapy.http.HtmlResponse): The response object representing the webpage.
        
        Returns:
        - int: The number of outbound links found on the webpage.
        """
        # Use CSS selector to select all <a> elements with href attribute
        # This selects all links on the webpage
        all_links = response.css('a::attr(href)').getall()
        
        # Get the base URL of the current webpage
        base_url = response.url
        
        # Filter the links to keep only the outbound links
        outbound_links = []
        for link in all_links:
            absolute_url = response.urljoin(link.strip())
            parsed_url = urlparse(absolute_url)
            if parsed_url.netloc != urlparse(base_url).netloc:
                outbound_links.append(absolute_url)
        
        # Return the count of outbound links
        return len(outbound_links)


    def measure_page_size(self, response):
        return len(response.body)

    def extract_response_headers(self, response):
        headers_dict = {}
        for key, value in response.headers.items():
            if isinstance(value, list):
                value = [v.decode('utf-8') for v in value]
            elif isinstance(value, bytes):
                value = value.decode('utf-8')
            headers_dict[key.decode('utf-8')] = value
        return headers_dict

    def extract_user_interaction_elements(self, response):
        user_interaction_elements = {}

        # Extract forms
        forms = response.xpath('//form')
        form_details = []
        for form in forms:
            form_details.append({
                'action': form.xpath('@action').get(),
                'method': form.xpath('@method').get(),
                'inputs': form.xpath('.//input/@name').getall(),
                'buttons': form.xpath('.//button/text()').getall()
            })
        user_interaction_elements['forms'] = form_details

        # Extract buttons
        buttons = response.xpath('//button/text()').getall()
        user_interaction_elements['buttons'] = buttons

        # Extract input fields
        inputs = response.xpath('//input/@name').getall()
        user_interaction_elements['input_fields'] = inputs

        return user_interaction_elements

    def extract_ad_networks(self, response):
        ad_networks = []
        try:
            ad_elements = response.xpath('//div[contains(@class, "ad")]')

            for ad_element in ad_elements:
                ad_network = {}

                ad_network['ad_type'] = ad_element.xpath('@data-ad-type').get()

                # Additional ad-related information extraction
                ad_network['advertiser_name'] = ad_element.xpath('@data-advertiser-name').get()
                ad_network['dimensions'] = ad_element.xpath('@data-dimensions').get()
                ad_network['click_through_url'] = ad_element.xpath('@data-click-through-url').get()

                ad_networks.append(ad_network)
        except Exception as e:
            logger.warning(f"Error extracting ad networks from URL: {response.url}. Error message: {str(e)}")

        return ad_networks

    def detect_structured_markup_errors(self, response):
        try:
            # Extract structured markup from the response
            structured_markup = response.xpath('//script[@type="application/ld+json"]/text()').get()

            if structured_markup:
                structured_data = json.loads(structured_markup)

                # Check for required properties
                required_properties = {'name', 'description', 'url'}
                missing_properties = required_properties - set(structured_data.keys())
                if missing_properties:
                    logger.warning(f"Missing required properties in structured markup: {missing_properties}")

                # Validate data types and values
                for prop, value in structured_data.items():
                    if prop == 'price':
                        if not isinstance(value, (int, float)) or value < 0:
                            logger.warning("Invalid value for 'price' property in structured markup")
                    elif prop == 'url':
                        if not self.is_valid_url(value):
                            logger.warning("Invalid URL format for 'url' property in structured markup")
                    elif prop == '@type':
                        if value != 'Product':
                            logger.warning("Structured data type is not 'Product' as per schema.org guidelines")
                    # Add more specific checks for other properties as needed

        except Exception as e:
            logger.error(f"Error detecting structured markup errors: {str(e)}")


    def is_valid_url(url):
        """
        Validate the format of a URL using regular expressions.
        
        Args:
        - url (str): The URL to validate.
        
        Returns:
        - bool: True if the URL is valid, False otherwise.
        """
        try:
            # Regular expression pattern to match a URL format
            url_pattern = re.compile(
                r'^(?:http|ftp)s?://'  # http:// or https:// or ftp://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
                r'localhost|'  # localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
                r'(?::\d+)?'  # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
            
            # Check if the URL matches the pattern
            return bool(re.match(url_pattern, url))
        except Exception as e:
            logger.error(f"Error detecting valid url errors: {str(e)}")
    
    def calculate_page_rank(self, response):
        try:
            # Create a directed graph
            G = nx.DiGraph()

            # Add nodes (pages)
            G.add_node(response.url)

            # Add edges (links between pages)
            for link in self.extract_internal_links(response):
                G.add_edge(response.url, link)

            # Calculate page rank
            page_rank = nx.pagerank(G)

            return page_rank.get(response.url, 0)  # Return the page rank of the current URL, defaulting to 0 if not found
        except Exception as e:
            logger.error(f"Error calculating page rank from URL: {response.url}. Error message: {str(e)}")
            return 0


    def generate_summary(self, response, num_sentences=3):
        try:
            # Extract meta description from response metadata
            meta_description = self.extract_meta_description(response)

            if meta_description and meta_description.strip():  # Check if meta description is available and not empty
                return meta_description.strip()  # Use meta description as summary

            # Extract main content
            main_content = self.extract_main_content(response)

            # Preprocess the text
            preprocessed_text = self.preprocess_text(main_content)

            # Tokenize sentences
            sentences = [sent.text for sent in self.nlp(preprocessed_text).sents]

            # Vectorize sentences
            vectorizer = CountVectorizer().fit(sentences)

            # Build similarity matrix
            similarity_matrix = self.build_similarity_matrix(sentences, vectorizer)

            # Apply PageRank algorithm
            scores = np.zeros(len(sentences))
            damping_factor = 0.85
            epsilon = 1e-4
            delta = 1
            while delta > epsilon:
                new_scores = (1 - damping_factor) + damping_factor * np.dot(similarity_matrix.T, scores)
                delta = np.linalg.norm(new_scores - scores)
                scores = new_scores

            # Sort sentences by score and select top-ranked sentences
            top_sentence_indices = np.argsort(-scores)[:num_sentences]
            summary = [sentences[i] for i in sorted(top_sentence_indices)]

            return " ".join(summary)
        except Exception as e:
            self.logger.error(f"Error generating summary for URL: {response.url}. Error message: {str(e)}")
            return None




    def preprocess_text(self, text):
        # Preprocess the text (tokenization, lemmatization, etc.)
        tokens = [token.lemma_ for token in self.nlp(text) if token.is_alpha]
        preprocessed_text = " ".join(tokens)
        return preprocessed_text



    def sentence_similarity(self, sent1, sent2, vectorizer):
        # Calculate cosine similarity between two sentences
        vectorized_sents = vectorizer.transform([sent1, sent2])
        similarity_matrix = cosine_similarity(vectorized_sents)
        similarity = similarity_matrix[0, 1]  # Extract the similarity value between the two sentences
        return similarity


    def build_similarity_matrix(self, sentences, vectorizer):
        # Build a similarity matrix for the sentences
        similarity_matrix = np.zeros((len(sentences), len(sentences)))
        for i in range(len(sentences)):
            for j in range(len(sentences)):
                if i != j:
                    similarity_matrix[i][j] = self.sentence_similarity(sentences[i], sentences[j], vectorizer)
        return similarity_matrix
    
    
    def cache_page_rank(self, url, page_rank):
        """
        Cache the page rank result in a dictionary.
        """
        self.page_rank_cache[url] = page_rank

if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        'DOWNLOAD_DELAY': 7,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS': 1,
        'AUTOTHROTTLE_ENABLED': True,
        'HTTPCACHE_ENABLED': True,
        'LOG_LEVEL': 'INFO',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True},
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_playwright.downloadermiddlewares.PlaywrightRequestMiddleware': 101,
            'scrapy_playwright.downloadermiddlewares.PlaywrightResponseMiddleware': 101,
        },
        'PLAYWRIGHT_BROWSER_PATH': '/home/genie/.cache/ms-playwright/chromium-1105/chrome-linux/chrome',
        'PLAYWRIGHT_DOWNLOADS_PATH': '/home/genie/python/WORKSHOP/myproject',
    })
    process.crawl(DynamicSpider)
    process.start()
