import scrapy


class BillboardSpiderSpider(scrapy.Spider):
    name = "billboard_spider"
    allowed_domains = ["en.wikipedia.org"]
    start_urls = ["https://en.wikipedia.org"]

    def parse(self, response):
        pass
import scrapy

class BillboardSpiderSpider(scrapy.Spider):
    name = 'billboard_spider'
    allowed_domains = ['en.wikipedia.org']
    start_urls = ['https://en.wikipedia.org/wiki/Billboard_Year-End']

    def parse(self, response):
        """
        Parses the main Wikipedia page to find links to year-end charts.
        """
        self.log(f'Parsing page: {response.url}')

        # This CSS selector targets the specific "Billboard Year-End Hot 100 singles"
        # navbox you provided in the HTML.
        # It selects all <a> (link) tags that are inside a list item (li)
        # within the navbox list (td.navbox-list).
        
        # We target the navbox by its 'aria-labelledby' attribute, which
        # starts with 'Billboard_Year-End_Hot_100_singles'
        links_selector = 'div.navbox[aria-labelledby^="Billboard_Year-End_Hot_100_singles"] .navbox-list li a'
        
        links = response.css(links_selector)

        self.log(f'Found {len(links)} year links.')

        for link in links:
            # Extract the year, which is the link's text
            year = link.css('::text').get()
            
            # Extract the relative URL (e.g., /wiki/...)
            relative_url = link.css('::attr(href)').get()

            # Ensure we have both a valid year (is a digit) and a URL
            if year and relative_url and year.strip().isdigit():
                # Clean up the year string
                year_clean = year.strip()
                
                # Create the full, absolute URL
                full_url = response.urljoin(relative_url)
                
                # Yield the data as a dictionary
                yield {
                    'year': year_clean,
                    'url': full_url
                }
            else:
                self.log(f'Skipped non-year link: {link.get()}')