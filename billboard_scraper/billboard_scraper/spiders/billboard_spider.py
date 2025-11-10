import scrapy

class BillboardSpiderSpider(scrapy.Spider):
    name = 'billboard_spider'
    allowed_domains = ['en.wikipedia.org']
    start_urls = ['https://en.wikipedia.org/wiki/Billboard_Year-End']

    def parse(self, response):
        """
        Parses the main Wikipedia index page.
        Finds all links to the year-end charts and yields a new
        Request for each one, calling 'self.parse_song_page'.
        """
        self.log(f'Parsing main index page: {response.url}')
        
        # CSS selector to find links within the navbox
        links_selector = 'div.navbox[aria-labelledby^="Billboard_Year-End_Hot_100_singles"] .navbox-list li a'
        links = response.css(links_selector)

        self.log(f'Found {len(links)} year links to crawl.')

        for link in links:
            relative_url = link.css('::attr(href)').get()
            year_text = link.css('::text').get()

            # Ensure it is a valid year link
            if relative_url and year_text and year_text.strip().isdigit():
                full_url = response.urljoin(relative_url)
                
                # Instead of yielding the link, we yield a new Request
                # to crawl this page.
                # The 'callback' tells Scrapy which function to call
                # once the page is downloaded.
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_song_page
                )
            else:
                self.log(f'Skipped link (not a year): {link.get()}')

    def parse_song_page(self, response):
        """
        Parses an annual chart page (e.g., "..._of_1970").
        This function is called for each link found in 'parse()'.
        It scrapes the song table and yields one dictionary per song.
        """
        self.log(f'Parsing song page: {response.url}')

        # 1. Extract the year from the page title (e.g., <h1>)
        page_title = response.css('h1#firstHeading::text').get()
        year = "UNKNOWN" # Default value
        if page_title and 'of ' in page_title:
            # Extracts the year, e.g., "1970" from "... singles of 1970"
            # Also works for "... top 30 singles of 1949"
            year = page_title.split('of ')[-1].strip()
        
        # 2. Find the song table
        # We target the table with both 'wikitable' and 'sortable' classes
        table = response.css('table.wikitable.sortable')
        if not table:
            self.log(f"No 'wikitable sortable' table found on {response.url}")
            return

        # 3. Get all rows <tr> that contain data cells <td>
        # (this skips the header row <th>)
        rows = table.css('tbody tr:has(td)')
        if not rows:
            self.log(f"No <tr> rows with <td> found in the table on {response.url}")
            return

        self.log(f'Found {len(rows)} songs for the year {year}.')

        # 4. Loop over each row to extract data
        for row in rows:
            # Get all cells (columns) in the row
            cells = row.css('td')
            
            # Ensure we have at least 3 cells (No., Title, Artist)
            if len(cells) < 3:
                self.log(f'Malformed row on {response.url}: {row.get()}')
                continue # Skip to the next row

            # --- Data Extraction ---
            
            # 'place' column: found in the 1st <td> cell
            place = cells[0].css('::text').get()
            
            # 'name' column: found in the 2nd <td> cell
            # We join all text nodes (just in case) and strip quotes
            name = "".join(cells[1].css('::text').getall()).strip().strip('"')
            
            # 'link' column: the 'href' from the <a> tag in the 2nd <td>
            link_relative = cells[1].css('a::attr(href)').get()
            link = None # Default to None if no link exists
            if link_relative:
                link = response.urljoin(link_relative) # Create the absolute link
            
            # 'artist' column: found in the 3rd <td> cell
            # We join all text nodes to handle multiple artists
            # (e.g., "Artist A & Artist B")
            artist = "".join(cells[2].css('::text').getall()).strip()

            # --- Yield one dictionary per song ---
            # The keys ('name', 'artist', etc.) will
            # automatically become the CSV headers.
            yield {
                'name': name,
                'artist': artist,
                'year': year,
                'place': place.strip() if place else None,
                'link': link
            }