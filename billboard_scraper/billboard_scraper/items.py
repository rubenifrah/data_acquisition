# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BillboardScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class YoutubeCommentItem(scrapy.Item):
    track_name = scrapy.Field()
    artist = scrapy.Field()
    youtube_id = scrapy.Field()
    comment_id = scrapy.Field()
    author = scrapy.Field()
    text = scrapy.Field()
    like_count = scrapy.Field()
    published_at = scrapy.Field()
    position = scrapy.Field()


class WikipediaAwardItem(scrapy.Item):
    track_name = scrapy.Field()
    artist = scrapy.Field()
    year = scrapy.Field()
    source = scrapy.Field()
    awards = scrapy.Field()
