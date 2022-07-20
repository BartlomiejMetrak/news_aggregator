# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsAllItem(scrapy.Item):
    # define the fields for your item here like:
    link = scrapy.Field()
    title = scrapy.Field()
    source = scrapy.Field()
    source_id = scrapy.Field()
    description = scrapy.Field()
    date = scrapy.Field()
    article = scrapy.Field()
    image = scrapy.Field()
    extraction_score = scrapy.Field()
    words_in_art_perc = scrapy.Field()
    hash_article = scrapy.Field()

    def set_all(self, value=None):  #domyślnie każda wartość jest zero
        for keys, _ in self.fields.items():
            self[keys] = value
