import asyncio
import json
import re
import logging
from io import BytesIO

import aiohttp
import aiohttp_retry
from lxml import etree
from tqdm import tqdm

from . import headers
from .browser import Browser
from .advertisement import Advertisement
from .html_helpers import get_html_content


class Parser:
    def __init__(self, browser: Browser):
        self.parser = etree.HTMLParser(encoding='utf-8')
        self.browser = browser

        self.logger = logging.getLogger(__name__)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def get_etree(self, url, *args, **kwargs):
        kwargs['headers'] = headers.device
        retry_options = aiohttp_retry.ExponentialRetry(attempts=10)
        retry_client = aiohttp_retry.RetryClient(self.session, retry_options=retry_options)
        async with retry_client.get(url, *args, **kwargs) as r:
            if r.status == 200:
                r_url = str(r.url)
                if url != r_url:
                    if r_url.split('/')[4] == 'sd':
                        return r_url
                html_bytes = await r.read()
                html_file = BytesIO(html_bytes)
                html = etree.parse(html_file, self.parser)
                return html
            elif r.status == 404:
                return None
            else:
                self.logger.error(f"{url} return status code {r.status}")
                open(f'debug/error {r.status}', 'w').write(url)
                return None

    async def get_pages_count(self, url=None, html=None) -> int:
        if not html:
            try:
                html = await self.get_etree(url)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e}")
                return 0

        nav_info_html = html.xpath('//p[@class="fSize11 centered"]')
        if nav_info_html:
            nav_info = get_html_content(nav_info_html[0])
            max_pages = nav_info.rsplit('-')[-1].split()[0]
            return int(max_pages)
        else:
            return 0

    async def get_base_advs_from_page(self,
                                      property_type,
                                      url,
                                      page_num: int,
                                      sem,
                                      pbar) -> list[Advertisement]:
        url = url + ':p:' + str(page_num)
        async with sem:  # semaphore limits num of simultaneous downloads
            try:
                page_html = await self.get_etree(url)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e}")
                return []
            
            pbar.update(1)
            if not page_html:
                return []
            elif advs_html := page_html.xpath('//li[@class="listingBox w100"]'):
                advs = []
                for adv_html in advs_html:
                    publish_date_html = adv_html.xpath('//span[@class="listingDetails iconPadR"]')[0]
                    publish_date_string = get_html_content(publish_date_html)
                    advs.append(Advertisement(
                        int(re.findall(r'/\d+/', adv_html.get('linkref'))[0].strip('/')),
                        property_type,
                        publish_date=publish_date_string,
                        url=adv_html.get('linkref')
                    ))
                return advs
            elif listings_li := page_html.xpath('//li[@class="promotionListing listingBox w100"]'):
                advs = []
                for listing_li in listings_li:
                    advs.append(Advertisement(
                        int(re.findall(r'/\d+/', listing_li.get('linkref'))[0].strip('/')),
                        property_type,
                        url=listing_li.get('linkref')
                    ))
                return advs

    async def get_all_base_advs(self, property_type, url):
        print('\nПолучаю все страницы с объявлениями из', url)
        max_pages = await self.get_pages_count(url)
        if max_pages:
            coros = []
            sem = asyncio.Semaphore(40)
            with tqdm(total=max_pages) as pbar:
                for i in range(1, max_pages + 1):
                    coro = self.get_base_advs_from_page(property_type, url, i, sem, pbar)
                    coros.append(coro)

                results = await asyncio.gather(*coros)
                all_advs = sum(results, [])
                return all_advs
        else:
            return []

    async def get_adv_phone_numbers(self, url, adv_html):
        phone_numbers_html = await self.browser.get_phone_numbers_xml(url, adv_html)
        if phone_numbers_html is None:
            return
        
        html_file = BytesIO(phone_numbers_html.encode('utf-8'))
        root = etree.parse(html_file, self.parser)
        phone_numbers_p = root.xpath('//p')
        return [get_html_content(p) for p in phone_numbers_p]

    async def get_adv_location(self, adv_html):
        try:
            map_html = adv_html.xpath('//div[@class="prop-map-holder"][1]')[0]
            lat = float(map_html.get('lat'))
            long = float(map_html.get('lon'))
            return [lat, long]
        # Координат прямо на странице не оказалось.
        except TypeError:
            request_url = adv_html.xpath('//input[@id="locDataUrlHidden"][1]')[0].get('value')
            location_type = adv_html.xpath('//input[@id="locationType"][1]')[0].get('value')
            location_id = adv_html.xpath('//input[@id="locationId"][1]')[0].get('value')
            return await self.browser.get_adv_location(
                request_url, location_type, location_id
            )

    def get_adv_photos(self, adv_html):
        urls = []
        if urls_holder_div := adv_html.xpath('//div[@class="flipsnap noRtl"][1]'):
            # [{'photo': {'url': str, 'extension': str, 'id': int, 'mainPicture': bool}}]
            pics = json.loads(urls_holder_div[0].get('pics'))
            for pic in pics:
                urls.append(pic['photo']['url'])
        elif no_photos_img := adv_html.xpath('//img[@alt="No Photo"][1]'):
            urls = None
        elif no_photos_img := adv_html.xpath('//div[contains(@class, "noPhoto")]'):
            urls = None
        else:
            raise
        return urls

    async def parse_adv(self, adv, sem, pbar, for_rent=False):
        async with sem:
            url = adv.url

            try:
                adv_html = await self.get_etree(url)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e}")
                return
            
            # Если вернулась строка, то значит, что объявление убрали.
            if type(adv_html) is str:
                adv.relevant = False
                return adv
            
            if adv_html is None:
                self.logger.error(f"Nothing to parse (url: {url})")
                adv.relevant = False
                return adv
            
            adv.relevant = True

            # Заголовок объявления.
            try:
                adv.title = get_html_content(adv_html.xpath('//h1')[0])
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing title)")

            # Описание объявления.
            try:
                description_html = adv_html.xpath('//div[contains(@class, "blockProp")]/p')[0]
                adv.description = get_html_content(description_html, '\n\n')
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing description)")

            # Стоимость недвижимости.
            try:
                price_string = get_html_content(adv_html.xpath('//div[@class="mainInfoProp"][1]//h3[@class="orangeTit"][1]')[0])
                if 'Price on request' not in price_string:
                    price, currency = price_string.strip().split(maxsplit=1)
                    if 'per' in price_string:
                        print(price_string, url)
                    if 'per day' in price_string:
                        adv.price_per_day = float(price.replace(',', ''))
                    else:
                        price = float(price.replace(',', ''))
                        if for_rent:
                            adv.rent_price = price
                        else:
                            adv.price = price
                    adv.currency = currency
                else:
                    adv.request_price = True
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing price)")

            # Район и город.
            try:
                city_html = adv_html.xpath('//div[contains(@class, "adBreadBlock")]/div/div/a[2]')[0]
                region = get_html_content(city_html).split(' ')[1]  # Отрезаем Property, оставляя название города
                adv.district = None
                adv.region = region
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing city)")

            # Берём информацию из тегов.
            try:
                tags_html = adv_html.xpath('//div[contains(@class, "adDetails")]/div/span')
                for span in tags_html:
                    tag = get_html_content(span)
                    tag = tag.lower()
                    adv.tags.append(tag)
                    if tag.endswith('m²'):
                        adv.area = int(tag.split()[0])
                    elif ' room' in tag:
                        adv.rooms_number = int(tag.split()[0])
                    elif tag.endswith('rooms'):
                        adv.rooms_number = int(tag.split()[0])
                    elif tag.endswith('th'):
                        adv.floor = int(tag.split()[0])
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing tags)")

            # Берем информацию из характеристик
            try:
                tags_html = adv_html.xpath('//div[contains(@class, "adFeatures")]/div/div[2]/p[2]')
                for span in tags_html:
                    tag = get_html_content(span)
                    tag = tag.lower()
                    if 'year' in tag:
                        # less than 1 year
                        if 'less than' in tag:
                            adv.age = int(tag.split()[-2])
                        # over 100 years old
                        elif 'over' in tag:
                            adv.age = int(tag.split()[1])
                        # 5-10 years old
                        elif '-' in tag:
                            years_range = tag.split()[0]
                            adv.age = int(years_range.split('-')[-1])
                        else:
                            adv.age = int(tag.split()[0])
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing features)")

            # Теги для дополнительной рекламы
            try:
                ad_features_html = adv_html.xpath('//div[contains(@class,"adFeatures")]/div/span')
                for li in ad_features_html:
                    ad_feature = get_html_content(li)
                    if ad_feature.lower() == 'elevator':
                        adv.elevator = True
                    adv.ad_features.append(ad_feature)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing ad features)")

            if not adv.elevator:
                adv.elevator = False
                
            # Координаты недвижимости
            try:
                adv.location = await self.get_adv_location(adv_html)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing coords)")

            # Фото объявления.
            try:
                adv.photos_urls = self.get_adv_photos(adv_html)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing photos urls)")

            # Получаем список номеров через браузер (нужно корректное выполнение JS).
            try:
                adv.phone_numbers = await self.get_adv_phone_numbers(url, adv_html)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing phone numbers)")

            pbar.update(1)
            return adv

    async def parse_new_adv(self, adv: Advertisement, sem, pbar):
        async with sem:
            url = adv.url
            
            try:
                adv_html = await self.get_etree(url)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e}")
                return
            
            # Если вернулась строка, то значит, что объявление убрали.
            if type(adv_html) is str:
                adv.relevant = False
                return url
            
            adv.relevant = True
            adv.new = True

            # Заголовок.
            try:
                title_h1 = adv_html.xpath('//h1[@class="SpremiumH2"][1]')[0]
                adv.title = get_html_content(title_h1)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing title) [new adv]")

            # Описание.
            try:
                description_p = adv_html.xpath('//p[@class="changeDescrip"][1]')[0]
                adv.description = get_html_content(description_p, '\n\n')
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing description) [new adv]")

            # Цена.
            try:
                price_h2 = adv_html.xpath('//h2[@class="SpremiumH2 orangeText"][1]')[0]
                price_string = get_html_content(price_h2).lower().replace(',', '')
                price_string_split = price_string.split()
                if 'from' in price_string:
                    adv.from_price = float(price_string_split[1])
                    adv.currency = price_string_split[-1]
                elif 'request' in price_string:
                    adv.request_price = True
                else:
                    adv.price = float(price_string_split[0])
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing price) [new adv]")

            # Теги.
            try:
                tags_p = adv_html.xpath('//p[@class="immoBadge"]')
                for tag_p in tags_p:
                    adv.tags.append(get_html_content(tag_p))
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing tags) [new adv]")

            # Локация.
            try:
                location_p = adv_html.xpath('//div[contains(@class, "adBreadBlock")]/div/div/a[3]')[0]
                city = get_html_content(location_p).split()[-1]  # Отрезаем New homes
                adv.district = None
                adv.region = city
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing location) [new adv]")

            # Телефон.
            try:
                adv.phone_numbers = await self.get_adv_phone_numbers(url, adv_html)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing phone numbers) [new adv]")

            # Координаты.
            try:
                adv.location = await self.get_adv_location(adv_html)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing location) [new adv]")

            # Фото.
            try:
                adv.photos_urls = self.get_adv_photos(adv_html)
            except Exception as e:
                self.logger.error(f"{type(e).__name__}: {e} (while parsing photos urls) [new adv]")

            pbar.update(1)
            return adv
