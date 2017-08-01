import re
import os
import time
import scrapy
import ConfigParser

from dateutil.parser import parse
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

from scrapecentral.items import ScrapecentralItem


class RCSpider(scrapy.Spider):
	name = "rc"
	domain = "https://secure.remindercall.com"
	start_urls = ["https://secure.remindercall.com/login"]

	config = ConfigParser.ConfigParser()
	section = config.read("config.ini")

	chrome_path = config.get('CHROME','path')

	# def __init__(self):
	#     """
	#     Creating header names for the CSV file
	#     :param data: self
	#     :return:
	#     """
		# self.filepath = '/home/arun/project/ScrapeCentral/scrapecentral/scrapecentral/rc_data.csv'
		# if os.path.exists(self.filepath) is False:
		#     with open('rc_data.csv', 'a') as csvfile:
		#         writer = csv.DictWriter(csvfile, fieldnames=self.detail_field_set)
		#         writer.writeheader()

	def parse(self, response):
		"""
		Login, fetch data and write into CSV file
		:param data: response
		:return:
		"""
		project_path = os.path.dirname(os.path.abspath(__file__))+'/chromedriver'
		driver = webdriver.Chrome(executable_path=project_path)
		url = 'https://secure.remindercall.com/login'
		driver.get(url)
		time.sleep(2)
		log = driver.find_element_by_id('pageUsername')
		log.clear()
		log.send_keys(self.login_)

		passw = driver.find_element_by_id('pagePassword')
		passw.clear()
		passw.send_keys(self.password_, Keys.ENTER)
		time.sleep(5)

		driver.get('https://secure.remindercall.com')
		time.sleep(5)

		if 'Michael Morgenstern' in driver.page_source:

			item = ScrapecentralItem()
			table = driver.find_element_by_id('statsTable')
			last = table.find_elements_by_tag_name('tr')[-1]
			last.find_element_by_tag_name('a').click()
			time.sleep(2)

			date = 'none'
			date_data = driver.find_elements_by_xpath('//h2[@id="statsDateContainer"]')[0].text
			if date_data:
				date = re.search(r'Activity for (.+?)$', date_data).group(1)

			dt = parse(date)
			date = dt.strftime('%d/%m/%Y')

			scrape_table = driver.find_element_by_id('rStatsTable')
			tbody = scrape_table.find_element_by_tag_name('tbody')

			rows = tbody.find_elements_by_tag_name('tr')
			rows = [i.find_elements_by_tag_name('td') for i in rows]
			rows = [[i.text for i in y] for y in rows]

			rows = [[x.encode('UTF8') for x in row] for row in rows]
			rows = [[x.replace('null ', '') for x in row] for row in rows]

			data_list = []
			for row in rows:
				row = filter(None, row)
				data_list.append(row)

			for row in data_list:
				item["rc_type"] = row[0]
				item["rc_recipient"] = row[2]
				item["rc_name"] = row[3]
				item["rc_group"] = row[4]
				item["rc_appt"] = row[5]
				item["rc_delivery"] = row[6]
				item["rc_duration"] = row[7]
				item["rc_tries_status"] = row[8]
				item["rc_reply"] = row[9]
				item["rc_date"] = date

				yield item
		else:
			print 'ERROR WHILE LOGIN'


