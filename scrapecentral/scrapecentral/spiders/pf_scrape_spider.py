import re
import os
import json
import time
import scrapy
import poplib
import logging
import requests
import ConfigParser

from datetime import datetime, timedelta
from scrapecentral.items import ScrapecentralItem


class PFDownloadSpider(scrapy.Spider):
	name = "pf_scrape"
	domain = "http://www.practicefusion.com/"
	start_urls = ["https://static.practicefusion.com/apps/ehr/?c=1385407302#/login"]
	conf_path = os.path.dirname(os.path.abspath(__file__))+'/config.ini'

	config = ConfigParser.ConfigParser()
	section = config.read(conf_path)

	password_ = config.get('FUSION','password')
	login_ = config.get('FUSION','username')

	# gmail account details
	server = config.get('GMAIL','server')
	user  = config.get('GMAIL','username')
	password = config.get('GMAIL','password')

	def parse(self, response):

		#login
		login_url = "https://static.practicefusion.com/EhrAuthEndpoint/api/v3/users/login"
		payload = {"loginEmailAddress":"2m@doctormm.com","password":"pA142*2@","carbonVersion":"1.69.0.1285"}
		headers = {
			'content-type': "application/json",
			'cache-control': "no-cache",
			}

		response = requests.request("POST", login_url, data=json.dumps(payload), headers=headers)
		data = json.loads(response.text)
		session_token = data["details"]["fauxSessionToken"]

		#send otp
		send_otp_url = "https://static.practicefusion.com/EhrAuthEndpoint/api/v3/browser/setup"
		send_otp_payload = {"isSms":"true","browserAuthorizationSessionToken":session_token}
		send_otp_headers = {
			'authorization': session_token,
			'content-type': "application/json",
			'cache-control': "no-cache",
			}

		send_otp_response = requests.request("POST", send_otp_url, data=json.dumps(send_otp_payload), headers=send_otp_headers)
		time.sleep(6)

		#parse otp from mail
		otp = self.parse_otp_from_mail()

		#submit otp
		sub_otp_url = "https://static.practicefusion.com/EhrAuthEndpoint/api/v3/browser/authorize"
		sub_otp_payload = {"validationCode":otp,"browserAuthorizationSessionToken":session_token,"rememberBrowser":"true","carbonVersion":"1.69.0.1285"}
		sub_otp_headers = {
			'authorization': session_token,
			'content-type': "application/json",
			'cache-control': "no-cache",
			}

		sub_otp_response = requests.request("POST", sub_otp_url, data=json.dumps(sub_otp_payload), headers=sub_otp_headers)
		sub_data = json.loads(sub_otp_response.text)
		session_tkn = sub_data["sessionToken"]

		#Run report
		run_report_url = "https://static.practicefusion.com/ScheduleEndpoint/api/v1/Schedule/Report/0/50"

		today = datetime.today()
		before = today - timedelta(days=7)
		before = before.strftime('%m/%d/%Y')

		after = today + timedelta(days=30)
		after = after.strftime('%m/%d/%Y')
		time.sleep(4)

		run_report_payload = {"startMinimumDateTimeUtc": before,"startMaximumDateTimeUtc": after}
		run_report_headers = {
			'authorization': session_tkn,
			'content-type': "application/json",
			'cache-control': "no-cache",
			}

		run_report_response = requests.request("POST", run_report_url, data=json.dumps(run_report_payload), headers=run_report_headers)
		patient_details = json.loads(run_report_response.text)
		patient_data =  patient_details["scheduledEventList"]
		for patient in patient_data:
			item = ScrapecentralItem()
			item["pf_date"] = patient['startAtDateTimeFlt'].split('T')[0]
			item["pf_time"] = patient['startAtDateTimeFlt'].split('T')[1]
			item["pf_status"] = patient['status']
			item["pf_facility"] = patient['facilityName']
			item["pf_source"] = 'PF'
			item["pf_appointment_type"] = patient['appointmentTypeName']
			item["pf_patient_name"] = patient['patientName']
			item["pf_patient_dob"] = patient['patientDateOfBirthDateTime'].split('T')[0]

			yield item

	def parse_otp_from_mail(self):
		"""
		Fetch otp details from gmail inbox
		:param data: self
		:return:
		"""
		logging.debug('connecting to ' + self.server)
		server = poplib.POP3_SSL(self.server)
		#server = poplib.POP3(SERVER)

		# login
		logging.debug('logging in')
		server.user(self.user)
		server.pass_(self.password)
		mail_count = server.stat()[0],

		## fetch the top mail
		latest_email = server.retr(mail_count)
		mail_text_data = str(latest_email)
		otp = re.findall(r'Your code is: (.+?). T', mail_text_data, re.DOTALL)
		if len(otp) == 2:
			otp = otp[0]
		else:
			otp = otp

		return otp