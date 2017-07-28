import re
import csv
import scrapy
import os.path
import requests
import subprocess
import urllib

from collections import defaultdict

from lxml import html
from twilio.rest import Client

from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect

import gspread
from oauth2client.service_account import ServiceAccountCredentials

server = 'tcp:medwiserstaging0001.database.windows.net'
username = 'medwiser'
password = 'Nhy65tgb'
database = 'ScrapeCentralDatabase'

metadata = MetaData()
Base = automap_base()
params = urllib.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params) 
Base.prepare(engine, reflect=True)
session = Session(engine)


class TwilioSpider(scrapy.Spider):
	name = "twilio"
	domain = "https://www.twilio.com"
	start_urls = ["https://www.twilio.com/login"]
	password_ = '4r6&^jhg&U1234'
	login_ = 'Tester@medwiser.org'

	account_sid = "ACda10725b966b653d1fd4e8ee3bc4fa9c"
	auth_token = "b57924fb283d944aa79b424b6b86bafb"



	def parse(self, response):
		"""
		Fetch details of a specific number and save it into database tables
		:param data: response
		:return:
		"""
		patient = Base.classes.patient
		_name = Base.classes.name
		carrier_index = Base.classes.carrier_index
		communication = Base.classes.communication
		phone = Base.classes.phone

		patient_obj = session.query(patient).filter_by(twilio_flag=0)

		for patient in patient_obj:
			name_obj = session.execute("select id from name where patient_id = '"+str(patient.id)+"'")
			name_obj = name_obj.fetchone()

			communication = session.execute("select id from communication where name_id = '"+str(name_obj[0])+"'")
			communication = communication.fetchone()

			phone_obj = session.execute("select * from phone where communication_id = '"+str(communication.id)+"'")
			phone_obj = phone_obj.fetchall()

			phone_num = phone_obj[0][1]
			phone_num = phone_num.replace('(','+1').replace(') ','-')

			client = Client(self.account_sid, self.auth_token)
			phone_num = "+1415-701-2311"
			number = client.lookups.phone_numbers(phone_num).fetch(type=["carrier", "caller-name"],)
			# number2 = client.lookups.phone_numbers("+1415-701-2311").fetch(type="caller-name")

			session.add(carrier_index(carrier=number.carrier["name"],type =number.carrier["type"],carrier_mms ="Not Provided",carrier_sms ="Not Provided",communication_id =communication.id))
			session.commit()

			print 'PHONE NUMBER DATA SAVED SUCCESSFULLY..'

			# patient_obj.twilio_flag = '1'
			# session.commit()