import os
import scrapy
import urllib
import ConfigParser

from twilio.rest import Client

from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

conf_path = os.path.dirname(os.path.abspath(__file__))+'/config.ini'
config = ConfigParser.ConfigParser()
section = config.read(conf_path)

server = config.get('DATABASE','server')
username = config.get('DATABASE','username')
password = config.get('DATABASE','password')
database = config.get('DATABASE','database')

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

	password_ = config.get('TWILIO','password')
	login_ = config.get('TWILIO','username')

	account_sid = config.get('TWILIO','account_sid')
	auth_token = config.get('TWILIO','auth_token')

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
			if phone_obj:
				phone_obj = phone_obj.fetchall()

				phone_num = phone_obj[0][1]
				phone_num = phone_num.replace('(','+1').replace(') ','-')

				client = Client(self.account_sid, self.auth_token)
				# phone_num = "+1415-701-2311"
				number = client.lookups.phone_numbers(phone_num).fetch(type=["carrier", "caller-name"],)
				# number2 = client.lookups.phone_numbers("+1415-701-2311").fetch(type="caller-name")

				session.add(carrier_index(carrier=number.carrier["name"],type =number.carrier["type"],carrier_mms ="Not Provided",carrier_sms ="Not Provided",communication_id =communication.id))
				session.commit()

				print 'PHONE NUMBER DATA SAVED SUCCESSFULLY..'
			else:
				print 'PHONE NUMBER IS NOT MATCHING..'

			# patient_obj.twilio_flag = '1'
			# session.commit()