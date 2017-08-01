import re
import json
import time
import scrapy
import poplib
import urllib
import logging
import requests
import ConfigParser

from datetime import datetime

from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

config = ConfigParser.ConfigParser()
section = config.read("config.ini")

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


class PFEntrySpider(scrapy.Spider):
	name = "pf_entry"
	domain = "http://www.practicefusion.com/"
	start_urls = ["https://static.practicefusion.com/apps/ehr/?c=1385407302#/login"]

	password_ = config.get('FUSION','password')
	login_ = config.get('FUSION','username')

	# gmail account details
	server = config.get('GMAIL','server')
	user  = config.get('GMAIL','username')
	password = config.get('GMAIL','password')

	def parse(self, response):

		# declare patient tables
		patient = Base.classes.patient
		association = Base.classes.association
		name = Base.classes.name
		appointment = Base.classes.appointment
		communication = Base.classes.communication
		phone = Base.classes.phone
		email = Base.classes.email

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

		patients_obj = session.query(patient).filter_by(pf_entry_flag=0).all()
		for patient_ob in patients_obj:

			association_obj = session.execute("select id from association where patient_id = '"+str(patient_ob.id)+"'")
			association_obj = association_obj.fetchone()

			patient_data = session.execute("select * from patient where id = '"+str(patient_ob.id)+"'")
			patient_data = patient_data.fetchall()

			social_sec_no = patient_data[0][8]

			name_obj = session.execute("select * from name where association_id = '"+str(association_obj[0])+"'")
			name_obj = name_obj.fetchall()

			md_id = patient_ob.id
			firstName = name_obj[0][2]
			middleName = name_obj[0][3]
			lastName = name_obj[0][4]

			detail_obj = session.execute("select id from detail where name_id = '"+str(name_obj[0][0])+"'")
			detail_obj = detail_obj.fetchone()

			dmo_obj = session.execute("select * from demographic_detail where detail_id = '"+str(detail_obj[0])+"'")
			dmo_obj = dmo_obj.fetchall()
			dob = dmo_obj[0][3]
			sex = dmo_obj[0][2]
			if 'f' in sex:
				gender  = '1'
			else:
				gender = '0'
			age = dmo_obj[0][1]
			marital_status = dmo_obj[0][4]
			ethnicity = dmo_obj[0][5]
			pre_lang = dmo_obj[0][6]
			race = dmo_obj[0][8]

			#Format date
			if ', ' in dob:
				date_of_birth = dob.replace(', ',' ')
				dt_obj = datetime.strptime(date_of_birth, '%b %d %Y')
				dob= str(dt_obj.month)+'/'+str(dt_obj.day)+'/'+str(dt_obj.year)
			else:
				dob = dob

			#  PHONE
			communication_obj = session.execute("select id from communication where name_id = '"+str(name_obj[0][0])+"'")
			communication_obj = communication_obj.fetchone()

			phone_obj = session.execute("select * from phone where communication_id = '"+str(communication_obj[0])+"'")
			phone_obj = phone_obj.fetchall()
			mobilePhone = phone_obj[0][1]
			home = phone_obj[0][7]
			work = phone_obj[0][8]
			cell = phone_obj[0][9]
			phone_secondary = phone_obj[0][6]

			# EMAIL
			email_obj = session.execute("select * from email where communication_id = '"+str(communication_obj[0])+"'")
			email_obj = email_obj.fetchall()
			emailAddress = email_obj[0][1]

			address_obj = session.execute("select * from address where communication_id = '"+str(communication_obj[0])+"'")
			address_obj = address_obj.fetchall()

			mobilePhoneCountry = ''
			isUserOfMobilePhone = ''
			isUserOfEmail = ''
			streetAddress1 = address_obj[0][1]
			streetAddress2 = ''
			postalCode = address_obj[0][6]
			# city = address_obj[0][4]
			# state = address_obj[0][5]

			full_name = name_obj[0][2]+' '+name_obj[0][3]+' '+name_obj[0][4]
			# primaryAddress = {  "streetAddress1":streetAddress1,
			#                     "streetAddress2":streetAddress2,
			#                      "postalCode":postalCode,
			#                      "city":city,
			#                      "state":state
			# }


			# isOptedInToMobilePhoneMessaging = ''
			# isOptedInToEmailMessaging = ''
			# isOptedInToVoiceMessaging = ''
			# allowDuplicatePatientIdentifier = ''
			# allowDuplicatePatientRecordNumber = ''
			# comments = ''
			# isActive = ''
			# appointmentRemindersEnabled = ''
			# isFakePatient = ''
			# raceOptions = ''

			# patientPreferences = {

			# }
			# patientSocialHistory = {

			# }

			# session.execute("update patient set pf_entry_flag='1' where id='"+str(patient_ob.id)+"'")
			# session.commit()

			#search with name
			name = full_name+middleName+lastName
			search_url = "https://static.practicefusion.com/PatientEndpoint/api/v1/patients/search"

			search_payload = {"matchAll":"true","firstOrLastName":name}
			search_headers = {
				'authorization': session_tkn,
				'content-type': "application/json; charset=UTF-8",
				'cache-control': "no-cache",
				}

			search_response = requests.request("POST", search_url, data=json.dumps(search_payload), headers=search_headers)
			search_response = json.loads(search_response.text)

			#get city and state from zip
			url = "https://static.practicefusion.com/PracticeEndpoint/api/v1/PostalCodeCityStateProvince/11552"
			headers = {
				'authorization': session_tkn,
				'cache-control': "no-cache",
				}

			zip_response = requests.request("GET", url, headers=headers)
			zip_response = json.loads(zip_response.text)
			city = zip_response["city"]
			state = zip_response["stateProvince"]

			if not search_response["patients"]:
				save_url = "https://static.practicefusion.com/PatientEndpoint/api/v1/patients"
				save_payload =   {"patient":
								{"generateNewPatientIdentifier":"true",
								"generateNewPatientRecordNumber":"true",
								"firstName":firstName,
								"lastName":lastName,
								"birthDate":dob,
								"gender":gender,
								"age":age,
								"mobilePhone":mobilePhone,
								"mobilePhoneCountry":"USA",
								"isUserOfMobilePhone":"true",
								"emailAddress":emailAddress,
								"isUserOfEmail":"true",
								"isOptedInToMobilePhoneMessaging":"false",
								"isOptedInToEmailMessaging":"false",
								"isOptedInToVoiceMessaging":"false",
								"allowDuplicatePatientIdentifier":"false",
								"allowDuplicatePatientRecordNumber":"false",
								"isActive":"true",
								"appointmentRemindersEnabled":"false",
								"isFakePatient":"false",
								"raceOptions":"[]",
								"primaryAddress":
									{"streetAddress1":streetAddress1,
									"streetAddress2":streetAddress2,
									"postalCode":postalCode,
									"city":city,
									"state":state
									}
								},
							"patientPreferences":"{}",
							"patientSocialHistory":"{}",
							"allowDuplicatePatientRecordNumber":"false",
							}

				# payload = {"patient":
				# 				{"patientGuid":"9709409e-92b9-401ca797-3a0704d09dc0",
				# 				"patientID":md_id,
				# 				"patientRecordNumber":"AA644041",
				# 				"generateNewPatientIdentifier":"false",
				# 				"generateNewPatientRecordNumber":"false",
				# 				"practiceGuid":"b4aaf54f-d06a-49fe-a0d1-ddc6f16cbfdf",
				# 				"firstName":firstName,
				# 				"lastName":lastName,
				# 				"birthDate":dob,
				# 				"deathDate":"",
				# 				"gender":gender,
				# 				"age":age,
				# 				"mobilePhone":mobilePhone,
				# 				"mobilePhoneCountry":"",
				# 				"isUserOfMobilePhone":"true",
				# 				"emailAddress":emailAddress,
				# 				"isUserOfEmail":"true",
				# 				"isOptedInToMobilePhoneMessaging":"false",
				# 				"isOptedInToEmailMessaging":"false",
				# 				"isOptedInToVoiceMessaging":"false",
				# 				"allowDuplicatePatientIdentifier":"false",
				# 				"allowDuplicatePatientRecordNumber":"false",
				# 				"mothersMaidenName":"",
				# 				"comments":"",
				# 				"isActive":"true",
				# 				"appointmentRemindersEnabled":"false",
				# 				"immunizationProtectionTypeID":"1",
				# 				"mostRecentVisitDate":"0001-01-01T00:00:00Z",
				# 				"mostRecentVisitTranscriptGuid":"00000000-0000-0000-0000-000000000000",
				# 				"isFakePatient":"false",
				# 				"externalPatientGuid":"0c106af9-ea4b-4d2f-bed3-d82beca6b886",
				# 				"raceOptions":"[9]",
				# 				"primaryAddress":
				# 					{"streetAddress1":streetAddress1,
				# 					"streetAddress2":streetAddress2,
				# 					"postalCode":postalCode,
				# 					"city":city,
				# 					"state":state
				# 					}
				# 				},
				# 			"patientPreferences":"{}",
				# 			"patientContacts":"[]",
				# 			"patientSocialHistory":"{}",
				# 			"allowDuplicatePatientRecordNumber":"false",
				# 			"generateNewPatientRecordNumber":"false"
				# 			}
				save_headers = {
					'authorization': session_tkn,
					'content-type': "application/json",
					'cache-control': "no-cache",
					}

				response = requests.request("POST", save_url, data=json.dumps(save_payload), headers=save_headers)

				print 'Saved data.....',response.text,'\n\n',response.status_code

	def convert_date(self,date):
		"""
		Convert different date format to single date format
		"""
		ALLOWED_FORMATS = ['%Y-%m-%d','%d-%m-%Y','%Y/%m/%d','%d/%m/%Y','%d.%m.%Y','%Y.%m.%d','%m-%d-%Y','%m/%d/%Y','%m.%d.%Y']
		for format in ALLOWED_FORMATS:
			try:
				return datetime.datetime.strptime(date, format).strftime('%d-%m-%Y')
			except ValueError:
				pass

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
