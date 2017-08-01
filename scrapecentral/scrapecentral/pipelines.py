import urllib
import unicodedata
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

class ScrapecentralPipeline(object):

	def process_item(self, item, spider):

		patient = Base.classes.patient
		association = Base.classes.association
		name = Base.classes.name
		physician = Base.classes.physician
		communication = Base.classes.communication
		address = Base.classes.address
		phone = Base.classes.phone
		email = Base.classes.email
		location = Base.classes.location
		medical_record = Base.classes.medical_record
		medical = Base.classes.medical
		record = Base.classes.record
		appointment = Base.classes.appointment
		history_medical_disease = Base.classes.history_medical_disease
		remind = Base.classes.remind
		history = Base.classes.history
		condition = Base.classes.condition
		rc_call_report = Base.classes.rc_call_report
		visit = Base.classes.visit
		pf_appt_report = Base.classes.pf_appt_report
		book = Base.classes.book
		request_appt = Base.classes.request_appt
		demographic_detail = Base.classes.demographic_detail
		qualify = Base.classes.qualify
		workflow = Base.classes.workflow

		detail = Base.classes.detail
		demographic_employment = Base.classes.demographic_employment
		text_message = Base.classes.text_message
		treatment = Base.classes.treatment
		recommendation = Base.classes.recommendation
		call_book_appt = Base.classes.call_book_appt


		print 'INSIDE PIPELINES...'

		if spider.name == 'md':
			print 'INSIDE MD'
			# patient table
			session.add(patient(patient_id=item['md_id']))
			session.commit()

			# association tabl
			pt = session.execute("select id from patient where patient_id = '"+str(item['md_id'])+"'")
			pt = pt.fetchone()
			session.add(association(patient_id=pt[0], relationship=item['relationship']))
			session.commit()

			# name table
			association = session.execute("select id from association where patient_id = '"+str(pt[0])+"'")
			association = association.fetchone()
			session.add(name(title=item['patient_title'], fname=item['fname'], mname =item['mname'], lname =item['lname'], patient_id =pt[0], association_id = association[0]))
			session.commit()

			# add demographic_detail
			try:
				name_ob = session.execute("select id from name where association_id = '"+str(association[0])+"'")
				name_ob = name_ob.fetchone()
				detail_obj = session.execute("select id from detail where name_id = '"+str(name_ob[0])+"'")
				detail_obj = detail_obj.fetchone()

				session.add(demographic_detail(age ='Not specified', sex =item['sex'], DOB=item['DOB'], marital_status ="Not specified", ethnicity = 'Not specified',pre_lang ='Not specified',detail_id = detail_obj[0]))
				session.commit()
			except:
				name_ob = session.execute("select id from name where association_id = '"+str(association[0])+"'")
				name_ob = name_ob.fetchone()
				session.add(detail(name_id=name_ob[0]))
				session.commit()
				detail_obj = session.execute("select id from detail where name_id = '"+str(name_ob[0])+"'")
				detail_obj = detail_obj.fetchone()
				session.add(demographic_detail(age ='Not specified', sex =item['sex'], DOB=item['DOB'], marital_status ="Not specified", ethnicity = 'Not specified',pre_lang ='Not specified',detail_id = detail_obj[0]))
				session.commit()

			# physician table
			session.add(physician(type=item['phy_type'], specialty=item['phy_specialty'], association_id = association[0], fname=item['phy_fname'], lname=item['phy_lname'], phone=item['phy_phone_work'], fax=item['phy_fax_work']))
			session.commit()

			# communication table
			session.add(communication(pref_comm=item['pref_comm'], type=item['comm_type'], email_comm =item['email_comm'], phone_comm=item['phone_comm'], text_comm=item['text_comm'], communication_agree=item['communication_agree'], name_id = name_ob[0]))
			session.commit()

			# address table
			try:
				communication = session.execute("select id from communication where name_id = '"+str(name_ob[0])+"'")
				communication = communication.fetchone()
				session.add(address(line_1=item['address1'], city=item['city'], state =item['state'], zip=item['zip'], source=item['md_source'], communication_id=communication[0]))
				session.commit()
			except:
				session.add(communication(name_id=name_ob[0]))
				session.commit()
				communication = session.execute("select id from communication where name_id = '"+str(name_ob[0])+"'")
				communication = communication.fetchone()
				session.add(address(line_1=item['address1'], city=item['city'], state =item['state'], zip=item['zip'], source=item['md_source'], communication_id=communication[0]))
				session.commit()

			# phone table
			session.add(phone(phone_number=item['phone'], ext=item['phone_ext'], type =item['phone_type'], source=item['md_source'], phone_number2=item['phone2'], communication_id=communication[0]))
			session.commit()

			# email table
			session.add(email(email=item['email'], source=item['md_source'], communication_id=communication[0]))
			session.commit()

			# text_message table
			session.add(text_message(text_address=item['text_address'],type = item['address_type'],source=item['md_source'], communication_id=communication[0]))
			session.commit()

			# record table
			session.add(record(name_id=name_ob[0]))
			session.commit()

			# detail table
			session.add(detail(name_id=name_ob[0]))
			session.commit()

			# demographic_detail table
			detail_obj = session.execute("select id from detail where name_id = '"+str(name_ob[0])+"'")
			detail_obj = detail_obj.fetchone()
			session.add(demographic_detail(age ='Nill', sex =item['sex'],DOB =item['DOB'],marital_status ='Nill',ethnicity ='Nill',pre_lang ='Nill',detail_id = detail_obj[0]))
			session.commit()

			# medical table
			session.add(medical(patient_id=pt[0]))
			session.commit()

			#  appointment table
			session.add(appointment(patient_id=pt[0]))
			session.commit()


			# condition table
			medical = session.execute("select id from medical where patient_id = '"+str(pt[0])+"'")
			medical = medical.fetchone()
			session.add(condition(medical_id=medical[0]))
			session.commit()


			# qualify table
			condition_obj = session.execute("select id from condition where medical_id = '"+str(medical[0])+"'")
			condition_obj = condition_obj.fetchone()
			session.add(qualify(name =item['qualify_condition'], type ='Not Specified',condition_id =condition_obj[0]))
			session.commit()


			# history_medical_disease table
			session.add(history_medical_disease(name=item['his_name'], category=item['his_category'], source =item['md_source'], onset_month =item['onset_month'], onset_year='onset_yr', current_treat=item['current_treat'], treat_last_visit=item['treat_last_visit'], condition_id = condition_obj[0]))
			session.commit()

			# history table
			session.add(history(prior_rec=item['prior_rec'], rec_date=item['rec_date'], rec_state=item['rec_state'], recent_medicine=item['recent_medicine'], medical_id=medical[0]))
			session.commit()

			# remind table
			appointment_obj = session.execute("select id from appointment where patient_id = '"+str(pt[0])+"'")
			appointment_obj = appointment_obj.fetchone()
			session.add(remind(appointment_id=appointment_obj[0]))
			session.commit()

			#book table
			session.add(book(appointment_id=appointment_obj[0]))
			session.commit()

			#request_appt table
			data1 = item['first_choice'].split(',')
			date_request1 = data1[0]+','+data1[1]
			time_request1 = data1[2]

			data2 = item['second_choice'].split(',')
			date_request2 = data1[0]+','+data1[1]
			time_request2 = data1[2]

			# book_obj = session.query(book).filter_by(appointment_id=appointment.id).first()
			book_obj = session.execute("select id from book where appointment_id = '"+str(appointment_obj[0])+"'")
			book_obj = book_obj.fetchone()
			session.add(request_appt(requested_on=item["requested_on"],location=item["location"],provider='Not Specified',type=item['appointment_type'],source=item['md_source'],reason=item['qualify_condition'],date_request1=date_request1,time_request1=time_request1,date_request2=date_request2,time_request2=time_request2,pt_notes=item['patient_note'],book_id=book_obj[0]))
			session.commit()

			# medical_record table
			record_obj = session.execute("select id from record where name_id = '"+str(name_ob[0])+"'")
			record_obj = record_obj.fetchone()
			session.add(medical_record(name=item["med_records"],type=item['med_type'],share=item['med_share'],record_id=record_obj[0]))
			session.commit()

			# visit table
			session.add(visit(appointment_id=appointment_obj[0]))
			session.commit()

			# workflow table
			visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0])+"'")
			visit_obj = visit_obj.fetchone()
			session.add(workflow(visit_id=visit_obj[0]))
			session.commit()

		if spider.name == 'rc':

			print 'INSIDE RC....'
			# rc_call_report table
			try:
				phone_obj = session.execute("select * from phone where phone_number = '"+str(item['rc_recipient'])+"'")
				phone_obj = phone_obj.fetchall()

				communication_obj = session.execute("select * from communication where id = '"+str(phone_obj[0][5])+"'")
				communication_obj = communication_obj.fetchall()

				name_obj = session.execute("select * from name where id = '"+str(communication_obj[0][7])+"'")
				name_obj = name_obj.fetchall()

				patient_id = name_obj[0][5]

				appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_id)+"'")
				appointment_obj = appointment_obj.fetchone()

				remind_obj = session.execute("select id from remind where appointment_id = '"+str(appointment_obj[0])+"'")
				remind_obj = remind_obj.fetchone()

				session.add(rc_call_report(date = item["rc_date"],group =item['rc_group'],delivery=item["rc_delivery"],duration=item["rc_duration"],type=item["rc_type"],recipient=item["rc_recipient"],name=item["rc_name"],appt=item["rc_appt"],tries_status=item["rc_tries_status"],reply=item["rc_reply"],remind_id=remind_obj[0]))
				session.commit()
			except:
				print 'PHONE NUMBER NOT FOUND'

		if spider.name == 'pf_scrape':

			print 'INSIDE PF SCRAPE....'
			site_pt_name=item["pf_patient_name"]
			site_pt_dob=self.convert_date(item["pf_patient_dob"])

			patients_obj = session.query(patient).filter_by(pf_flag=0).all()
			for patient_ob in patients_obj:
				name_obj = session.execute("select * from name where patient_id = '"+str(patient_ob.id)+"'")
				name_obj = name_obj.fetchall()
				db_pt_name = name_obj[0][2]+' '+name_obj[0][4]

				detail_obj = session.execute("select id from detail where name_id = '"+str(name_obj[0][0])+"'")
				detail_obj = detail_obj.fetchall()

				demographic_obj = session.execute("select * from demographic_detail where detail_id = '"+str(detail_obj[0][0])+"'")
				demographic_obj = demographic_obj.fetchall()
				if demographic_obj:
					dob = demographic_obj[0][3]
					db_pt_dob = self.convert_date(unicodedata.normalize('NFKD',dob).encode('ascii','ignore'))

					print 'SITE NAME...',site_pt_name,'\nDB NAME...',db_pt_name,'\nSITE DOB...',site_pt_dob,'\nDB DOB...',db_pt_dob
					if site_pt_name == db_pt_name and site_pt_dob == db_pt_dob:
						try:
							appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_ob.id)+"'")
							appointment_obj = appointment_obj.fetchall()
						except:
							session.add(appointment(patient_id=patient_ob.id))
							session.commit()
							appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_ob.id)+"'")
							appointment_obj = appointment_obj.fetchone()

						try:
							visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0])+"'")
							visit_obj = visit_obj.fetchall()
						except:
							session.add(visit(appointment_id=appointment_obj[0][0]))
							session.commit()
							visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0][0])+"'")
							visit_obj = visit_obj.fetchone()

						visit_id = visit_obj[0][0]
						# pf table
						session.add(pf_appt_report(date=item["pf_date"],time=item["pf_time"],status=item["pf_status"],patient_name=item["pf_patient_name"],patient_dob=item["pf_patient_dob"],facility=item["pf_facility"],appointment_type =item["pf_appointment_type"] ,visit_id= visit_id))
						session.commit()

						# int_session = Session(engine)
						# int_session.execute("update patient set pf_flag='1' where id='"+str(patient_obj.id)+"'")
						# int_session.commit()
						# int_session.close()
						print 'PATIENT UPDATED WITH SCRAPED DATA:...'

		if spider.name == 'pf_download':

			print 'INSIDE PF DOWNLOAD...'
			# patient table
			session.add(patient(patient_id=item["pf_pt_id"]))
			session.commit()

			# association table
			pt = session.execute("select id from patient where patient_id = '"+str(item["pf_pt_id"])+"'")
			pt = pt.fetchone()
			session.add(association(patient_id=pt[0], relationship='Self'))
			session.commit()

			# name table
			association = session.execute("select id from association where patient_id = '"+str(pt[0])+"'")
			association = association.fetchone()
			session.add(name(fname=item["pf_pt_firstName"],lname =item["pf_pt_lastName"], patient_id =pt[0], association_id = association[0]))
			session.commit()

			# add demographic_detail
			try:
				name_ob = session.execute("select id from name where association_id = '"+str(association[0])+"'")
				name_ob = name_ob.fetchone()
				detail_obj = session.execute("select id from detail where name_id = '"+str(name_ob[0])+"'")
				detail_obj = detail_obj.fetchone()

				session.add(demographic_detail(age =item["pf_pt_age"], sex =item["pf_pt_gender"], DOB=item["pf_pt_dob"],detail_id = detail_obj[0]))
				session.commit()
			except:
				name_ob = session.execute("select id from name where association_id = '"+str(association[0])+"'")
				name_ob = name_ob.fetchone()
				session.add(detail(name_id=name_ob[0]))
				session.commit()
				detail_obj = session.execute("select id from detail where name_id = '"+str(name_ob[0])+"'")
				detail_obj = detail_obj.fetchone()
				session.add(demographic_detail(age =item["pf_pt_age"], sex = item["pf_pt_gender"], DOB=item["pf_pt_dob"],detail_id = detail_obj[0]))
				session.commit()

			# communication table
			session.add(communication(name_id = name_ob[0]))
			session.commit()

			# address table
			try:
				communication = session.execute("select id from communication where name_id = '"+str(name_ob[0])+"'")
				communication = communication.fetchone()
				session.add(address(line_1=item["pf_pt_street_add1"], city=item["pf_pt_city"], state =item["pf_pt_state"], zip=item["pf_pt_zip"],communication_id=communication[0]))
				session.commit()
			except:
				session.add(communication(name_id=name_ob[0]))
				session.commit()
				communication = session.execute("select id from communication where name_id = '"+str(name_ob[0])+"'")
				communication = communication.fetchone()
				session.add(address(line_1=item["pf_pt_street_add1"], city=item["pf_pt_city"], state =item["pf_pt_state"], zip=item["pf_pt_zip"],communication_id=communication[0]))
				session.commit()

			# phone table
			session.add(phone(phone_number=item["pf_pt_mob"],communication_id=communication[0]))
			session.commit()

			# email table
			session.add(email(email=item["pf_pt_email"], communication_id=communication[0]))
			session.commit()

			print 'DOWNLOADED DATA SAVED.....'

	def convert_date(self, date):
		"""
		Convert different date format to single date format
		"""
		ALLOWED_FORMATS = ['%Y-%m-%d','%d-%m-%Y','%Y/%m/%d','%d/%m/%Y','%d.%m.%Y','%Y.%m.%d','%m-%d-%Y','%m/%d/%Y','%m.%d.%Y']
		for format in ALLOWED_FORMATS:
			try:
				return datetime.strptime(date, format).strftime('%d-%m-%Y')
			except ValueError:
				pass


