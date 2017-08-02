import urllib
import gspread
import datetime
import unicodedata
import ConfigParser

from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from oauth2client.service_account import ServiceAccountCredentials

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



class GoogleSpreedSheetSync(object):

	def __init__(self):
		client = self.login()
		self.sheet = client.open("C.Mailer").sheet1
		self.sync_data_from_db(client)
		self.sync_data_from_sheet(client)


	def sync_data_from_sheet(self, client):

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
		g_sheet_scheduling = Base.classes.g_sheet_scheduling
		remind = Base.classes.remind
		history = Base.classes.history
		condition = Base.classes.condition
		rc_call_report = Base.classes.rc_call_report
		visit = Base.classes.visit
		pf_appt_report = Base.classes.pf_appt_report
		book = Base.classes.book
		request_appt = Base.classes.request_appt
		gen_availability = Base.classes.gen_availability

		rows = self.sheet.get_all_values()
		for row in rows[1:]:
			patient_id = row[0]

			# update Name
			name_obj = session.execute("select * from name where patient_id = '"+str(patient_id)+"'")
			name_obj = name_obj.fetchall()

			nm_session = Session(engine)
			if row[2]!= name_obj[0][2] and row[3]!= name_obj[0][4]:
				nm_session.execute("update name set fname='"+str(row[2])+"',lname='"+str(row[3])+"' where id='"+str(name_obj[0][0])+"'")
				nm_session.commit()

			# update DOB
			detail_obj = session.execute("select id from detail where name_id = '"+str(name_obj[0][0])+"'")
			detail_obj = detail_obj.fetchone()

			dmo_obj = session.execute("select * from demographic_detail where detail_id = '"+str(detail_obj[0])+"'")
			dmo_obj = dmo_obj.fetchall()

			if row[4]!= dmo_obj[0][3]:
				nm_session.execute("update demographic_detail set DOB='"+str(row[4])+"' where detail_id='"+str(detail_obj[0])+"'")
				nm_session.commit()

			# update Email
			communication_obj = session.execute("select id from communication where name_id = '"+str(name_obj[0][0])+"'")
			communication_obj = communication_obj.fetchone()

			email_obj = session.execute("select * from email where communication_id = '"+str(communication_obj[0])+"'")
			email_obj = email_obj.fetchall()

			if row[5]!= email_obj[0][1]:
				session.execute("update email set email='"+str(row[5])+"' where communication_id='"+str(communication_obj[0])+"'")
				session.commit()


			# update Location
			appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_id)+"'")
			appointment_obj = appointment_obj.fetchone()

			book_obj = session.execute("select id from book where appointment_id = '"+str(appointment_obj[0])+"'")
			book_obj = book_obj.fetchone()

			appt_obj = session.execute("select * from request_appt where book_id = '"+str(book_obj[0])+"'")
			appt_obj = appt_obj.fetchall()

			if row[8]!= appt_obj[0][2]:
				session.execute("update request_appt set location='"+str(row[8])+"' where book_id='"+str(book_obj[0])+"'")
				session.commit()

			# update Date, Time, Status
			visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0])+"'")
			visit_obj = visit_obj.fetchone()

			pf_obj = session.execute("select * from pf_appt_report where visit_id = '"+str(visit_obj[0])+"'")
			pf_obj = pf_obj.fetchall()

			pf_date = self.convert_date(unicodedata.normalize('NFKD',pf_obj[0][1]).encode('ascii','ignore'))

			if row[10]!= pf_obj[0][1] and row[11]!= pf_obj[0][2]:
				session.execute("update pf_appt_report set date='"+str(row[10])+"', time='"+str(row[11])+"',status='"+str(row[12])+"'  where visit_id='"+str(visit_obj[0])+"'")
				session.commit()

			# add intake
			intake_obj = session.execute("select * from intake where visit_id = '"+str(visit_obj[0])+"'")
			intake_obj = intake_obj.fetchall()

			if row[13]!= intake_obj[0][2]:
				session.execute("update intake set status='"+str(row[13])+"' where visit_id='"+str(visit_obj[0])+"'")
				session.commit()

			print 'DATABASE UPDATED SUCCESSFULLY..'


	def sync_data_from_db(self, client):

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
		g_sheet_scheduling = Base.classes.g_sheet_scheduling
		remind = Base.classes.remind
		history = Base.classes.history
		condition = Base.classes.condition
		rc_call_report = Base.classes.rc_call_report
		visit = Base.classes.visit
		pf_appt_report = Base.classes.pf_appt_report
		book = Base.classes.book
		request_appt = Base.classes.request_appt
		gen_availability = Base.classes.gen_availability


		patient_obj = session.query(patient).filter_by(mail_flag=0).all()
		for patient_ob in patient_obj:
			rows = self.sheet.get_all_values()

			row_count = len(rows)
			new_row = row_count + 1

			# add Id
			cell_name = 'A'+ str(new_row)
			self.sheet.update_acell(cell_name, patient_ob.id)

			# add title
			name_obj = session.execute("select * from name where patient_id = '"+str(patient_ob.id)+"'")
			name_obj = name_obj.fetchall()

			detail_obj = session.execute("select id from detail where name_id = '"+str(name_obj[0][0])+"'")
			detail_obj = detail_obj.fetchone()

			dmo_obj = session.execute("select * from demographic_detail where detail_id = '"+str(detail_obj[0])+"'")
			dmo_obj = dmo_obj.fetchall()
			if dmo_obj:
				sex = dmo_obj[0][2]

			cell_name = 'B'+ str(new_row)
			if 'F' in sex:
				title = 'Ms'
			else:
				title = 'Mr'
			self.sheet.update_acell(cell_name, title)

			# add First Name
			cell_name = 'C'+ str(new_row)
			self.sheet.update_acell(cell_name, name_obj[0][2])

			# add Last Name
			cell_name = 'D'+ str(new_row)
			self.sheet.update_acell(cell_name, name_obj[0][4])

			# add DOB
			cell_name = 'E'+ str(new_row)
			self.sheet.update_acell(cell_name, dmo_obj[0][3])

			# add Email
			communication_obj = session.execute("select id from communication where name_id = '"+str(name_obj[0][0])+"'")
			communication_obj = communication_obj.fetchone()

			email_obj = session.execute("select * from email where communication_id = '"+str(communication_obj[0])+"'")
			email_obj = email_obj.fetchall()

			cell_name = 'F'+ str(new_row)
			self.sheet.update_acell(cell_name, email_obj[0][1])

			# # add Text Address
			# cell_name = 'G'+ str(new_row)
			# self.sheet.update_acell(cell_name, appointment_obj.location)

			# # add Type
			# cell_name = 'F'+ str(new_row)
			# date_requested = appointment_obj.date_request1+' '+appointment_obj.date_request2
			# self.sheet.update_acell(cell_name, date_requested)

			# add Location
			appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_ob.id)+"'")
			appointment_obj = appointment_obj.fetchone()

			book_obj = session.execute("select id from book where appointment_id = '"+str(appointment_obj[0])+"'")
			book_obj = book_obj.fetchone()

			appt_obj = session.execute("select * from request_appt where book_id = '"+str(book_obj[0])+"'")
			appt_obj = appt_obj.fetchall()

			cell_name = 'I'+ str(new_row)
			self.sheet.update_acell(cell_name, appt_obj[0][2])

			# add date
			visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0])+"'")
			visit_obj = visit_obj.fetchone()

			pf_obj = session.execute("select * from pf_appt_report where visit_id = '"+str(visit_obj[0])+"'")
			pf_obj = pf_obj.fetchall()

			if pf_obj:
				pf_date = self.convert_date(unicodedata.normalize('NFKD',pf_obj[0][1]).encode('ascii','ignore'))

				# add time
				cell_name = 'L'+ str(new_row)
				self.sheet.update_acell(cell_name, pf_obj[0][2])

				# add status
				cell_name = 'M'+ str(new_row)
				self.sheet.update_acell(cell_name, pf_obj[0][3])

			cell_name = 'K'+ str(new_row)
			self.sheet.update_acell(cell_name, pf_date)

			# add day
			if pf_date:
				day = self.get_day(pf_date)
				# day = calendar.day_name[pf_date.weekday()]
				cell_name = 'J'+ str(new_row)
				self.sheet.update_acell(cell_name, day)



			# add intake
			intake_obj = session.execute("select * from intake where visit_id = '"+str(visit_obj[0])+"'")
			intake_obj = intake_obj.fetchall()

			cell_name = 'I'+ str(new_row)
			self.sheet.update_acell(cell_name,intake_obj[0][2])

			print 'GOOGLE SHEET UPDATED SUCCESSFULLY '

			#Change mail_flag
			# session.execute("update patient set intake_flag='1' where id='"+str(patient_ob.id)+"'")
			# session.commit()

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

	def get_day(self,date):
		"""
		Get day from a particular date format
		"""
		ALLOWED_FORMATS = ['%Y-%m-%d','%d-%m-%Y','%Y/%m/%d','%d/%m/%Y','%d.%m.%Y','%Y.%m.%d','%m-%d-%Y','%m/%d/%Y','%m.%d.%Y']
		for format in ALLOWED_FORMATS:
			try:
				return datetime.datetime.strptime(date, format).strftime('%A')
			except ValueError:
				pass


	def login(self):
		# use creds to create a client to interact with the Google Drive API
		scope = ["https://spreadsheets.google.com/feeds"]
		credentials = ServiceAccountCredentials.from_json_keyfile_name('ScrapeCentral-7c9473d82829.json', scope)
		client = gspread.authorize(credentials)
		return client


if __name__ == "__main__":
	d = GoogleSpreedSheetSync()