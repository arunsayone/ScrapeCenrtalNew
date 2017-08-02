import urllib
import gspread
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

		self.sheet = client.open("C.Scheduling").sheet1
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
		remind = Base.classes.remind
		history = Base.classes.history
		condition = Base.classes.condition
		rc_call_report = Base.classes.rc_call_report
		visit = Base.classes.visit
		pf_appt_report = Base.classes.pf_appt_report
		book = Base.classes.book
		request_appt = Base.classes.request_appt
		call_book_appt = Base.classes.call_book_appt
		qualify = Base.classes.qualify
		intake = Base.classes.intake

		rows = self.sheet.get_all_values()
		for row in rows[1:]:
			patient_id = row[0]

			# update Name
			name_obj = session.execute("select * from name where patient_id = '"+str(patient_id)+"'")
			name_obj = name_obj.fetchall()

			nm_session = Session(engine)
			full_name = name_obj[0][2]+' '+name_obj[0][4]
			if row[1]!= full_name:
				fname = row[1].split(' ')[0]
				lname = row[1].split(' ')[1]
				nm_session.execute("update name set fname='"+str(fname)+"',lname='"+str(lname)+"' where id='"+str(name_obj[0][0])+"'")
				nm_session.commit()

			# update REQUESTED ON
			appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_id)+"'")
			appointment_obj = appointment_obj.fetchone()

			book_obj = session.execute("select id from book where appointment_id = '"+str(appointment_obj[0])+"'")
			book_obj = book_obj.fetchone()

			request_appt_obj = session.execute("select * from request_appt where book_id = '"+str(book_obj[0])+"'")
			request_appt_obj = request_appt_obj.fetchall()

			# update DATES REQUESTED
			date_requested = request_appt_obj[0][5]+';'+request_appt_obj[0][9]
			date_request1 = str(row[5]).split(';')[0]
			date_request2 = str(row[5]).split(';')[1]

			if row[2]!=request_appt_obj[0][1] and row[3]!=request_appt_obj[0][4]:
				session.execute("update request_appt set requested_on='"+str(row[1])+"',type='"+str(row[4])+"',location='"+str(row[2])+"',date_request1='"+str(date_request1)+"' date_request2='"+str(date_request2)+"',pt_notes='"+str(row[11])+"' where book_id='"+str(book_obj[0])+"'")
				request_appt_obj.requested_on = row[2]
				session.commit()

			# update PHONE
			communication_obj = session.execute("select id from communication where name_id = '"+str(name_obj[0][0])+"'")
			communication_obj = communication_obj.fetchone()

			phone_obj = session.execute("select * from phone where communication_id = '"+str(communication_obj[0])+"'")
			phone_obj = phone_obj.fetchall()

			if row[6]!=phone_obj[0][1]:
				session.execute("update phone set phone_number='"+str(row[6])+"' where communication_id='"+str(communication_obj[0])+"'")
				session.commit()

			# update EMAIL
			email_obj = session.execute("select * from email where communication_id = '"+str(communication_obj[0])+"'")
			email_obj = email_obj.fetchall()
			if row[7]!=email_obj[0][1]:
				session.execute("update email set email='"+str(row[7])+"' where communication_id='"+str(communication_obj[0])+"'")
				session.commit()

			# update Conditions
			medical = session.query(medical).filter_by(patient_id=patient_id).first()
			condition_obj = session.query(condition).filter_by(medical_id=medical.id).first()
			qualify_obj = session.query(qualify).filter_by(condition_id=condition_obj.id).first()

			medical_obj = session.execute("select id from medical where patient_id = '"+str(patient_id)+"'")
			medical_obj = medical_obj.fetchone()

			condition_obj = session.execute("select id from condition where medical_id = '"+str(medical_obj[0])+"'")
			condition_obj = condition_obj.fetchone()

			qualify_obj = session.execute("select * from qualify where condition_id = '"+str(condition_obj[0])+"'")
			qualify_obj = qualify_obj.fetchall()

			if row[8]!=qualify_obj[0][1]:
				session.execute("update qualify set name='"+str(row[6])+"' where condition_id='"+str(condition_obj[0])+"'")
				session.commit()

			# # update Flag
			# Error in doc

			# update details from representative
			if row[11]:
				session.add(call_book_appt(date_last_call=row[11], time_last_call=row[12], call_no=row[13], status=row[14], action=row[15], notes=row[17], book_id = book_obj[0]))
				session.commit()

			# update intake status from representative
			visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0])+"'")
			visit_obj = visit_obj.fetchone()

			if row[18]:
				session.add(intake(type='Not specified', status=row[18], source='Not specified', last_series='Not specified', date_update='Not specified', prior_series='Not specified', visit_id=visit_obj[0]))
				session.commit()

			print 'DATABASE UPDATED SUCCESSFULLY..'


	def sync_data_from_db(self, client):

		patient = Base.classes.patient
		association = Base.classes.association
		name = Base.classes.name
		appointment = Base.classes.appointment
		communication = Base.classes.communication
		phone = Base.classes.phone
		email = Base.classes.email
		book = Base.classes.book
		request_appt = Base.classes.request_appt
		medical = Base.classes.medical
		qualify = Base.classes.qualify
		condition = Base.classes.condition


		patients_obj = session.query(patient).filter_by(schedule_flag=0).all()
		for patient_ob in patients_obj:

			rows = self.sheet.get_all_values()
			row_count = len(rows)
			new_row = row_count + 1

			# add Id
			cell_name = 'A'+ str(new_row)
			self.sheet.update_acell(cell_name, patient_ob.id)

			# add NAME
			asso_session = Session(engine)
			association_obj = asso_session.execute("select id from association where patient_id = '"+str(patient_ob.id)+"'")
			association_obj = association_obj.fetchone()

			name_obj = session.execute("select * from name where association_id = '"+str(association_obj[0])+"'")
			name_obj = name_obj.fetchall()

			if name_obj:
				full_name = name_obj[0][2]+' '+name_obj[0][4]

			cell_name = 'B'+ str(new_row)
			self.sheet.update_acell(cell_name, full_name)

			# add REQUESTED ON
			appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_ob.id)+"'")
			appointment_obj = appointment_obj.fetchone()

			book_obj = session.execute("select id from book where appointment_id = '"+str(appointment_obj[0])+"'")
			book_obj = book_obj.fetchone()

			request_appt_obj = session.execute("select * from request_appt where book_id = '"+str(book_obj[0])+"'")
			request_appt_obj = request_appt_obj.fetchall()

			cell_name = 'C'+ str(new_row)
			self.sheet.update_acell(cell_name, request_appt_obj[0][1])

			# add TYPE
			cell_name = 'D'+ str(new_row)
			self.sheet.update_acell(cell_name, request_appt_obj[0][4])

			# add LOCATION
			cell_name = 'E'+ str(new_row)
			self.sheet.update_acell(cell_name, request_appt_obj[0][2])

			# add DATES REQUESTED
			cell_name = 'F'+ str(new_row)
			date_requested = str(request_appt_obj[0][7])+';'+str(request_appt_obj[0][9])
			self.sheet.update_acell(cell_name, date_requested)

			if name_obj:
				# add PHONE
				communication_obj = session.execute("select id from communication where name_id = '"+str(name_obj[0][0])+"'")
				communication_obj = communication_obj.fetchone()
				cell_name = 'G'+ str(new_row)

			phone_obj = session.execute("select * from phone where communication_id = '"+str(communication_obj[0])+"'")
			phone_obj = phone_obj.fetchall()
			self.sheet.update_acell(cell_name, phone_obj[0][1])

			# add EMAIL
			cell_name = 'H'+ str(new_row)
			email_obj = session.execute("select * from email where communication_id = '"+str(communication_obj[0])+"'")
			email_obj = email_obj.fetchall()
			self.sheet.update_acell(cell_name, email_obj[0][1])

			# add Conditions
			medical_obj = session.execute("select id from medical where patient_id = '"+str(patient_ob.id)+"'")
			medical_obj = medical_obj.fetchone()

			condition_obj = session.execute("select id from condition where medical_id = '"+str(medical_obj[0])+"'")
			condition_obj = condition_obj.fetchone()
			qualify_obj = session.execute("select * from qualify where condition_id = '"+str(condition_obj[0])+"'")
			qualify_obj = qualify_obj.fetchall()
			print 'qualifyyy...\n',qualify_obj
			if qualify_obj:
				cell_name = 'I'+ str(new_row)
				self.sheet.update_acell(cell_name, qualify_obj[0][1])

			# add Flag
			# Working on this field in sql

			# add Notes
			cell_name = 'K'+ str(new_row)
			self.sheet.update_acell(cell_name, request_appt_obj[0][11])

			print 'GOOGLE SHEET UPDATED SUCCESSFULLY '
			# patient_status = session.query(patient).filter_by(patient_id=patient_obj.id).first()
			# patient_status.schedule_flag = '1'
			# session.commit()

			# These fields are added by representatives
			# add DATE CALLED
			# add TIME
			# add CALLS
			# add STATUS
			# add ACTION
			# add OTHER
			# add ADDITIONAL NOTES
			# add EMAIL



	def login(self):
		# use creds to create a client to interact with the Google Drive API
		scope = ["https://spreadsheets.google.com/feeds"]
		credentials = ServiceAccountCredentials.from_json_keyfile_name('ScrapeCentral-7c9473d82829.json', scope)
		client = gspread.authorize(credentials)
		return client


if __name__ == "__main__":
	d = GoogleSpreedSheetSync()