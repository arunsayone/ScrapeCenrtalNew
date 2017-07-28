
import pyodbc
import json
import sqlalchemy
import urllib
import unicodedata
import datetime

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



class GoogleSpreedSheetSync(object):

	def __init__(self):
		client = self.login()
		self.sheet = client.open("C.Patients").sheet1
		# self.sync_data_from_db(client)
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
		workflow = Base.classes.workflow

		rows = self.sheet.get_all_values()
		for row in rows[1:]:
			patient_id = row[0]

			# update Name
			name_obj = session.execute("select * from name where patient_id = '"+str(patient_id)+"'")
			name_obj = name_obj.fetchall()

			print '\n\n',name_obj

			nm_session = Session(engine)
			full_name = name_obj[0][2]+' '+name_obj[0][4]
			if row[1]!= full_name:
				fname = row[1].split(' ')[0]
				lname = row[1].split(' ')[1]
				nm_session.execute("update name set fname='"+str(fname)+"',lname='"+str(lname)+"' where id='"+str(name_obj[0][0])+"'")
				nm_session.commit()

			# update DOB
			detail_obj = session.execute("select id from detail where name_id = '"+str(name_obj[0][0])+"'")
			detail_obj = detail_obj.fetchone()

			dmo_obj = session.execute("select * from demographic_detail where detail_id = '"+str(detail_obj[0])+"'")
			dmo_obj = dmo_obj.fetchall()

			db_dob = self.convert_date(unicodedata.normalize('NFKD',dmo_obj[0][3]).encode('ascii','ignore'))
			sheet_dob = self.convert_date(row[2])

			if sheet_dob!= db_dob:
				nm_session.execute("update demographic_detail set DOB='"+str(sheet_dob)+"' where detail_id='"+str(detail_obj[0])+"'")
				nm_session.commit()

			# update Date, Time, Status, type
			appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_id)+"'")
			appointment_obj = appointment_obj.fetchone()

			visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0])+"'")
			visit_obj = visit_obj.fetchone()

			pf_obj = session.execute("select * from pf_appt_report where visit_id = '"+str(visit_obj[0])+"'")
			pf_obj = pf_obj.fetchall()

			db_date = self.convert_date(unicodedata.normalize('NFKD',pf_obj[0][1]).encode('ascii','ignore'))
			sheet_date = self.convert_date(row[1])

			db_status = pf_obj[0][3]
			sheet_status = row[5]

			db_type = pf_obj[0][10]
			sheet_type = row[6]

			if sheet_date!= db_date:
				session.execute("update pf_appt_report set date='"+str(sheet_date)+"' where visit_id='"+str(visit_obj[0])+"'")
				session.commit()

			if sheet_status!= db_status:
				session.execute("update pf_appt_report set status='"+str(sheet_status)+"' where visit_id='"+str(visit_obj[0])+"'")
				session.commit()

			if sheet_type!= db_type:
				session.execute("update pf_appt_report set appointment_type='"+str(sheet_type)+"' where visit_id='"+str(visit_obj[0])+"'")
				session.commit()

			# update details from representative
			if rows[7]:
				session.add(workflow(type=rows[7], istop=rows[8], records=rows[9], labs=rows[10], MMP=rows[11], billing=rows[12], note=rows[13],  visit_id=visit_obj[0]))
				session.commit()
				session.close()


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


		patient_obj = session.query(patient).filter_by(patient_flag=0).all()
		for patient_ob in patient_obj:
			rows = self.sheet.get_all_values()

			row_count = len(rows)
			new_row = row_count + 1

			# add Id
			cell_name = 'A'+ str(new_row)
			self.sheet.update_acell(cell_name, patient_ob.id)

			# add Name
			asso_session = Session(engine)
			association_obj = asso_session.execute("select id from association where patient_id = '"+str(patient_ob.id)+"'")
			association_obj = association_obj.fetchone()

			name_obj = session.execute("select * from name where association_id = '"+str(association_obj[0])+"'")
			name_obj = name_obj.fetchall()

			full_name = name_obj[0][2]+' '+name_obj[0][4]

			cell_name = 'B'+ str(new_row)
			self.sheet.update_acell(cell_name, full_name)

			# add DOB
			name_obj = session.execute("select * from name where patient_id = '"+str(patient_ob.id)+"'")
			name_obj = name_obj.fetchall()

			detail_obj = session.execute("select id from detail where name_id = '"+str(name_obj[0][0])+"'")
			detail_obj = detail_obj.fetchone()

			dmo_obj = session.execute("select * from demographic_detail where detail_id = '"+str(detail_obj[0])+"'")
			dmo_obj = dmo_obj.fetchall()
			dob = dmo_obj[0][3]

			cell_name = 'C'+ str(new_row)
			self.sheet.update_acell(cell_name, dob)

			# add Date
			appointment_obj = session.execute("select id from appointment where patient_id = '"+str(patient_ob.id)+"'")
			appointment_obj = appointment_obj.fetchone()

			visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0])+"'")
			visit_obj = visit_obj.fetchone()

			pf_obj = session.execute("select * from pf_appt_report where visit_id = '"+str(visit_obj[0])+"'")
			pf_obj = pf_obj.fetchall()

			pf_date = self.convert_date(unicodedata.normalize('NFKD',pf_obj[0][1]).encode('ascii','ignore'))

			cell_name = 'D'+ str(new_row)
			self.sheet.update_acell(cell_name, pf_date)

			# add time
			cell_name = 'E'+ str(new_row)
			self.sheet.update_acell(cell_name, pf_obj[0][2])

			# add status
			cell_name = 'F'+ str(new_row)
			self.sheet.update_acell(cell_name, pf_obj[0][3])

			# add type
			cell_name = 'G'+ str(new_row)
			self.sheet.update_acell(cell_name,pf_obj[0][10])


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


	def login(self):
		# use creds to create a client to interact with the Google Drive API
		scope = ["https://spreadsheets.google.com/feeds"]
		credentials = ServiceAccountCredentials.from_json_keyfile_name('ScrapeCentral-7c9473d82829.json', scope)
		client = gspread.authorize(credentials)
		return client


if __name__ == "__main__":
	d = GoogleSpreedSheetSync()