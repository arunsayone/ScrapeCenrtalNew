import time
import urllib
import gspread
import unicodedata
import ConfigParser

from datetime import datetime

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


class GoogleSpreedSheetSync(object):

	def __init__(self):
		client = self.login()

		self.sheet = client.open("sql").sheet1
		self.sync_data_to_db(client)

	def sync_data_to_db(self, client):
		"""
		Fetch data from intake sheet and update with user
		"""
		# Declare tables here
		patient = Base.classes.patient
		association = Base.classes.association
		name = Base.classes.name
		physician = Base.classes.physician
		communication = Base.classes.communication
		phone = Base.classes.phone
		location = Base.classes.location
		record = Base.classes.record

		session = Session(engine)

		patients_obj = session.query(patient).filter_by(intake_flag=0).all()
		for patient_obj in patients_obj:
			rows = self.sheet.get_all_values()

			for row in rows[1:]:

				sheet_pt_name = row[1]
				sheet_pt_dob = self.convert_date(row[13])

				name_id = session.query(name).filter_by(patient_id=patient_obj.id).first()
				if name_id:
					name_obj = session.execute("select * from name where patient_id = '"+str(patient_obj.id)+"'")
					name_obj = name_obj.fetchall()
					db_pt_name = name_obj[0][2]+' '+name_obj[0][4]

					detail_obj = session.execute("select id from detail where name_id = '"+str(name_obj[0][0])+"'")
					dt_id = detail_obj.fetchone()[0]

					demographic_obj = session.execute("select * from demographic_detail where detail_id = '"+str(dt_id)+"'")
					demographic_obj = demographic_obj.fetchall()
					if demographic_obj:
						db_pt_dob = self.convert_date(unicodedata.normalize('NFKD',demographic_obj[0][3]).encode('ascii','ignore'))
						if sheet_pt_name == db_pt_name and sheet_pt_dob == db_pt_dob:
							print 'MATCH FOUND....'

							time.sleep(4)
							cm_session = Session(engine)
							communication_obj = cm_session.execute("select id from communication where name_id = '"+str(name_obj[0][0])+"'")
							phone_obj = cm_session.execute("select * from phone where communication_id = '"+str(communication_obj.fetchone()[0])+"'")
							ph_obj = phone_obj.fetchall()
							cm_session.close()

							demographic_detail_obj = session.execute("select * from demographic_detail where detail_id = '"+str(dt_id)+"'")
							dm_id = demographic_detail_obj.fetchall()

							rc_session = Session(engine)
							record_obj = rc_session.execute("select id from record where name_id = '"+str(name_obj[0][0])+"'")
							rc_id = record_obj.fetchone()[0]
							rc_session.close()

							lc_session = Session(engine)
							location_obj = lc_session.execute("select * from location where record_id = '"+str(rc_id)+"'")
							lc_obj = location_obj.fetchall()
							lc_session.close()

							demographic_employment_obj = session.execute("select * from demographic_employment where detail_id = '"+str(dt_id)+"'")
							dmo_obj = demographic_employment_obj.fetchall()

							ass_session = Session(engine)
							association_obj = ass_session.execute("select id from association where patient_id = '"+str(patient_obj.id)+"'")
							as_id = association_obj.fetchone()[0]
							ass_session.close()

							physician_obj = session.execute("select * from physician where association_id = '"+str(as_id)+"'")
							phy_obj = physician_obj.fetchall()

							in_session = Session(engine)
							insurance_obj = in_session.execute("select * from insurance where patient_id = '"+str(patient_obj.id)+"'")
							ins_obj = insurance_obj.fetchall()
							in_session.close()

							# pat_obj = session.execute("select * from patient where patient_id = '"+str(patient_obj.id)+"'")

							# table phone
							ph_session = Session(engine)
							ph_session.execute("update phone set home='"+str(row[9])+"',work='"+str(row[10])+"',cell='"+str(row[11])+"' where id='"+str(ph_obj[0][0])+"'")
							ph_session.commit()
							ph_session.close()

							# table patient
							session.execute("update patient set social_sec_no='"+str(row[12])+"' where id='"+str(patient_obj.id)+"'")
							session.commit()

							# table demographic_detail
							session.execute("update demographic_detail set marital_status='"+str(row[15])+"', age='"+str(row[390])+"' where id='"+str(dm_id[0][0])+"'")
							session.commit()

							#table location
							session.execute("update location set location_google='"+str(row[392])+"' where id='"+str(lc_obj[0][0])+"'")
							session.commit()

							#employer details
							session.execute("update demographic_employment set employer='"+str(row[24])+"' where id='"+str(dmo_obj[0][0])+"'")
							session.commit()

							#physician details
							phy_session = Session(engine)
							phy_name = row[35]
							if ' ' in phy_name:
								fname = phy_name.split(' ')[0]+' '+phy_name.split(' ')[1]
								lname = phy_name.split(' ')[2]
								phy_session.execute("update physician set fname='"+str(fname)+"',lname ='"+str(lname)+"',phone ='"+str(row[36])+"', fax ='"+str(row[37])+"' where id='"+str(phy_obj[0][0])+"'")
							else:
								fname = row[35]
								lname = 'None'
								phy_session.execute("update physician set fname='"+str(fname)+"',lname ='"+str(lname)+"',phone ='"+str(row[36])+"', fax ='"+str(row[37])+"' where id='"+str(phy_obj[0][0])+"'")
							phy_session.commit()
							phy_session.close()

							#insurance details  #table insurance
							session.execute("update insurance set name='"+str(row[43])+"',policy ='"+str(row[44])+"',i_group='"+str(row[45])+"' where id='"+str(ins_obj[0][0])+"'")
							session.commit()

							print 'table values updated successfully'
				else:
					print 'MATCH ID NOT FOUND....'

	def convert_date(self,date):
		"""
		Convert different date format to single date format
		"""
		ALLOWED_FORMATS = ['%Y-%m-%d','%d-%m-%Y','%Y/%m/%d','%d/%m/%Y','%d.%m.%Y','%Y.%m.%d','%m-%d-%Y','%m/%d/%Y','%m.%d.%Y']
		for format in ALLOWED_FORMATS:
			try:
				return datetime.strptime(date, format).strftime('%d-%m-%Y')
			except ValueError:
				pass

	def login(self):
		"""
		use credentials to create a client to interact with the Google Drive API
		"""
		scope = ["https://spreadsheets.google.com/feeds"]
		credentials = ServiceAccountCredentials.from_json_keyfile_name('ScrapeCentral-7c9473d82829.json', scope)
		client = gspread.authorize(credentials)
		return client

if __name__ == "__main__":
	d = GoogleSpreedSheetSync()
