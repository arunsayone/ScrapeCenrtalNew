import pyodbc
import json
import sqlalchemy
import datetime
import urllib
import gspread

from dateutil.parser import parse
from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect

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


class GoogleSheetWriteToXlsx(object):

    def __init__(self):
        client = self.login()
        self.sheet = client.open("UploadRC1.xlsx").sheet1
        self.add_data_to_sheet(client)
        # self.sync_data_from_db(client)

    def add_data_to_sheet(self, client):

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


        patient_obj = session.query(patient).filter_by(rc_flag=0).all()
        for patient_ob in patient_obj:

            rows = self.sheet.get_all_values()
            row_count = len(rows)
            new_row = row_count + 1

            # add Id
            cell_name = 'A'+ str(new_row)
            self.sheet.update_acell(cell_name, patient_ob.id)

            # add First Name
            asso_session = Session(engine)
            association_obj = asso_session.execute("select id from association where patient_id = '"+str(patient_ob.id)+"'")
            association_obj = association_obj.fetchone()

            na_session = Session(engine)
            name_obj = na_session.execute("select * from name where association_id = '"+str(association_obj[0])+"'")
            name_obj = name_obj.fetchall()

            cell_name = 'B'+ str(new_row)
            self.sheet.update_acell(cell_name, name_obj[0][2])
            na_session.close()

            # add Last Name
            cell_name = 'C'+ str(new_row)
            self.sheet.update_acell(cell_name, name_obj[0][4])
            asso_session.close()

            # add Phone
            cm_session = Session(engine)
            communication_obj = na_session.execute("select id from communication where name_id = '"+str(name_obj[0][0])+"'")
            communication_obj = communication_obj.fetchone()
            cm_session.close()

            ph_session = Session(engine)
            phone_obj = na_session.execute("select * from phone where communication_id = '"+str(communication_obj[0])+"'")
            phone_obj = phone_obj.fetchall()
            ph_session.close()

            cell_name = 'D'+ str(new_row)
            self.sheet.update_acell(cell_name, phone_obj[0][1])

            # add Date
            ap_session = Session(engine)
            appointment_obj = ap_session.execute("select id from appointment where patient_id = '"+str(patient_ob.id)+"'")
            appointment_obj = appointment_obj.fetchone()
            ap_session.close()

            book_obj = session.execute("select id from book where appointment_id = '"+str(appointment_obj[0])+"'")
            book_obj = book_obj.fetchone()

            cell_name = 'F'+ str(new_row)
            ra_session = Session(engine)
            appt_obj = ra_session.execute("select * from request_appt where book_id = '"+str(book_obj[0])+"'")
            appt_obj = appt_obj.fetchall()
            ra_session.close()

            date = appt_obj[0][7]
            if ',' in date:
                dt = parse(date)
                date = dt.strftime('%d/%m/%Y')

            print 'date......\t',date
            self.sheet.update_acell(cell_name, date)

            # add Day
            date_str = str(date)
            day, month, year = (int(x) for x in date_str.split('/'))
            date_convert = datetime.date(year, month, day)
            print date_convert.strftime("%A")
            day = date_convert.strftime("%A")
            cell_name = 'E'+ str(new_row)
            self.sheet.update_acell(cell_name, day)

            # add Time
            cell_name = 'G'+ str(new_row)
            self.sheet.update_acell(cell_name, appt_obj[0][8])

            # add Type
            visit_obj = session.execute("select id from visit where appointment_id = '"+str(appointment_obj[0])+"'")
            visit_obj = visit_obj.fetchone()

            pf_obj = session.execute("select * from pf_appt_report where visit_id = '"+str(visit_obj[0])+"'")
            pf_obj = pf_obj.fetchall()

            cell_name = 'H'+ str(new_row)
            self.sheet.update_acell(cell_name,pf_obj[0][10])

            #add status
            intake_obj = session.execute("select * from intake where visit_id = '"+str(visit_obj[0])+"'")
            intake_obj = intake_obj.fetchall()

            cell_name = 'I'+ str(new_row)
            self.sheet.update_acell(cell_name,intake_obj[0][2])

            if 'New' in pf_obj[0][10] and intake_obj[0][2]=='sent':
                add1 = 'Before your visit please try to complete the patient intake sent to your email.'
                cell_name = 'J'+ str(new_row)
                self.sheet.update_acell(cell_name,add1)

            elif 'Follow' in pf_obj[0][10] and intake_obj[0][2]=='sent':
                add2 = 'Before your visit please try to complete the patient followup document sent to your email.'

                cell_name = 'K'+ str(new_row)
                self.sheet.update_acell(cell_name,add2)

            # int_session = Session(engine)
            # int_session.execute("update patient set rc_flag='1' where id='"+str(patient_obj.id)+"'")
            # int_session.commit()
            # int_session.close()



    def login(self):
        # use creds to create a client to interact with the Google Drive API
        scope = ["https://spreadsheets.google.com/feeds"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name('ScrapeCentral-7c9473d82829.json', scope)
        client = gspread.authorize(credentials)
        return client


if __name__ == "__main__":
    d = GoogleSheetWriteToXlsx()