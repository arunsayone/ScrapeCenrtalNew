import csv
import re
import pyodbc
import json
import sqlalchemy
import urllib
from sqlalchemy import text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, mapper, sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

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

with open('/home/sayone/project/ScrapeCentral/scrapecentral/scrapecentral/PatientList_EHRmdoctormmcom_2017-07-13.csv', 'rb') as f:
    reader = csv.reader(f)
    all_data = list(reader)

header = all_data[0]

results = all_data[1:]

count = 0
for result in results:
    print 'ssssssss',result[3]
    patient_last_name = result[0]
    patient_first_name = result[1]
    patient_middle_name = result[2]
    # pt_id = result[3]
    sex = result[4]
    dob = result[5]
    age = result[6]
    addr_l1 = result[7]
    addr_l2 = result[8]
    city = result[9]
    state = result[10]
    postal_code = result[11]
    recent_visit_date = result[12]
    mtd_of_commu = result[13]
    home_phone = result[14]
    mobile_phone = result[15]
    office_phone = result[16]
    mail = result[17]
    active = result[18]
    fake = result[19]
    ethinicity = result[20]
    pre_lan = result[21]
    ethnicity_pref_lan_eff_date = result[22]
    race = result[23]
    race_eff_date = result[24]
    smoke_status = result[25]
    smoke_status_eff_date = result[26]

    patient = Base.classes.patient
    association = Base.classes.association
    name = Base.classes.name
    demographic_detail = Base.classes.demographic_detail
    detail = Base.classes.detail
    address = Base.classes.address
    phone = Base.classes.phone
    email = Base.classes.email
    history_medical_disease = Base.classes.history_medical_disease
    medical = Base.classes.medical
    communication = Base.classes.communication
    condition = Base.classes.condition
    # pt_id = 'DP12'
    
    # patient table
    session.execute("insert into patient(patient_id)values('"+str(pt_id)+"')")
    session.commit()

    # get id from patient table
    pt = session.execute("select id from patient where patient_id = '"+pt_id+"'")

    # association table
    # pt = dict(pt.fetchall())

    pt = pt.fetchone()
    print '.........;;;;;',pt[0]
    session.add(association(patient_id=pt[0], relationship='self'))
    session.commit()

    # name table
    association = session.execute("select id from association where patient_id = '"+str(pt[0])+"'")
    # session.commit()
    association = association.fetchone()

    print 'association....',association[0]
    session.add(name(fname=patient_first_name, mname =patient_middle_name, lname =patient_last_name, patient_id =pt.id, association_id = association[0]))
    session.commit()

    # add demographic_detail
    try:
        name_ob = session.execute("select id from name where association_id = '"+str(association[0])+"'")
        detail_obj = session.execute("select id from detail where name_id = '"+str(name_ob.fetchone()[0])+"'")
        session.add(demographic_detail(age=age,sex=sex,DOB=dob,ethnicity = ethinicity,pre_lang =pre_lan,detail_id = detail_obj.fetchone()[0],race=race,race_effective_date=race_eff_date,smoking_status=smoke_status,smoking_status_eff_date=smoke_status_eff_date,ethnicity_pref_lan_eff_date=ethnicity_pref_lan_eff_date))
        session.commit()

    except:
        name_ob = session.execute("select id from name where association_id = '"+str(association[0])+"'")
        # print 'name....',name_ob.fetchone()[0]
        nm_id = name_ob.fetchone()[0]
        session.add(detail(name_id=nm_id))
        session.commit()

        # nm_id = name_ob.fetchone()[0]
        detail_obj = session.execute("select id from detail where name_id = '"+str(nm_id)+"'")
        session.add(demographic_detail(age=age,sex=sex,DOB=dob,ethnicity = ethinicity,pre_lang =pre_lan,detail_id = detail_obj.fetchone()[0],race=race,race_effective_date=race_eff_date,smoking_status=smoke_status,smoking_status_eff_date=smoke_status_eff_date,ethnicity_pref_lan_eff_date=ethnicity_pref_lan_eff_date))
        session.commit()
    
    # address table
    try:
        communication_obj = session.execute("select id from communication where name_id = '"+str(nm_id)+"'")
        session.add(address(line_1=addr_l1, city=city, state =state, zip=postal_code,communication_id=communication_obj.fetchone()[0]))
        session.commit()
    except:
        session.add(communication(name_id=nm_id))
        session.commit()
        communication_obj = session.execute("select id from communication where name_id = '"+str(nm_id)+"'")
        cm_id = communication_obj.fetchone()[0]
        session.add(address(line_1=addr_l1, city=city, state =state, zip=postal_code,communication_id=cm_id))
        session.commit()

    # phone table
    session.add(phone(home=home_phone,phone_number=mobile_phone,work=office_phone, communication_id=cm_id))
    session.commit()

    # email table
    session.add(email(email=mail,fake=fake,active=active, communication_id=cm_id))
    session.commit()

    # history_medical_disease table

    # medical table
    session.add(medical(patient_id=pt[0]))
    session.commit()

    try:
        medical_id = session.execute("select id from medical where patient_id = '"+str(pt[0])+"'")
        md_id = medical_id.fetchone()[0]
        condition = session.execute("select id from condition where medical_id = '"+str(md_id)+"'")
        cn_id = condition.fetchone()[0]
        session.add(history_medical_disease(treat_last_visit=recent_visit_date, condition_id = cn_id))
        session.commit()
    except:
        medical_id = session.execute("select id from medical where patient_id = '"+str(pt[0])+"'")
        md_id = medical_id.fetchone()[0]

        # print'md.....',md_id
        session.execute("insert into condition(medical_id)values('"+str(md_id)+"')")
        # session.add(condition(medical_id=md_id))
        session.commit()

        condition = session.execute("select id from condition where medical_id = '"+str(md_id)+"'")
        cn_id = condition.fetchone()[0]
        session.add(history_medical_disease(treat_last_visit=recent_visit_date, condition_id = cn_id))
        session.commit()

    print("SAVE:")

