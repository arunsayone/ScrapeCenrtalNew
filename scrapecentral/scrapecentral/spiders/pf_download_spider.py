import re
import json
import time
import scrapy
import poplib
import logging
import requests
import ConfigParser

from scrapecentral.items import ScrapecentralItem


class PFDownloadSpider(scrapy.Spider):
    name = "pf_download"
    domain = "http://www.practicefusion.com/"
    start_urls = ["https://static.practicefusion.com/apps/ehr/?c=1385407302#/login"]

    config = ConfigParser.ConfigParser()
    section = config.read("/home/sayone/project/ScrapeCenrtalNew/scrapecentral/scrapecentral/config.ini")

    password_ = config.get('FUSION','password')
    login_ = config.get('FUSION','username')

    # gmail account details
    server = config.get('GMAIL','server')
    user  = config.get('GMAIL','username')
    password = config.get('GMAIL','password')

    def parse(self, response):

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

        #Run report
        run_report_url = "https://static.practicefusion.com/PatientEndpoint/api/v1/patients/list"
        run_report_payload = {"match":"all","query":[{"group":"demographic","key":"age","value":"0","comparator":">"}]}
        run_report_headers = {
            'authorization': session_tkn,
            'content-type': "application/json; charset=UTF-8",
            'cache-control': "no-cache",
            }

        run_response = requests.request("POST", run_report_url, data=json.dumps(run_report_payload), headers=run_report_headers)
        patient_data = json.loads(run_response.text)
        patient_data = patient_data['patientList']

        item = ScrapecentralItem()
        for patient in patient_data:
            item["pf_pt_id"] = patient['patientRecordNumber']
            item["pf_pt_street_add1"] = patient['streetAddress1']
            if 'mobilePhone' in patient:
                item["pf_pt_mob"] = patient['mobilePhone']
            else:
                item["pf_pt_mob"] = "No Number"
            item["pf_pt_firstName"] = patient['firstName']
            item["pf_pt_city"] = patient['city']
            item["pf_pt_lastName"] = patient['lastName']
            item["pf_pt_age"] = patient['age']
            item["pf_pt_dob"] = patient['birthDate']
            item["pf_pt_state"] = patient['state']
            if 'emailAddress' in patient:
                item["pf_pt_email"] = patient['emailAddress']
            else:
                item["pf_pt_email"] = 'No email'
            item["pf_pt_gender"] = patient['gender']
            item["pf_pt_zip"] = patient['postalCode']

        yield item

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