import re
import os
import scrapy
import ConfigParser

from lxml import html
from scrapecentral.items import ScrapecentralItem


class MDSpider(scrapy.Spider):
    name = "md"
    domain = "https://www.marijuanadoctors.com"
    start_urls = ["https://www.marijuanadoctors.com/user/practice/index"]
    conf_path = os.path.dirname(os.path.abspath(__file__))+'/config.ini'

    config = ConfigParser.ConfigParser()
    section = config.read(conf_path)

    login_user = config.get('DOCTORS','username')
    login_pass = config.get('DOCTORS','password')


    # def __init__(self):
    #     """
    #     Creating header names for the CSV file
    #     :param data: self
    #     :return:
    #     """
        # self.filepath = '/home/sayone/projects/Scrapy/scrapecentral/scrapecentral/md_data.csv'
        # if os.path.exists(self.filepath) is False:
        #     with open('md_data.csv', 'a') as csvfile:
        #         writer = csv.DictWriter(csvfile, fieldnames=self.detail_field_set)
        #         writer.writeheader()

    def parse(self, response):
        """
        Login section to marijuanadoctors.com
        :param data: response
        :return:
        """
        doc = html.fromstring(response.body)
        qf = doc.xpath('//input[@name="qf"]/@value')[1]
        payload = {
            "login_full__fields__email": self.login_user,
            "login_full__fields__password": self.login_pass,
            "qf": qf,
        }

        yield scrapy.FormRequest.from_response(
            response,
            formxpath='//form[@id="qf_login_full"]',
            formdata=payload,
            callback=self.parse_patient_basic_data
        )

    def parse_patient_basic_data(self, response):
        """
        Fetch patient basic details
        :param data: response
        :return:
        """
        if "There are no new appointment requests" in response.body:
            print 'No appointments present..'
            # item = ScrapecentralItem()
            # yield scrapy.Request(
            #             # url="https://www.marijuanadoctors.com/user/practice/appointment/view_request/120805",
            #             url="https://www.marijuanadoctors.com/user/practice/appointment/view_request/121355",
            #             meta = {'item':item},
            #             callback = self.parse_patient_details)
        else:
            doc = html.fromstring(response.body)
            appoinment_data = doc.xpath('//table[@id="appointment_list_new"]/tbody/tr')
            # appoinment = doc.xpath('//table[@id="appointment_list_new"]/tbody/tr')[0]
            for appoinment in appoinment_data:
                item = ScrapecentralItem()
                item["requested_on"] = appoinment.xpath('./td/text()')[0]
                item["location"] = appoinment.xpath('./td/text()')[1]
                item["patient_name"] = appoinment.xpath('./td/text()')[2]
                item["type"] = appoinment.xpath('./td/text()')[3]
                item["dates_requested"] = appoinment.xpath('./td/text()')[4]
                url = appoinment.xpath('./td/a/@href')
                yield scrapy.Request(
                            url=self.domain + url[0],
                            callback = self.parse_appointment_details,
                            meta = {'item': item},
                            dont_filter=True)

    def parse_appointment_details(self, response):
        """
        Fetch patient appointment details
        :param data: response
        :return:
        """
        doc = html.fromstring(response.body)
        meta = response.meta
        meta['url'] = response.url
        url = doc.xpath('//a[@id="green"]/@href')
        yield scrapy.Request(
                        url=self.domain + url[0],
                        callback = self.unlock_contact_post,
                        meta = meta,
                        dont_filter=True)

    def unlock_contact_post(self, response):
        """
        Generating a post request to unlock the contact data
        :param data: response
        :return:
        """
        # url = "https://www.marijuanadoctors.com/user/practice/appointment/accept/115315"
        doc = html.fromstring(response.body)
        meta = response.meta
        id = doc.xpath('//input[@name="appointment_accept__appointment_id"]/@value')
        type_id = doc.xpath('//input[@name="appointment_accept__appointment_type_id"]/@value')
        comment_type_id = doc.xpath('//input[@name="appointment_accept__appointment_comment_type_id"]/@value')
        comment_type_text = doc.xpath('//input[@name="appointment_accept__appointment_comment_type_text"]/@value')
        qf = doc.xpath('//input[@name="qf"]/@value')[1]

        payload = {
            "appointment_accept__appointment_id": id[0],
            "appointment_accept__appointment_type_id": type_id[0],
            "appointment_accept__appointment_comment_type_id": comment_type_id[0],
            "appointment_accept__appointment_comment_type_text": comment_type_id[0],
            "appointment_accept__content": "",
            "qf": qf,
        }

        yield scrapy.FormRequest.from_response(
            response,
            formxpath='//form[@id="qf_appointment_accept"]',
            formdata = payload,
            callback = self.unlock_contact_get,
            meta = meta,
            dont_filter=True
        )

    def unlock_contact_get(self, response):
        """
        Generating a get request to unlock the contact data
        :param data: response
        :return:
        """
        meta = response.meta
        url = meta['url']
        yield scrapy.Request(
                        url=url,
                        callback = self.parse_patient_details,
                        meta = meta,
                        dont_filter=True)

    def parse_patient_details(self, response):
        """
        Fetch patient details
        :param data: response
        :return:
        """
        doc = html.fromstring(response.body)
        docs = response.body
        meta = response.meta
        item = meta['item']
        id = re.findall(r'view_request/(.+)', response.url)
        item['md_id'] = id[0]
        item['m_type'] = 'patient'
        item['relationship'] = 'self'
        # Patient Info
        patient_info = doc.xpath('//div[@class="content clear-block"]/h3[text()="Patient Info"]')
        appointment_type = patient_info[0].xpath('./following-sibling::dl[1]/dd/text()')[0]
        patient_name = patient_info[0].xpath('./following-sibling::dl[1]/dd/text()')[1]

        item['appointment_type'] = appointment_type
        item['patient_name'] = patient_name

        # changes when start full work flow
        item["requested_on"] = '5/6/2016'

        # Patient Appointment Request
        patient_appointment_request = doc.xpath('//div[@class="content clear-block"]/h3[text()="Patient Appointment Request"]')
        location = patient_appointment_request[0].xpath('./following-sibling::dl[1]/dd/text()')[0]
        first_choice = patient_appointment_request[0].xpath('./following-sibling::dl[1]/dd/text()')[1]+','+patient_appointment_request[0].xpath('./following-sibling::dl[1]/dd/text()')[2]
        second_choice = patient_appointment_request[0].xpath('./following-sibling::dl[1]/dd/text()')[3]+','+patient_appointment_request[0].xpath('./following-sibling::dl[1]/dd/text()')[4]
        item['location'] =location
        item['first_choice'] =first_choice
        item['second_choice'] =second_choice

        # General Availability
        general_availability = self.parse_general_availability(doc, docs)
        item['general_availability'] = general_availability

        # Patient Notes
        patient_notes_obj = doc.xpath('//div[@class="content clear-block"]/h3[text()="Patient Notes"]')
        patient_note = self._remove_special_characters(''.join(patient_notes_obj[0].xpath('./following-sibling::p[1]/text()')))
        item['patient_note'] =patient_note[0]

        # Titration History
        titration_history_obj = doc.xpath('//div[@class="content clear-block"]/h3[text()="Titration History"]')
        titration_history = ''.join(titration_history_obj[0].xpath('./following-sibling::p[1]/text()'))
        item['titration_history'] =titration_history

        # Recent Medicine
        item['recent_medicine'] = ' '
        recent_medicine_obj = doc.xpath('//div[@class="patient-details"]/div/h3[text()="Recent Medicine"]')
        if recent_medicine_obj:
            recent_medicine = recent_medicine_obj[0].xpath('./following-sibling::text()')[0]
            item['recent_medicine'] =recent_medicine

        # Uploaded Files
        item['uploaded_files'] =' '
        uploaded_files_obj = doc.xpath('//div[@class="patient-details"]/div/h3[text()="Uploaded Files"]')
        if uploaded_files_obj:
            uploaded_files = uploaded_files_obj[0].xpath('./following-sibling::text()')[0]
            item['uploaded_files'] =uploaded_files

        # Patient Details
        patient_details = doc.xpath('//div[@class="patient-details"]/div/h3[text()="Patient Details"]')
        f_name = patient_details[0].xpath('./following-sibling::dl[1]/dd/text()')[0]
        l_name = patient_details[0].xpath('./following-sibling::dl[1]/dd/text()')[1]
        sex = patient_details[0].xpath('./following-sibling::dl[1]/dd/text()')[2]
        DOB = patient_details[0].xpath('./following-sibling::dl[1]/dd/text()')[3]
        phone = patient_details[0].xpath('./following-sibling::dl[1]/dd/text()')[4]
        email = patient_details[0].xpath('./following-sibling::dl[1]/dd/text()')[5]
        item['fname'] =f_name
        item['mname'] = "Nill"
        item['lname'] =l_name
        item['sex'] =sex
        item['DOB'] =DOB
        item['phone'] =phone
        item['email'] =email
        item['patient_title'] = 'Nill'
        item['md_source'] = 'MD'
        item['phone_ext'] = 'Nill'
        item['phone_type'] = 'Nill'



        # The Patient is a current NY Resident
        ny_resident = doc.xpath('//div[@class="patient-details"]/div/dl/dl/h4[text()="The Patient is a current NY Resident"]')
        if ny_resident:
            address1 = ny_resident[0].xpath('./following-sibling::p/text()')[0]
            item['address1'] = address1

        location = patient_details[0].xpath('./following-sibling::dl[1]/dd/text()')[6]
        if location:
            address_data = location.split(',')
            city = address_data[0]
            state = address_data[1].strip().split(' ')[0]
            zip = address_data[1].strip().split(' ')[1]
            prequalifies = patient_details[0].xpath('./following-sibling::dl[1]/dd/text()')[7]
            item['address_data'] =address_data
            item['city'] =city
            item['zip'] =zip
            item['state'] =state
            item['prequalifies'] =prequalifies

        # Prequalification Data
        prequalification_data = doc.xpath('//div[@class="patient-details"]/div/dl/dl/h3[text()="Prequalification Data"]')
        state_residence = prequalification_data[0].xpath('./following-sibling::p/text()')[0]
        current_NYresident = prequalification_data[0].xpath('./following-sibling::p/text()')[1]
        valid_identification = prequalification_data[0].xpath('./following-sibling::p/text()')[2]
        adult_18 = prequalification_data[0].xpath('./following-sibling::p/text()')[3]
        med_records = prequalification_data[0].xpath('./following-sibling::p/text()')[4]
        qualify_condition = prequalification_data[0].xpath('./following-sibling::p/text()')[5]
        item['state_residence'] =state_residence
        item['current_nyresident'] =current_NYresident
        item['valid_identification'] =valid_identification
        item['adult_18'] =adult_18
        item['med_records'] =med_records
        item['qualify_condition'] =qualify_condition
        item['med_type'] = 'nill'
        item['med_share'] = 'nill'

        # When did this health issue begin:
        health_issue_begin = doc.xpath('//div[@class="patient-details"]/div/dl/dl/h4[text()="When did this health issue begin:"]')
        onset_month = health_issue_begin[0].xpath('./following-sibling::p/text()')[0]
        onset_yr = health_issue_begin[0].xpath('./following-sibling::p/text()')[1]
        current_treat = health_issue_begin[0].xpath('./following-sibling::p/text()')[2]
        treat_last_visit = health_issue_begin[0].xpath('./following-sibling::p/text()')[3]
        item['onset_month'] =onset_month
        item['onset_yr'] =onset_yr
        item['current_treat'] =current_treat
        item['treat_last_visit'] =treat_last_visit


        # What is your primary care physicians contact information:
        physicians_contact_information = doc.xpath('//div[@class="patient-details"]/div/dl/dl/h4[text()="What is your primary care physicians contact information:"]')
        if physicians_contact_information:
            fname = physicians_contact_information[0].xpath('./following-sibling::p/text()')[0]
            lname = physicians_contact_information[0].xpath('./following-sibling::p/text()')[1]
            phone_work = physicians_contact_information[0].xpath('./following-sibling::p/text()')[2]
            # phone_home = physicians_contact_information[0].xpath('./following-sibling::p/text()')[3]
            # phone_mob = physicians_contact_information[0].xpath('./following-sibling::p/text()')[4]
            fax_work = physicians_contact_information[0].xpath('./following-sibling::p/text()')[3]
            prior_rec = physicians_contact_information[0].xpath('./following-sibling::p/text()')[4]
            item['phy_fname'] =fname
            item['phy_lname'] =lname
            item['phy_phone_work'] =phone_work
            item['phy_fax_work'] =fax_work
            item['prior_rec'] = prior_rec
            item['phy_type'] = 'Not specified'
            item['phy_specialty'] = 'Not specified'
        else:
            item['phy_fname'] ='Not specified'
            item['phy_lname'] ='Not specified'
            item['phy_phone_work'] ='Not specified'
            item['phy_fax_work'] ='Not specified'
            item['prior_rec'] = 'Not specified'
            item['phy_type'] = 'Not specified'
            item['phy_specialty'] = 'Not specified'


        # If yes, please tell us the state, approximate date and medical condition:
        medical_condition = doc.xpath('//div[@class="patient-details"]/div/dl/dl/h4[text()="If yes, please tell us the state, approximate date and medical condition:"]')
        rec_state = medical_condition[0].xpath('./following-sibling::p/text()')[0]
        rec_date = medical_condition[0].xpath('./following-sibling::p/text()')[1]
        current_parole_probation = medical_condition[0].xpath('./following-sibling::p/text()')[2]
        drug_arrest = medical_condition[0].xpath('./following-sibling::p/text()')[3]
        ins_coverage = medical_condition[0].xpath('./following-sibling::p/text()')[4]
        item['rec_state'] =rec_state
        item['rec_date'] =rec_date
        item['current_parole_probation'] =current_parole_probation
        item['drug_arrest'] =drug_arrest
        item['ins_coverage'] =ins_coverage

        # Do you currently have insurance?
        insurance = doc.xpath('//div[@class="patient-details"]/div/dl/dl/h4[text()="Do you currently have insurance?"]')
        if insurance:
            ins_name = insurance[0].xpath('./following-sibling::p/text()')[0]
            prescribed = insurance[0].xpath('./following-sibling::p/text()')[1]
            item['ins_name'] =ins_name
            item['prescribed'] =prescribed
        else:
            item['ins_name'] ="Not specified"
            item['prescribed'] ="Not prescribed"

        # Are you currently taking any prescriptions?
        taking_any_prescriptions = doc.xpath('//div[@class="patient-details"]/div/dl/dl/h4[text()="Are you currently taking any prescriptions?"]')
        if taking_any_prescriptions:
            prescription_meds = taking_any_prescriptions[0].xpath('./following-sibling::p/text()')[0]
            if prescription_meds:
                item['prescription_meds'] = prescription_meds
            else:
                item['prescription_meds'] = "Not given"

        # Do you have a secondary phone number where you can be reached?
        second_phone = doc.xpath('//div[@class="patient-details"]/div/dl/dl/h4[text()="Do you have a secondary phone number where you can be reached?"]')
        if second_phone:
            phone2 = second_phone[0].xpath('./following-sibling::p/text()')[1]
            item['phone2'] = phone2
        # fieldnames = [item['drug_arrest'], item['DOB']]

        # Others
        # communication
        item['pref_comm'] = 'nill'
        item['comm_type'] =  'nill'
        item['email_comm'] = 'yes'
        item['phone_comm'] = 'yes'
        item['text_comm'] = 'nill'
        item['communication_agree'] = 'nill'
        # history
        item['his_name'] = 'nill'
        item['his_category'] = 'nill'
        item['text_address'] = 'nill'
        item['address_type'] = 'nill'

        yield item

    def _remove_special_characters(self, data):
        """
        Removes special characters from the inout data
        :param data: String
        :return: String
        """
        un_wanted_characters = ['\n','\r' ]
        for special_character in un_wanted_characters:
            data = re.sub(special_character, '', data)
        list_data = [item.strip() for item in data.split('*') if item.strip()]
        return list_data

    def parse_general_availability(self, doc, docs):
        """
        Fetch general availability details of each day
        :param data: doc , docs
        :return: dict
        """
        general_avail = {}
        general_availability = doc.xpath('//div[@class="content clear-block"]/h3[text()="General Availability"]')
        if general_availability[0].text_content() == "General Availability":

            monday = re.findall(r'Monday</dt><dd>(.+?)</dd', docs, re.DOTALL)
            monday_time = self.parse_available_time(monday, day='monday')
            general_avail.update(monday_time)

            tuesday = re.findall(r'Tuesday</dt><dd>(.+?)</dd', docs, re.DOTALL)
            tuesday_time = self.parse_available_time(tuesday, day='tuesday')
            general_avail.update(tuesday_time)

            wednesday = re.findall(r'Wednesday</dt><dd>(.+?)</dd', docs, re.DOTALL)
            wednesday_time = self.parse_available_time(wednesday, day='wednesday')
            general_avail.update(wednesday_time)

            thursday = re.findall(r'Thursday</dt><dd>(.+?)</dd', docs, re.DOTALL)
            thursday_time = self.parse_available_time(thursday, day='thursday')
            general_avail.update(thursday_time)

            friday = re.findall(r'Friday</dt><dd>(.+?)</dd', docs, re.DOTALL)
            friday_time = self.parse_available_time(monday, day='friday')
            general_avail.update(friday_time)

            saturday = re.findall(r'Saturday</dt><dd>(.+?)</dd', docs, re.DOTALL)
            saturday_time = self.parse_available_time(saturday, day='saturday')
            general_avail.update(saturday_time)

            sunday = re.findall(r'Sunday</dt><dd>(.+?)</dd', docs, re.DOTALL)
            sunday_time = self.parse_available_time(sunday, day='sunday')
            general_avail.update(sunday_time)

        return general_avail

    def parse_available_time(self, today, day):
        """
        Fetch general availability details of each time in a day
        :param data: today ,day
        :return: dict
        """
        gen_avail = {}
        if "Morning" in ''.join(today):
            print 'Inside morning'
            morning_data = re.findall(r'Morning \((.+?)\)', today[0])[0]
            morning_data = morning_data.split('-')
            if day in gen_avail:
                gen_avail[day].update   ( {day+'Morning Start Time':morning_data[0],
                                        day+'Morning End Time':morning_data[1]})
            else:
                gen_avail[day] = {day+'Morning Start Time':morning_data[0],
                              day+'Morning End Time':morning_data[1]}

        if "Mid-Day" in ''.join(today):
            print 'Inside Mid-Day'
            mid_day_data = re.findall(r'Mid-Day \((.+?)\)', today[0])[0]
            mid_day_data = mid_day_data.split('-')
            if day in gen_avail:
                gen_avail[day].update({day+'Mid-Day Start Time':mid_day_data[0],
                              day+'Mid-Day End Time':mid_day_data[1]})
            else:
                gen_avail[day] = {day+'Mid-Day Start Time':mid_day_data[0],
                              day+'Mid-Day End Time':mid_day_data[1]}

        if "Afternoon" in ''.join(today):
            print 'Inside Afternoon'
            afternoon_data = re.findall(r'Afternoon \((.+?)\)', today[0])[0]
            afternoon_data = afternoon_data.split('-')
            if day in gen_avail:
               gen_avail[day].update({day+'Afternoon Start Time':afternoon_data[0],
                              day+'Afternoon End Time':afternoon_data[1]})
            else:
                gen_avail[day] = {day+'Afternoon Start Time':afternoon_data[0],
                              day+'Afternoon End Time':afternoon_data[1]}

        if "Evening" in ''.join(today):
            print 'Inside Evening'
            evening_data = re.findall(r'Evening \((.+?)\)', today[0])[0]
            evening_data = evening_data.split('-')
            if day in gen_avail:
                gen_avail[day].update({day+'Evening Start Time':evening_data[0],
                              day+'Evening End Time':evening_data[1]})
            else:
                gen_avail[day] = {day+'Evening Start Time':evening_data[0],
                              day+'Evening End Time':evening_data[1]}

        return gen_avail