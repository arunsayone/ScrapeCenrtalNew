import scrapy


class ScrapecentralItem(scrapy.Item):
    md_id = scrapy.Field()
    relationship = scrapy.Field()
    requested_on = scrapy.Field()
    location = scrapy.Field()
    patient_name = scrapy.Field()
    patient_title = scrapy.Field()
    m_type = scrapy.Field()
    dates_requested = scrapy.Field()
    first_choice = scrapy.Field()
    second_choice = scrapy.Field()
    general_availability = scrapy.Field()
    patient_note = scrapy.Field()
    titration_history = scrapy.Field()
    dates_requested = scrapy.Field()
    recent_medicine = scrapy.Field()
    uploaded_files = scrapy.Field()
    type = scrapy.Field()

    fname = scrapy.Field()
    mname = scrapy.Field()
    lname = scrapy.Field()
    sex = scrapy.Field()
    DOB = scrapy.Field()
    phone = scrapy.Field()
    email = scrapy.Field()
    city = scrapy.Field()
    zip = scrapy.Field()
    state = scrapy.Field()
    prequalifies = scrapy.Field()
    state_residence = scrapy.Field()
    address_data = scrapy.Field()
    md_source = scrapy.Field()

    phone2 = scrapy.Field()
    prescription_meds = scrapy.Field()
    prescribed = scrapy.Field()
    ins_name = scrapy.Field()
    ins_coverage = scrapy.Field()
    drug_arrest = scrapy.Field()
    current_parole_probation = scrapy.Field()
    rec_date = scrapy.Field()
    rec_state = scrapy.Field()
    prior_rec = scrapy.Field()
    
    phy_fax_work = scrapy.Field()
    phy_phone_work = scrapy.Field()
    phy_lname = scrapy.Field()
    phy_fname = scrapy.Field()
    phy_type = scrapy.Field()
    phy_specialty = scrapy.Field()

    treat_last_visit = scrapy.Field()
    current_treat = scrapy.Field()
    onset_yr = scrapy.Field()
    onset_month = scrapy.Field()
    qualify_condition = scrapy.Field()

    med_records = scrapy.Field()
    adult_18 = scrapy.Field()
    valid_identification = scrapy.Field()
    current_nyresident = scrapy.Field()
    general_availability = scrapy.Field()
    address1 = scrapy.Field()
    appointment_type = scrapy.Field()
    med_type = scrapy.Field()
    med_share = scrapy.Field()

    # Others
    pref_comm = scrapy.Field()
    comm_type = scrapy.Field()
    email_comm = scrapy.Field()
    phone_comm = scrapy.Field()
    text_comm = scrapy.Field()
    communication_agree = scrapy.Field()
    phone_ext = scrapy.Field()
    phone_type = scrapy.Field()
    his_category = scrapy.Field()
    his_name = scrapy.Field()
    text_address = scrapy.Field()
    address_type = scrapy.Field()
    
    # phone_ext = scrapy.Field()


    #RC
    rc_type = scrapy.Field()
    rc_recipient = scrapy.Field()
    rc_name = scrapy.Field()
    rc_group = scrapy.Field()
    rc_appt = scrapy.Field()
    rc_delivery = scrapy.Field()
    rc_duration = scrapy.Field()
    rc_tries_status = scrapy.Field()
    rc_reply = scrapy.Field()
    rc_details = scrapy.Field()
    rc_date = scrapy.Field()


    #PF
    # scrape
    pf_date = scrapy.Field()
    pf_time = scrapy.Field()
    pf_status = scrapy.Field()
    pf_facility = scrapy.Field()
    pf_source = scrapy.Field()
    pf_appointment_type = scrapy.Field()
    pf_patient_name = scrapy.Field()
    pf_patient_dob  = scrapy.Field()
    patient_data = scrapy.Field()

    #download
    pf_pt_id = scrapy.Field()
    pf_pt_street_add1 = scrapy.Field()
    pf_pt_mob = scrapy.Field()
    pf_pt_firstName = scrapy.Field()
    pf_pt_city = scrapy.Field()
    pf_pt_lastName = scrapy.Field()
    pf_pt_age = scrapy.Field()
    pf_pt_dob = scrapy.Field()
    pf_pt_state = scrapy.Field()
    pf_pt_email = scrapy.Field()
    pf_pt_gender = scrapy.Field()
    pf_pt_zip = scrapy.Field()





    

    
