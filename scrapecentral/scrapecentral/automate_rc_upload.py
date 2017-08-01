import time
import datetime
import ConfigParser

from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class AutomateRC(object):

    config = ConfigParser.ConfigParser()
    section = config.read("/home/sayone/project/ScrapeCenrtalNew/scrapecentral/scrapecentral/config.ini")

    login = config.get('REMINDER','username')
    password = config.get('REMINDER','password')

    def __init__(self):
        self.upload_file()

    def upload_file(self):
        """
        Login, and upload CSV file
        :param data:
        :return:
        """
        driver = webdriver.Chrome(executable_path=r'/home/sayone/Downloads/chromedriver')
        url = 'https://secure.remindercall.com/login'
        driver.get(url)
        time.sleep(2)
        log = driver.find_element_by_id('pageUsername')
        log.clear()
        log.send_keys(self.login)

        passw = driver.find_element_by_id('pagePassword')
        passw.clear()
        passw.send_keys(self.password, Keys.ENTER)
        time.sleep(3)

        driver.find_element_by_partial_link_text("Make Calls").click();
        time.sleep(3)

        if 'Michael Morgenstern' in driver.page_source:
            #write extra automation here
            driver.find_element_by_partial_link_text("Upload a spreadsheet").click();
            time.sleep(2)
            driver.find_element_by_partial_link_text("1: Upload").click();
            time.sleep(2)
            driver.find_element_by_css_selector('input[type="file"]').clear()
            driver.find_element_by_css_selector('input[type="file"]').send_keys(r"/home/sayone/Downloads/UploadRC1.xlsx.xlsx")
            time.sleep(2)
            driver.find_element_by_xpath("//input[@type='submit' and @value='Send File']").click()
            time.sleep(2)
            driver.find_element_by_xpath(".//button[text()='Set Columns and Continue']").click()
            time.sleep(2)
            driver.find_element_by_xpath(".//button[text()='Continue and Skip Rules']").click()
            time.sleep(2)

            #get time
            now = datetime.datetime.now()
            day_time = str(now.strftime("%Y-%m-%d"))+' 18:00'

            day_obj = driver.find_element_by_xpath('.//input[@type="text" and @name="delivery"]')
            day_obj.clear()
            day_obj.send_keys(day_time)

            time.sleep(2)
            driver.find_element_by_xpath(".//button[text()='Review Settings']").click()
            time.sleep(2)
            driver.quit();

            print 'We are not queueing the calls now..........hi..hi..'
            driver.quit();
            # driver.find_element_by_xpath(".//button[text()='Queue Calls Now']").click()


if __name__ == "__main__":
    d = AutomateRC()
