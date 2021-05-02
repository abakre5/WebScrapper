import time
import pickle
import logging as log
from datetime import date

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

import CollectorUtil

if __name__ == "__main__":
    log.basicConfig(filename='../scratch/logs/CookieGenerator_' + str(date.today()) + '.log',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=log.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
    try:
        options = Options()
        options.headless = not True
        options.add_argument("start-maximized")

        urlpage = 'https://99designs.com/login'
        log.info("Logging at {}".format(urlpage))

        browser = webdriver.Firefox(options=options)
        CollectorUtil.fetchURL(browser, urlpage, log)
        time.sleep(2)

        email = browser.find_element_by_id("email")
        password = browser.find_element_by_id("password")

        username = ""
        password = ""
        email.send_keys(username)
        password.send_keys(password)
        log.info("Waiting for 90 secs for user to complete captcha.")
        time.sleep(90)
        log.info("Done waiting for captcha.")

        pickle.dump(browser.get_cookies(), open("../Cookie/Cookie.pkl", "wb"))
        log.info("Cookie generation complete.")
        browser.close()
    except Exception as e:
        log.error("Error occurred while cookie generation => {}".format(str(e)))
