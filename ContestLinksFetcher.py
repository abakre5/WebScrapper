import csv
import os
import pickle

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from datetime import date
import logging as log

import CollectorUtil

MAX_NEW_CONTEST_EACH_DAY_FOR_TYPE = 30


def login():
    try:
        urlLogin = "https://99designs.com/logo-design/contests"
        CollectorUtil.fetchURL(browser, urlLogin, log)
        cookieFile = open("../Cookie/Cookie.pkl", "rb")
        for cookie in pickle.load(cookieFile):
            browser.add_cookie(cookie)
        cookieFile.close()
        login_button = browser.find_element_by_xpath(".//a[contains(@href,'/login')]")
        login_button.click()
        log.info("Logged into 99desings.")
    except Exception as e:
        log.error("Error occurred while login => {}".format(str(e)))


def WriteToCSV(outputCSV, data):
    log.info("Task started to write links into CSV")
    try:
        newPath = os.path.join("../Data/", "ContestLinks")
        if not os.path.exists(newPath):
            os.mkdir(newPath)
        with open(os.path.join(newPath, outputCSV + str(date.today()) + ".csv"), 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["URL"])
            for url in data:
                writer.writerow([url])
    except IOError as err:
        log.error("Error occurred while writing to CSV => {}".format(err))
    log.info("Task completed to write links into CSV")
    return


def parseUrlForContestLink(browser, urlpage, urls, typeOfContest):
    try:
        CollectorUtil.fetchURL(browser, urlpage, log)
        log.info("Task started to get competition links ...")
        # Scrap first 18 Pages in pagination index
        newContestCount = 0
        for i in range(1, 18):
            elements = browser.find_elements_by_xpath(".//a[contains(@class,'listing-details__title__link')]")
            for el in elements:
                if newContestCount >= MAX_NEW_CONTEST_EACH_DAY_FOR_TYPE and typeOfContest == "OPEN":
                    break
                current_link = el.get_attribute("href")
                if "/brief" in current_link:
                    current_link = current_link[:-6]
                if current_link is not urls:
                    urls.add(current_link)
                    newContestCount += 1

            if newContestCount >= MAX_NEW_CONTEST_EACH_DAY_FOR_TYPE and typeOfContest == "OPEN":
                break

            CollectorUtil.fetchURL(browser, urlpage + '&page=' + str(i), log)
        log.info("No of new contest added: {}".format(str(newContestCount)))
        log.info("Getting competition link task successful")
    except Exception as e:
        log.error("Error occurred while fetching links => {}".format(str(e)))
    return urls


def getDBContestURLs(typeOfContest):
    cursor = databaseConnection.cursor()
    cursor.execute("SELECT URL FROM contestdata WHERE STATUS != 'Finished' AND TYPE = '{}'".format(typeOfContest))
    urls = cursor.fetchall()
    cursor.close()
    return set(t[0] for t in urls)


if __name__ == "__main__":
    log.basicConfig(filename='../scratch/logs/ContestLinkFetcher_' + str(date.today()) + '.log',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=log.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
    options = Options()
    options.headless = True
    options.add_argument("start-maximized")
    urlOpenContestPage = 'https://99designs.com/logo-design/contests?sort=start-date%3Adesc&status=open&entry-level=0&mid' \
                         '-level=0&top-level=0&favorite=0&worked-together=0&blind=none&dir=desc&order=start-date '
    urlBlindContestPage = 'https://99designs.com/logo-design/contests?sort=start-date%3Adesc&status=open&entry-level=0' \
                          '&mid-level=0&top-level=0&favorite=0&worked-together=0&blind=only&dir=desc&order=start-date '

    outputCSVOpenContest = 'Open_Contest_'
    outputCSVBlindContest = 'Blind_Contest_'

    browser = webdriver.Firefox(options=options)
    login()

    # Fetch Open Contest

    databaseConnection = CollectorUtil.getDBConnection()
    urls = set(getDBContestURLs("OPEN"))
    # urls = parseUrlForContestLink(browser, urlOpenContestPage, urls, "OPEN")
    WriteToCSV(outputCSVOpenContest, urls)

    # Fetch Blind Contest
    urls = set(getDBContestURLs("BLIND"))
    # urls = parseUrlForContestLink(browser, urlBlindContestPage, urls, "BLIND")
    WriteToCSV(outputCSVBlindContest, urls)

    browser.close()
    databaseConnection.close()
