# ['scraping_date', 'tags', 'title', 'ContestId', 'owner', 'owner-url', 'prize-money', 'award-type', 'entry-level',
# 'additional-winner', 'startDate', 'purchasePrice', 'Watchers', 'Name to incorporate in the logo', 'Slogan to
# incorporate in the logo', 'Description of the organization and its target audience', 'Colors to explore',
# 'Other color requirements', 'Design inspiration', 'Other notes', 'contest-info', 'Classic-Modern',
# 'Mature-Youthful', 'Feminine-Masculine', 'Playful-Sophisticated', 'Economical-Luxurious', 'Geometric-Organic',
# 'Abstract-Literal', 'Contest awards', 'Contests in progress', 'Projects', 'member_since', 'feedback_timestamp',
# 'feedback_info']
import json

import pytz
from dateutil import parser
import os
import pickle
from datetime import datetime
import logging as log
import pandas as pd
import re
import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

import traceback

from datetime import date

import CollectorUtil

outputCSVOpenContest = "../Data/ContestLinks/Open_Contest_" + str(date.today()) + ".csv"
outputCSVBlindContest = "../Data/ContestLinks/Blind_Contest_" + str(date.today()) + ".csv"


def writeDataToDB(dict_data):
    try:
        cursor = databaseConnection.cursor()
        placeholder = ", ".join(["%s"] * len(dict_data))
        columnsD = "`,`".join(dict_data.keys())
        columnsD = '`' + columnsD + '`'
        stmt = "insert into `{table}` ({columns}) values ({values});".format(table='ContestInformation',
                                                                             columns=columnsD,
                                                                             values=placeholder)
        cursor.execute(stmt, list(dict_data.values()))
        cursor.close()
    except Exception as err:
        print("Error occurred while pushing contest data for ID {} => {}".format(str(dict_data['ContestId']), str(err)))
        traceback.print_stack()
        raise Exception("Error occurred while pushing contest data for ID {} => ".format(str(dict_data['ContestId'])))
    try:
        if dict_data['guaranteed'] == "1":
            cursor = databaseConnection.cursor()
            cursor.execute("INSERT IGNORE INTO GUARANTEED (CONTESTID) VALUES (" + str(dict_data['ContestId'] + ");"))
            cursor.close()
    except Exception as err:
        print(
            "Error occurred while adding contest id " + str(dict_data['ContestId']) + " as guaranteed. Error => " + str(
                err))
        traceback.print_stack()
        raise Exception(
            "Error occurred while adding contest id {} as guaranteed.".format(str(dict_data['ContestId'])))
    try:
        if dict_data['fast-tracked'] == "1":
            cursor = databaseConnection.cursor()
            cursor.execute("INSERT IGNORE INTO FASTTRACKED VALUES (" + str(dict_data['ContestId'] + ");"))
            cursor.close()
    except Exception as err:
        print("Error occurred while adding contest id " + str(
            dict_data['ContestId']) + " as fast-tracked. Error => " + str(err))
        traceback.print_stack()
        raise Exception(
            "Error occurred while adding contest id {} as fast-tracked.".format(str(dict_data['ContestId'])))


def addContestTracking(contestID, contestStatus, type, url):
    try:
        SELECTQUERY = "SELECT CONTESTID FROM CONTESTDATA WHERE CONTESTID = " + str(contestID)
        cursor = databaseConnection.cursor()
        cursor.execute(SELECTQUERY)
        result = cursor.fetchone()
    except Exception as err:
        print("Error occurred while fetching contest data for ID {} => {}".format(str(contestID), str(err)))
        traceback.print_stack()
        raise Exception("Error occurred while fetching contest data for ID {} => ".format(str(contestID)))
    if result is None:
        try:
            INSERTQUERY = "INSERT INTO CONTESTDATA (CONTESTID, TYPE, STATUS, URL) VALUES (%s,%s,%s,%s)"
            val = (int(contestID), type, str(contestStatus), str(url))
            cursor.execute(INSERTQUERY, val)
            log.info("Inserted new contest {}".format(str(contestID)))
        except Exception as err:
            print("Error occurred while inserting new contest data for ID {} => {}".format(str(contestID), str(err)))
            traceback.print_stack()
            raise Exception("Error occurred while inserting new contest data for ID {} => ".format(str(contestID)))
    else:
        try:
            UPDATEQUERY = "UPDATE CONTESTDATA SET STATUS = '" + str(contestStatus) + "' WHERE CONTESTID = " + str(
                contestID) + ""
            cursor.execute(UPDATEQUERY)
            log.info("Updated the existing contest {}".format(str(contestID)))
        except Exception as err:
            print("Error occurred while updating contest data for ID {} => {}".format(str(contestID), str(err)))
            traceback.print_stack()
            raise Exception("Error occurred while updating contest data for ID {} => ".format(str(contestID)))
    try:
        SELECTQUERY = "SELECT CONTESTID FROM CONTESTSTATUS WHERE CONTESTID = {} AND STATUS = '{}'".format(
            str(contestID), contestStatus)
        cursor = databaseConnection.cursor()
        cursor.execute(SELECTQUERY)
        result = cursor.fetchone()
    except Exception as err:
        print("Error occurred while fetching contest status from conteststatus table for ID {} => {}".format(
            str(contestID), str(err)))
        traceback.print_stack()
        raise Exception("Error occurred while fetching contest status from conteststatus table for ID {} => ".format(
            str(contestID)))
    if result is None:
        try:
            INSERTQUERY = "INSERT IGNORE INTO CONTESTSTATUS (CONTESTID, STATUS) VALUES ({}, '{}')".format(
                str(contestID), str(contestStatus))
            cursor.execute(INSERTQUERY)
        except Exception as err:
            print("Error occurred while updating contest status for ID {} => {} in conteststatus table".format(
                str(contestID), str(err)))
            traceback.print_stack()
            raise Exception(
                "Error occurred while updating contest data for ID {}  in conteststatus table".format(str(contestID)))
    cursor.close()


def getIntegerMonth(month):
    return {
        'january': "01",
        'february': "02",
        'march': "03",
        'april': "04",
        'may': "05",
        'june': "06",
        'july': "07",
        'august': "08",
        'september': "09",
        'october': "10",
        'november': "11",
        'december': "12"
    }[month]


def addCollectedImagesToDB(databaseConnection, images, tableName):
    if images:
        cursor = databaseConnection.cursor()
        cursor.execute("INSERT IGNORE INTO {} (CONTESTID, PATH) VALUES {}".format(tableName,
                                                                                  CollectorUtil.getCollectorArgList(
                                                                                      images)))
        cursor.close()


def insertFeedbackInfoInDB(databaseConnection, contestID, feedbackMsgs, feedbackTimeStamps):
    rows = []
    lenToRun = min(len(feedbackMsgs), len(feedbackTimeStamps))
    i = 0
    while i < lenToRun:
        rows.append((contestID, feedbackMsgs[i], feedbackTimeStamps[i]))
        i += 1
    cursor = databaseConnection.cursor()
    cursor.execute("INSERT IGNORE INTO feedbackinfo VALUES {}".format(CollectorUtil.getCollectorArgList(rows)))
    cursor.close()


def parseContestForInformation(browser, contests, type, noOfRows):
    try:
        parentFolderPath = "../Data/ContestDesignImages"
        if not os.path.exists(parentFolderPath):
            os.mkdir(parentFolderPath)
    except Exception as err:
        print("Error occurred creating Design Images folder {} => {}".format(parentFolderPath, str(err)))
        traceback.print_stack()
        raise Exception("Error occurred while creating ContestDesignImages DIR => {}".format(str(err)))

    urlNo = 1
    try:
        for urlpage in contests:
            rawURL = urlpage
            urlpage = urlpage + '/brief'
            CollectorUtil.fetchURL(browser, urlpage, log)
            print(str(urlNo) + "/" + noOfRows)
            log.info("Hitting URL: {}".format(urlpage))
            cookieFile = open("../Cookie/Cookie.pkl", "rb")
            for cookie in pickle.load(cookieFile):
                browser.add_cookie(cookie)
            cookieFile.close()
            CollectorUtil.fetchURL(browser, urlpage, log)

            contestID = urlpage[urlpage.rfind('-') + 1:urlpage.rfind('/')]
            try:
                contestStatus = browser.find_element_by_xpath(
                    ".//div[contains(@class, 'contest-status-pill')]").get_attribute(
                    "innerText")
            except Exception as err:
                log.warning("ContestID {} not parsed : {}.".format(str(contestID), err))
                urlNo += 1
                continue

            # Check if Contest is finished or not
            if contestStatus.lower() != 'finished':
                log.info("Skipping contest: {} as it is not finished.".format(str(contestID)))
                addContestTracking(contestID, contestStatus, type, rawURL)
                databaseConnection.commit()
                urlNo += 1
                continue

            log.info("Collecting information for contest: {} as it is finished.".format(str(contestID)))
            data = {}
            # data['scraping_date'] = str(datetime.utcnow())
            data['fast-tracked'] = '0'
            data['guaranteed'] = '0'
            data['title'] = browser.find_element_by_xpath(
                ".//h1[contains(@class,'contest-header__title__heading')]").get_attribute("innerText")  # for title
            data['title'] = re.sub(r"[-_()\"#/@;\\:<>{}`+=~|.!?,*]", "",
                                   data['title'].strip())  # removing extra spaces & chars
            data['title'] = data['title'].strip()
            data['ContestId'] = urlpage[urlpage.rfind('-') + 1:urlpage.rfind('/')]
            try:
                data['owner'] = browser.find_element_by_xpath(".//a[contains(@class,'display-name')]").get_attribute(
                    "innerText")
                data['owner-url'] = browser.find_element_by_xpath(
                    ".//a[contains(@class,'display-name')]").get_attribute(
                    "href")
            except Exception as er:
                data['owner'] = browser.find_element_by_xpath(".//span[contains(@class,'display-name')]").get_attribute(
                    "innerText")
                data['owner-url'] = browser.find_element_by_xpath(
                    ".//span[contains(@class,'display-name')]").get_attribute(
                    "href")

            spans = browser.find_element_by_xpath(
                ".//div[contains(@class,'contest-price__primary')]").find_elements_by_xpath(
                ".//span")
            data['prize-money'] = re.sub(r'[\W_]+', '', spans[0].get_attribute("innerText"))
            data['award-type'] = spans[1].find_element_by_xpath(".//span").get_attribute("innerText")
            try:
                # extracting level,additional winner rules, start date, purchase price
                element = browser.find_element_by_xpath('//div[@id = "header-price-data"]')
                summary = element.get_attribute("data-translations")
                res = json.loads(str(summary))['messages']
                data["entry-level"] = res["price_tooltip_entry_level"]
                if "price_tooltip_additional_winner" in res:
                    data["additional-winner"] = res["price_tooltip_additional_winner"]
                else:
                    data["additional-winner"] = None

                summary = element.get_attribute("data-initial-props")
                res = json.loads(str(summary))
                data["startDate"] = str(parser.parse(res["startDate"]))[:-6]
                if "purchasePrice" in res:
                    data["purchasePrice"] = re.sub(r'[a-zA-Z$]+', '', res["purchasePrice"])
                else:
                    data["purchasePrice"] = None

                # print(res.keys())
            except Exception:
                pass

            data['status'] = browser.find_element_by_xpath(
                ".//div[contains(@class, 'contest-status-pill')]").get_attribute(
                "innerText")
            try:
                newtag = str(
                    browser.find_element_by_xpath(
                        ".//span[contains(@class,'meta-item__label hidden--small')]").get_attribute(
                        "innerText"))
                if newtag == 'Fast-tracked':
                    data['fast-tracked'] = '1'
                if newtag == 'Guaranteed':
                    data['guaranteed'] = '1'
                data['Watchers'] = re.sub(r"[()]", "", str(
                    browser.find_element_by_xpath(".//span[contains(@class,'meta-item__count')]").get_attribute(
                        "innerText")))
            except Exception:
                pass

            elements1 = browser.find_elements_by_xpath(".//h3[contains(@class,'brief-element__title')]")
            elements2 = browser.find_elements_by_xpath(".//div[contains(@class,'brief-element__value')]")
            for el in range(len(elements1)):
                data[elements1[el].get_attribute("innerText").strip()] = elements2[el].get_attribute(
                    "innerText").strip()

            data['Industry'] = str(data["Industry"])
            if "Attachments" in data:
                del data["Attachments"]
            if "Colors to explore" in data:
                data["Colors to explore"] = data["Colors to explore"].replace('\n', ',')

            if "Style Attributes" in data:
                data["Style Attributes"] = data["Style Attributes"].replace('\n', ',')

            li_elements = browser.find_elements_by_xpath(
                ".//li[contains(@class,'contest-holder__tooltip__list__item')]")
            try:
                data['contest-info'] = str("Last feedback :" + li_elements[0].find_element_by_xpath(
                    ".//span[contains(@class,'gmttime abbr descr')]").get_attribute("title"))
            except:
                data['contest-info'] = str("Last feedback : NA")

            for i in range(1, len(li_elements)):
                data['contest-info'] += ", " + " ".join(li_elements[i].get_attribute("innerText").strip().split())

            try:
                elements1 = browser.find_elements_by_xpath(".//div[contains(@class,'slider__handle slider__handle-0')]")
                features = data["Style Attributes"].split(',')
                for i in range(len(elements1)):
                    data[features[2 * i] + "-" + features[2 * i + 1]] = elements1[i].get_attribute("aria-valuenow")
                del data["Style Attributes"]

            except Exception:
                pass

            # creating necessary folders per contest

            newpath = os.path.join(parentFolderPath, data["ContestId"])
            if not os.path.exists(newpath):
                os.mkdir(newpath)
            newpath2 = os.path.join(parentFolderPath, data["ContestId"], "inspiration")
            if not os.path.exists(newpath2):
                os.mkdir(newpath2)

            # saving inspiration design counts and corresponding images
            inspiration_count = 0
            try:
                inspirationImages = []
                referenceImages = []
                # saving two images sets
                elements = browser.find_elements_by_xpath(
                    '//div[@class = "matrix matrix--of-two-small matrix--of-three-medium matrix--of-three-normal matrix--of-three-large matrix--separated"]')
                for it in range(len(elements)):
                    if it == 0:
                        elements1 = elements[it].find_elements_by_xpath(
                            ".//img[contains(@class,'fixed-ratio-image__inner__image')]")
                        inspiration_count = len(elements1)
                        if "Design inspiration" in data:
                            data["Design inspiration"] = str(inspiration_count)
                        for i in range(len(elements1)):
                            response = requests.get(elements1[i].get_attribute("src"), stream=True)
                            imagePath = os.path.join(newpath2, str(i) + '.png')
                            with open(imagePath, 'wb') as outfile:
                                for chunk in response.iter_content(1024):
                                    outfile.write(chunk)
                                    # shutil.copyfileobj(response.raw, outfile)
                            inspirationImages.append((str(contestID), os.path.relpath(imagePath)))
                            del response
                    else:
                        newpath2 = os.path.join(parentFolderPath, data["ContestId"], "references")
                        if not os.path.exists(newpath2):
                            os.mkdir(newpath2)
                        elements1 = elements[it].find_elements_by_xpath(
                            ".//img[contains(@class,'fixed-ratio-image__inner__image')]")
                        for i in range(len(elements1)):
                            response = requests.get(elements1[i].get_attribute("src"), stream=True)
                            imagePath = os.path.join(newpath2, str(i) + '.png')
                            with open(os.path.join(imagePath), 'wb') as outfile:
                                for chunk in response.iter_content(1024):
                                    outfile.write(chunk)
                                    # shutil.copyfileobj(response.raw, outfile)
                            referenceImages.append((str(contestID), os.path.relpath(imagePath)))
                            del response

                addCollectedImagesToDB(databaseConnection, inspirationImages, "inspirationimages")
                addCollectedImagesToDB(databaseConnection, referenceImages, "referenceimages")

            except Exception as err:
                log.error("Error occurred while fetching images for Contest {} => ".format(str(contestID), str(err)))
                pass
            CollectorUtil.fetchURL(browser, data['owner-url'], log)
            try:
                # elements1 = browser.find_elements_by_xpath(".//div[@class='stats-panel__item__title']")
                # elements2 = browser.find_elements_by_xpath(".//div[@class='stats-panel__item__value']")
                # for i in range(len(elements1)):
                #     key = elements1[i].get_attribute("innerText").strip()  # rating attribute header
                #     value = elements2[i].get_attribute("innerText").strip() # ratiing attribute value
                #     data[key]=value

                # #member_since
                # data["member_since"] = browser.find_element_by_xpath(".//span[@class='subtle-text']").get_attribute("innerText").strip()
                data['contest awards'], data['contests in progress'], data['success rate'], data['projects'], data[
                    'member_since'], data['country'] = None, None, None, None, None, None
                elements1 = browser.find_elements_by_xpath(".//div[@class='stats-panel__item__title']")
                elements2 = browser.find_elements_by_xpath(".//div[@class='stats-panel__item__value']")
                for i in range(len(elements1)):
                    key = elements1[i].get_attribute("innerText").strip().lower()
                    value = elements2[i].get_attribute("innerText").strip()
                    if key == 'contest awards':
                        data[key] = value
                    elif key == 'contests in progress':
                        data[key] = value
                    elif key == 'success rate':
                        data[key] = value
                    elif key == 'projects':
                        data[key] = value

                elements = browser.find_elements_by_xpath(".//span[@class='subtle-text']")
                for element in elements:
                    x = element.get_attribute("innerText").strip().lower()
                    if 'country' in x:
                        data['country'] = re.sub(r"[-()\"#/@;.\\:<>{}`+=~|!?,*]", "", x.split(':')[1].strip())
                    elif 'member since' in x:
                        try:
                            writtenFormat = re.sub(r"[-()\"#/@;.\\:<>{}`+=~|!?*]", "", x.split(':')[1].strip());
                            writtenFormat = writtenFormat.split(",")
                            year = writtenFormat[-1]
                            writtenFormat = writtenFormat[0].split(" ")
                            dateV = writtenFormat[-1]
                            month = writtenFormat[0]
                            month = getIntegerMonth(str(month).lower())
                            t = str(year) + str(month) + str(dateV)
                            data['member_since'] = str(pd.to_datetime(t, format='%Y%m%d'))
                        except Exception:
                            data['member_since'] = None

            except Exception as e:
                continue

            data['owner-url'] = data['owner-url'].split("/")[-1]

            CollectorUtil.fetchURL(browser, "{}messages".format(urlpage[:-5]), log)
            elements1 = browser.find_elements_by_xpath(
                ".//span[contains(@class,'activity-item__header__time')]//span[contains(@title, 'MST')]")
            elements2 = browser.find_elements_by_xpath(".//div[contains(@class,'activity-item__comment')]")

            # separated by "#$#" symbol
            feedbackMsgs = []
            feedbackTimeStamps = []
            for each in elements1:
                feedbackTimeStamps.append(parser.parse(each.get_attribute('title'),
                                                       tzinfos={"MST": "UTC-7"}).astimezone(pytz.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"))
            for each in elements2:
                feedbackMsgs.append(each.get_attribute('innerText'))

            if len(feedbackMsgs) > 0 and len(feedbackTimeStamps) > 0:
                insertFeedbackInfoInDB(databaseConnection, contestID, feedbackMsgs, feedbackTimeStamps)

            for key in data:
                if data[key]:
                    data[key] = data[key].replace('\n', ',')
            writeDataToDB(data)
            addContestTracking(contestID, contestStatus, type, rawURL)
            databaseConnection.commit()
            urlNo += 1
            log.info("Information collection for contest: {} is finished successfully.".format(str(contestID)))
    except Exception as e:
        log.error(
            "Error occurred while parsing for URL No: {} and URL {} in category {} => {}".format(str(urlNo), urlpage,
                                                                                                 type,
                                                                                                 str(e)))
        traceback.print_stack()
        browser.close()
        raise Exception("Error occurred while parsing for URL No: {} in category {}".format(str(urlNo), type))


if __name__ == "__main__":
    log.basicConfig(filename='../scratch/logs/ContestInfoCollector_' + str(date.today()) + '.log',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=log.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
    startTime = datetime.now()
    databaseConnection = CollectorUtil.getDBConnection()
    options = Options()
    options.headless = True
    options.add_argument("start-maximized")

    browser = webdriver.Firefox(options=options)

    contests = pd.read_csv(outputCSVOpenContest)
    numberOfRows = len(contests.index)
    contests = contests['URL']
    log.info("Starting parsing all OPEN contest")
    parseContestForInformation(browser, contests, "OPEN", str(numberOfRows))
    log.info("Parsed all OPEN contest")

    contests = pd.read_csv(outputCSVBlindContest)
    numberOfRows = len(contests.index)
    contests = contests['URL']
    log.info("Starting parsing all BLIND contest")
    parseContestForInformation(browser, contests, "BLIND", str(numberOfRows))
    log.info("Parsed all BLIND contest")

    print("Script run time", datetime.now() - startTime)
    browser.close()
    databaseConnection.close()
