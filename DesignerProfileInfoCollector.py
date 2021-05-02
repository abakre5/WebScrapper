import pickle
import traceback
import re
from datetime import date

import pandas

import logging as log

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

import CollectorUtil


def insertDesignInfoInDB(dictData):
    try:
        cursor = databaseConnection.cursor(buffered=True)
        cursor.execute('SELECT DISTINCT URL FROM designerinfo WHERE username = "{}"'.format(str(dictData['url'])))
        designerExists = cursor.fetchone()
        if designerExists is not None:
            cursor.execute('DELETE FROM {} WHERE URL = "{}"'.format("designerinfo", str(dictData['url'])))
        placeholder = ", ".join(["%s"] * len(dictData))
        columnsD = "`,`".join(dictData.keys())
        columnsD = '`' + columnsD + '`'
        stmt = 'insert IGNORE into `{table}` ({columns}) values ({values});'.format(table='designerinfo',
                                                                                    columns=columnsD,
                                                                                    values=placeholder)
        cursor.execute(stmt, list(dictData.values()))
        cursor.close()
    except Exception as err:
        log.error("Error occurred while pushing designerInfo for designer {} => {}".format(dictData["url"], str(err)))
        traceback.print_exc()
        # raise Exception("Error occurred while pushing designerInfo for designer {}".format(dictData["url"]))


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


def parse(result):
    noOfDesigners = str(len(result))
    urlNo = 1
    for url in result:
        print(str(urlNo) + "/" + noOfDesigners)
        urlNo += 1
        try:
            urlpage = "https://www.99designs.com" + str(url[0]) + "/about"
            try:
                CollectorUtil.fetchURL(browser, urlpage, log)
                log.info("Hitting URL: {}".format(urlpage))
                cookieFile = open("../Cookie/Cookie.pkl", "rb")
                for cookie in pickle.load(cookieFile):
                    browser.add_cookie(cookie)
            except Exception as e:
                log.warning("URL not available -> " + str(url[0]))
                continue
            cookieFile.close()

            data = {}
            try:
                data['username'] = browser.find_element_by_xpath(".//h1[@class='user-details__name']").get_attribute(
                    "innerText")  # for username
                data['member_since'] = \
                    browser.find_elements_by_xpath(".//span[@class='subtle-text']")[-1].get_attribute(
                        "innerText").split(":")[-1].replace(' ', '')  # for username
                try:
                    writtenFormat = data['member_since']
                    writtenFormat = writtenFormat.split(",")
                    year = writtenFormat[-1]
                    writtenFormat = re.match(r"([a-z]+)([0-9]+)", writtenFormat[0], re.I)
                    dateV = writtenFormat[2]
                    month = writtenFormat[1]
                    month = getIntegerMonth(str(month).lower())
                    t = str(year) + str(month) + str(dateV)
                    data['member_since'] = str(pandas.to_datetime(t, format='%Y%m%d'))
                except Exception:
                    data['member_since'] = None
            except Exception as e:
                log.error("Error occurred while parsing designer: {}".format(str(url[0])))
                continue
            try:
                data["rating"] = browser.find_element_by_xpath(
                    ".//span[contains(@itemprop,'ratingValue')]").get_attribute(
                    "innerText")
                data["review_count"] = browser.find_element_by_xpath(
                    ".//span[contains(@itemprop,'reviewCount')]").get_attribute("innerText")
            except Exception:
                data["rating"] = None
                data["review_count"] = None

            try:
                data["intro"] = browser.find_elements_by_xpath(".//p")[1].get_attribute("innerText").strip()
            except Exception:
                data["intro"] = "NA"

            elements = browser.find_elements_by_xpath(".//div[contains(@class,'stats-panel__item__value')]")

            data["contests_won"] = elements[0].get_attribute("innerText").strip()
            data["runner_up"] = elements[1].get_attribute("innerText").strip()
            data["OneToOne_ projects"] = elements[2].get_attribute("innerText").strip()
            data["repeat_clients"] = elements[3].get_attribute("innerText").strip()

            elements = browser.find_elements_by_xpath(".//span[contains(@class,'pill--tag')]")
            data["tags"] = ""
            for element in elements:
                data["tags"] += (element.get_attribute("innerText") + ", ")
            data["tags"] = data["tags"][:-2]

            elements = browser.find_elements_by_xpath(".//span[contains(@class,'pill--certification')]")
            certifications = set()
            for element in elements:
                certifications.add(element.get_attribute("innerText"))

            elements = browser.find_elements_by_xpath(".//span[contains(@class,'-level')]")
            for element in elements:
                certifications.add(element.get_attribute("innerText"))

            data["certifications"] = ', '.join(str(e) for e in certifications)

            data["url"] = urlpage.split("/")[-2]

            # elements = browser.find_elements_by_xpath(".//span[contains(@class,'pill pill--tag')]")
            # data["languages"] = ""
            # for element in elements:
            #     data["languages"]+=element.get_attribute("innerText")

            # data['prize-money'] = re.sub(r'[\W_]+', '', spans[0].get_attribute("innerText"))
            # data['award-type'] = spans[1].find_element_by_xpath(".//span").get_attribute("innerText")

            insertDesignInfoInDB(data)
            databaseConnection.commit()
            log.info("Successfully parsed information for designer URL {}".format(urlpage))
        except Exception as err:
            log.error("Error occurred while parsing designer information id {} => {}".format(str(url[0]), str(err)))
            browser.close()
#            log.error(traceback.print_stack())
            # raise Exception(
            #     "Error occurred while parsing designer information id {} => {}".format(str(url[0]), str(err)))


def getDesignerUrls():
    try:
        cursor = databaseConnection.cursor()
        cursor.execute("SELECT distinct url FROM designerURLs")
        result = cursor.fetchall()
    except Exception as err:
        log.error("No designers found. Run contest info collection scripts. = > {}".format(str(err)))
        traceback.print_stack()
        raise Exception("No designers found. Run contest info collection scripts.")
    return result


if __name__ == "__main__":
    log.basicConfig(filename='../scratch/logs/DesignerProfileInfoCollector_' + str(date.today()) + '.log',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=log.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
    options = Options()
    options.headless = True
    options.add_argument("start-maximized")

    browser = webdriver.Firefox(options=options)

    databaseConnection = CollectorUtil.getDBConnection()

    result = getDesignerUrls()
    log.info("Starting to get designer information ...")
    parse(result)
    log.info("Designer information gathering completed...")

    databaseConnection.close()
    browser.close()
