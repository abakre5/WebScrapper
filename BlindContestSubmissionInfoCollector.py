import json
import logging as log
import time
import traceback

from datetime import datetime, date
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import pandas as pd

import CollectorUtil


def getContestFinalParsingStatus(contestID):
    try:
        cursor = databaseConnection.cursor()
        cursor.execute("SELECT STATUS, FINALPARSINGDONE FROM CONTESTDATA WHERE CONTESTID = " + str(contestID))
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as err:
        log.error("Contest {} not found in the contestdata table. Please run ContestInfoCollector.py".format(str(contestID), str(err)))
        traceback.print_stack()
        raise Exception("Contest {} not found".format(str(contestID)))


def parse(browser, contests, noOfRows):
    parentDirPath = CollectorUtil.createCollectorImageDir("../Data/BlindContestSubmissionImages", log)
    try:
        urlNo = 1
        for url in contests:
            index = 0
            contestData = []
            designerURLs = set()
            contestID = url.split("-")[-1]
            result = CollectorUtil.getCollectorContestFinalParsingStatus(databaseConnection, contestID, log)
            invalidEntry = CollectorUtil.getInvalidEntries(databaseConnection, contestID, log)
            print(str(urlNo) + "/" + noOfRows)
            invalidEntry = list(invalidEntry)
            urlNo += 1
            if not ((result[0].lower() == 'finished' and result[1] == 1) or (result[0].lower() == 'locked')):
                while True:
                    try:
                        index += 1
                        if str(index) in invalidEntry:
                            continue
                        CollectorUtil.getImageCollectionURL(browser, index, url, log)
                        try:
                            dataScrapped = browser.find_element_by_id("standalone-design-details-app-data")
                        except Exception:
                            log.info("Information collection for contest {} is finished. {} entries found".format(str(contestID), str(index - 1)))
                            break
                        data = {}
                        jsonObj = json.loads(dataScrapped.get_attribute('innerText'))
                        contestID = str(jsonObj['designCollection']['listingid'])

                        imageDetail = jsonObj['designCollection']['_embedded']['designs'][0]
                        data['ContestId'] = str(contestID)
                        data['ImageURL'] = str(imageDetail['image_url'])
                        data['Eliminated'] = str(imageDetail['is_eliminated'])
                        data['Withdrawn'] = str(imageDetail['is_withdrawn'])
                        data['Deleted'] = str(imageDetail['is_deleted'])
                        data['EntryId'] = str(imageDetail['entry_id'])
                        data['Winner'] = str(imageDetail['is_winner'])
                        data['Rating'] = str(imageDetail['rating'])
                        data['LastUpdated'] = str(
                            datetime.fromtimestamp(imageDetail['time_created']).astimezone().strftime(
                                "%Y-%m-%d %H:%M:%S"))
                        # str(datetime.utcnow())
                        data['OwnerProfileUrl'] = str(
                            jsonObj['designCollection']['_embedded']['designs'][0]['_embedded']['user']['profileUrl'])
                        designerURLs.add((data['OwnerProfileUrl']))
                    except Exception as err:
                        print("An error occurred while parsing dom for contest {} => {}".format(str(contestID), str(err)))
                        traceback.print_stack()
                        browser.close()
                        raise Exception("An error occurred while parsing dom for contest {} => {}".format(str(contestID), str(err)))
                    CollectorUtil.getCollectorImage(contestID, data, parentDirPath, result[0].lower() == 'finished', log)
                    contestData.append(data)
                CollectorUtil.insertCollectorContestImageInfo(databaseConnection, contestData, log)
                CollectorUtil.insertCollectorDesigner(databaseConnection, designerURLs)
                CollectorUtil.updateCollectorParsingStatusForFinishedContest(databaseConnection, contestID, result[0].lower() == 'finished')
                databaseConnection.commit()
    except Exception as err:
        log.error("Error occurred while parsing contest id {} => {}".format(str(contestID), str(err)))
        traceback.print_stack()
        raise Exception("Error occurred while parsing contest id {} => {}".format(str(contestID), str(err)))


if __name__ == "__main__":
    log.basicConfig(filename='../scratch/logs/BlindContestSubmissionInfoCollector' + str(date.today()) + '.log',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=log.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
    startTime = datetime.now()

    outputCSVBlindContest = "../Data/ContestLinks/Blind_Contest_" + str(date.today()) + ".csv"

    options = Options()
    options.headless = True
    browserFirefox = webdriver.Firefox(options=options)

    contestsList = pd.read_csv(outputCSVBlindContest)
    numberOfRows = len(contestsList.index)
    contestsList = contestsList['URL']

    databaseConnection = CollectorUtil.getDBConnection()

    log.info("Starting to parse BLIND contest image submissions")
    parse(browserFirefox, contestsList, str(numberOfRows))
    log.info("Completed parsing BLIND contest image submissions")

    browserFirefox.close()
    databaseConnection.close()

    print("Script run time", datetime.now() - startTime)
