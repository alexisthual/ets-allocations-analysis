# %%
import csv
import logging
import pickle
import requests
import threading

from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm

# %%
url = 'https://ec.europa.eu/clima/ets/ohaDetails.do?accountID={}&action=all&languageCode=en'

accountHolders = []
accountHoldersHeaders = [
  'accountID',
  'installationID',
  'nationalAdministrator',
  'accountType',
  'accountHolderName'
]

installations = []
installationInformationHeaders = [
  'accountID',
  'installationID',
  'installationName',
  'mainActivity'
]

complianceHistory = []
complianceHistoryHeaders = [
  'accountID',
  'installationID',
  'year',
  'allowancesInAllocation',
  'verifiedEmissions',
  'unitsSurrendered',
  'complianceCode'
]

def searchPages(i, j):
  with requests.Session() as s:
    for accountID in tqdm(range(i, j)):
      try:
        response = requests.get(url=url.format(str(accountID)))
      except requests.exceptions.RequestException as e:
        logging.error('accountID [{}]: {}'.format(accountID, e))
        pass

      soup = BeautifulSoup(response.text, 'html.parser')

      # Account holder information
      installationID = None
      nationalAdministrator = None
      accountType = None
      accountHolderName = None

      try:
        accountHolderInformation = soup \
          .find('table', id='tblAccountGeneralInfo') \
          .findAll('tr', recursive=False)[2] \
          .findAll('td')
        
        nationalAdministrator = accountHolderInformation[0].find('span').text.strip()
        accountType = accountHolderInformation[1].find('span').text.strip()
        accountHolderName = accountHolderInformation[2].find('span').text.strip()
        installationID = accountHolderInformation[3].find('span').text.strip()

        accountHolders.append([
          accountID,
          installationID,
          nationalAdministrator,
          accountType,
          accountHolderName
        ])
      except:
        pass

      # Installation information
      installationID = None
      installationName = None
      mainActivity = None

      try:
        installationInformation = soup \
          .find('table', id='tblChildDetails')
        
        installationID = installationInformation \
          .findAll('table')[0] \
          .findAll('tr', recursive=False)[2] \
          .findAll('td')[0] \
          .find('span').text.strip()
        installationName = installationInformation \
          .findAll('table')[0] \
          .findAll('tr', recursive=False)[2] \
          .findAll('td')[1] \
          .find('span').text.strip()
        mainActivity = installationInformation \
          .findAll('table')[1] \
          .findAll('tr', recursive=False)[2] \
          .findAll('td')[7] \
          .find('span').text.strip()
        
        installations.append([
          accountID,
          installationID,
          installationName,
          mainActivity
        ])
      except:
        pass

      # Compliance information
      year = None
      allowancesInAllocation = None
      verifiedEmissions = None
      unitsSurrendered = None
      complianceCode = None

      try:
        complianceHistoryRows = soup \
          .findAll('table', id='tblChildDetails')[1] \
          .find('table') \
          .findAll('tr', recursive=False)

        for row in complianceHistoryRows[2:]:
          information = row.findAll('td')

          year = information[1].find('span').text.strip()
          allowancesInAllocation = information[2].find('span').text.strip()
          verifiedEmissions = information[3].find('span').text.strip()
          unitsSurrendered = information[4].find('span').text.strip()
          complianceCode = information[7].find('span').text.strip()

          complianceHistory.append([
            accountID,
            installationID,
            year,
            allowancesInAllocation,
            verifiedEmissions,
            unitsSurrendered,
            complianceCode
          ])
      except:
        pass

# %%
if __name__ == '__main__':
  # Checked min = 70 000 but 90 000 is enough
  MIN = 90000
  # Checked max = 130 000 but 120 000 is enough
  MAX = 120000
  THREADS = 5
  STEP = (MAX - MIN) // THREADS

  # Allocate and start all threads
  for t in range(THREADS):
    t = threading.Thread(
      target=searchPages,
      args=(
        MIN + t * STEP,
        MIN + (t+1) * STEP if t != THREADS-1 else MAX,
      )
    )
    t.start()

  # Wait for all threads to finish
  main_thread = threading.main_thread()
  for t in threading.enumerate():
    if t is not main_thread:
      t.join()

  print('\nSuccessful requests: {} / {} ({}%)'.format(len(successfulRequests), MAX - MIN, round(len(successfulRequests) / (MAX - MIN)) * 100))

  # Write files
  files = [
    ('account_holders', accountHolders, accountHoldersHeaders),
    ('installations', installations, installationInformationHeaders),
    ('compliance_history', complianceHistory, complianceHistoryHeaders),
  ]

  for fileName, rows, headers in files:
    with open('./data/{}-{}.csv'.format(fileName, round(datetime.now().timestamp())), 'w') as out_file:
      writer = csv.writer(out_file, delimiter=';', quotechar="'")

      writer.writerow(headers)
      for row in rows:
        writer.writerow(row)