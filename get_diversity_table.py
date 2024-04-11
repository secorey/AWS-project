from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd

url = 'https://wallethub.com/edu/most-diverse-cities/12690'

DRIVER_PATH = '/usr/local/bin/chromedriver'
driver = webdriver.Chrome(executable_path=DRIVER_PATH)

driver.get(url)

print('Please click to expand the desired table, and press enter when finished.')
input()

tables = driver.find_elements(By.TAG_NAME, 'table')
table = tables[1]

# Scrape the table:
rows = table.find_elements(By.TAG_NAME, 'tr')
data = []
for row in rows:
    values = row.find_elements(By.TAG_NAME, 'td')
    if values:
        to_add = []
        for value in values:
            to_add.append(value.text)
        data.append(tuple(to_add))

headers = ['Overall Rank', 'City', 'Total Score', 'Socioeconomic Diversity', 
           'Cultural Diversity', 'Economic Diversity', 'Household Diversity', 
           'Religious Diversity']
df = pd.DataFrame(data, columns=headers)
df.to_csv('data/diversity_table.csv')

driver.quit()