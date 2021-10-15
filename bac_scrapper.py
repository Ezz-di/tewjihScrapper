import timeit
import pandas as pd
from bs4 import BeautifulSoup
from requests.models import Response
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.driver import Driver
import requests
import logging


def get_bac_refs(filename):
    df_bac = pd.read_csv(filename)
    df_bac_ref = df_bac['NumDossier']
    return df_bac_ref

def format_orientation(html):
    pass








def scrapper():
    refs = get_bac_refs('bac_data.csv')
    total_candidats_s = refs.size
    print(total_candidats_s)
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install())

    driver.get("https://www.tewjih.com/cnb2-201-res47s5dCdkjfioog562.php")
    driver.maximize_window()
    df_candidates = []
    total_candidats = []

    total_candidats.append('23136')
    start = timeit.timeit()

    for ref in total_candidats:
        print(ref)
        sleep(5)
        try:
            input_ = driver.find_element_by_xpath("//input[@name='num_bac']")
            input_.clear()
            input_.send_keys(ref)
        except NoSuchElementException:
            pass
        
        try:
            btn_ = driver.find_element_by_xpath("//input[@name='Ajouter']")
            btn_.click()
        except NoSuchElementException:
            pass
        sleep(10)
        Soup = BeautifulSoup(driver.page_source, 'html.parser')

        results = Soup.find("div", {"id" : "Ftrouve"})
        try :
            field_1 = results.find_all("span", {"class", "rouge18B"})[1].text.replace("\n", "").replace("\t", "")
        except IndexError:
            continue
        rows = results.find_all("tr", {"class", "noir11"})
        choices = []

        for row in rows:
            celles = row.find_all("td")
            country = celles[0].text.replace("\t", "").replace("\n", "")
            field = celles[1].text.replace("\t", "").replace("\n", "")
            choice = celles[2].text.replace("\t", "").replace("\n", "")
            me_average = celles[3].text.replace("\t", "").replace("\n", "")
            min_average = celles[4].text.replace("\t", "").replace("\n", "")
            choices.append({'country' : country, 'field': field, 'choice' : choice, 'avg' : me_average, 'min_avg': min_average})
        
        df_candidates.append({'field_accepted' : field_1, 'choices' : choices })
        end = timeit.timeit()
        print(end - start)
    print(df_candidates)
    driver.quit()



            
scrapper()


    




