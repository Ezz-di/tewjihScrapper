import time
import os
import ast
import glob
import json
from tqdm import tqdm
import numpy as np
import concurrent.futures
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


#Link to scrap

SCRAP_LINK = 'https://www.tewjih.com/cnb2-201-res47s5dCdkjfioog562.php'

#Columns of output dataset
columns = [ 
    'Bac_id', 'field_accepted', 
    'country0', 'field0', 'choice0', 'avg0', 'min_avg0',
    'country1', 'field1', 'choice1', 'avg1', 'min_avg1',
    'country2', 'field2', 'choice2', 'avg2', 'min_avg2',
    'country3', 'field3', 'choice3', 'avg3', 'min_avg3',
    'country4', 'field4', 'choice4', 'avg4', 'min_avg4',
    'country5', 'field5', 'choice5', 'avg5', 'min_avg5',
    'country6', 'field6', 'choice6', 'avg6', 'min_avg6',
    'country7', 'field7', 'choice7', 'avg7', 'min_avg7',
    'country8', 'field8', 'choice8', 'avg8', 'min_avg8',
    'country9', 'field9', 'choice9', 'avg9', 'min_avg9',
    'country10', 'field10', 'choice10', 'avg10', 'min_avg10',
    'country11', 'field11', 'choice11', 'avg11', 'min_avg11',
    'country12', 'field12', 'choice12', 'avg12', 'min_avg12',
    'country13', 'field13', 'choice13', 'avg13', 'min_avg13',
    'country14', 'field14', 'choice14', 'avg14', 'min_avg14',
    'country15', 'field15', 'choice15', 'avg15', 'min_avg15',
    'country16', 'field16', 'choice16', 'avg16', 'min_avg16',
    'country17', 'field17', 'choice17', 'avg17', 'min_avg17',
    'country18', 'field18', 'choice18', 'avg18', 'min_avg18',
    'country19', 'field19', 'choice19', 'avg19', 'min_avg19',
    'country20', 'field20', 'choice20', 'avg20', 'min_avg20',
    'country21', 'field21', 'choice21', 'avg21', 'min_avg21',
    'country22', 'field22', 'choice22', 'avg22', 'min_avg22',
    'country23', 'field23', 'choice23', 'avg23', 'min_avg23',
    'country24', 'field24', 'choice24', 'avg24', 'min_avg24',
    'country25', 'field25', 'choice25', 'avg25', 'min_avg25',
    'country26', 'field26', 'choice26', 'avg26', 'min_avg26',
    'country27', 'field27', 'choice27', 'avg27', 'min_avg27',
    'country28', 'field28', 'choice28', 'avg28', 'min_avg28',
    'country29', 'field29', 'choice29', 'avg29', 'min_avg29',
    'country30', 'field30', 'choice30', 'avg30', 'min_avg30'
    ]


def concatenate_bac_files():
    path = os.getcwd()
    bac_files = glob.glob(os.path.join(path + '/bac_files/', "*.csv"))
    dfs = (pd.read_csv(file_, index_col = None) for file_ in bac_files)
    results = pd.concat(dfs, ignore_index = True)
    results.to_csv(os.path.join(path + "/bac_files/" + "bac_orientation.csv"))


def process_file_separatly():
    start = time.time()
    path = os.getcwd()
    df_ref = get_references('bac_data.csv')
    datasets_1000 = prepare_data(df_ref, 10000)
    index_ = 1
    if os.path.exists(path + "/bac_files/datasets_1000_1.csv") :
        index_ = 2
        datasets_1000.pop(0)
    if os.path.exists(path + "/bac_files/datasets_1000_2.csv") :
        index_ = 3
        datasets_1000.pop(0)
    if os.path.exists(path + "/bac_files/datasets_1000_3.csv") :
        index_ = 4
        datasets_1000.pop(0)
    if os.path.exists(path + "/bac_files/datasets_1000_4.csv") :
        datasets_1000.pop(0)

    if datasets_1000 == []:
        print("Data already scrapped")
        exit(0)

    for item_df in datasets_1000:
        df = next(run_scrapper(item_df))
        df.to_csv(path + "/bac_files/datasets_1000_" + str(index_) + '.csv')
        index_+=1
    end = time.time()
    print("it takes %s" %(end - start))
    print("End of processing !!! Good Bye")




def run_scrapper(df) -> None:
    """
    Run the fast scrapper using multiprocess python features.
    Save data into a csv file
    """
    datasets_100 = prepare_data(df, 1000)
    # refs = []
    # refs.append('3435454455')
    # datasets_1000 = []
    # datasets_1000.append(refs)
    start = time.time()
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers = 10) as pexec:
        candidat_res = {pexec.submit(fast_scrapper, dataset_100) for dataset_100 in datasets_100} 
        for candidat in concurrent.futures.as_completed(candidat_res):
            res = candidat.result()
            if res != [{}] and res != {}:
                results = results + res
    df = pd.DataFrame(results, columns = columns)
    yield df
    # df.to_csv('bac_orientation.csv')
    # end = time.time()
    # print(end - start)




def prepare_data(df, nb_split):
    """
    Pre-prepare the dataset, it returns multiple set of 1000 rows.
    @param 
        filenme : csv file input
    @return
        datasets_1000 
    """
    total_datasets_1000_rows = int(df.size / nb_split) + 1
    
    start_row = 0
    end_row = nb_split
    datasets_1000 = []
    for row in range(total_datasets_1000_rows):
        if end_row == df.size:
            end_row = df.size
        sub_data = df.iloc[start_row : end_row]
        datasets_1000.append(sub_data)
        start_row += nb_split
        end_row += nb_split
    return datasets_1000

def get_references(filename):
    """
    Fetch only the reference column
    @param:
        filename
    @return:
        A dataframe of refrences.
    """
    df_bac = pd.read_csv(filename)
    df_bac_ref = df_bac['NumDossier']
    return df_bac_ref

def fast_scrapper(refs):
    """
    A fast scrapper based on Multithreading python.
    @param:
        refs : Array of references.
    @returns:
        array
    """

    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install())
    driver.get(SCRAP_LINK)
    driver.maximize_window()
    df_candidates = []
    total_candidats = refs


    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers = 30) as ex:
        for candidat in total_candidats:
            candidat_res = ex.submit(get_element, candidat, driver)
            res = candidat_res.result()
            if res != {} and res != []:
                res_len = len(res)
                column_len = len(columns)
                if res_len < column_len:
                    for i in range(res_len, column_len):
                        res.append("null")
                results.append(res)
    driver.quit()

    return results

def get_element(student_number, driver):
    """
        Scrapp student information. 
        Convert selenium to Beautiful to facilate the scrapping.
        @params :
            student_number -> Reference
            driver -> selenium chrom web driver.
        @return :
            Array : Student object
    """
    try:
        input_ = driver.find_element_by_xpath("//input[@name='num_bac']")
        input_.clear()
        input_.send_keys(student_number)
    except NoSuchElementException:
        pass
        
    try:
        btn_ = driver.find_element_by_xpath("//input[@name='Ajouter']")
        btn_.click()
    except NoSuchElementException:
        pass
    sleep(0.4)
    Soup = BeautifulSoup(driver.page_source, 'html.parser')

    results = Soup.find("div", {"id" : "Ftrouve"})
    try :
        field_1 = results.find_all("span", {"class", "rouge18B"})[1].text.replace("\n", "").replace("\t", "")
    except IndexError:
        return {}
    choices = []
    choices.append(student_number)
    choices.append(field_1)
    rows = results.find_all("tr", {"class", "noir11"})

    count = 0
    for row in tqdm(rows):
        c_str = str(count)
        celles = row.find_all("td")
        choices.append(celles[0].text.replace("\t", "").replace("\n", ""))
        choices.append(celles[2].text.replace("\t", "").replace("\n", ""))
        choices.append(celles[3].text.replace("\t", "").replace("\n", ""))
        choices.append(celles[1].text.replace("\t", "").replace("\n", ""))
        choices.append(celles[4].text.replace("\t", "").replace("\n", ""))
        count += 1

    print(choices)
    return choices


def scrapper():
    """
    A slow scrapper.

    """
    refs = get_references('bac_data.csv')
    total_candidats_s = refs.size
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install())

    driver.get("https://www.tewjih.com/cnb2-201-res47s5dCdkjfioog562.php")
    driver.maximize_window()
    df_candidates = []
    total_candidats = []
    start = time.time()

    for ref in total_candidats:
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
        sleep(7)
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
        end = time.time()
        print(end - start)
    print(df_candidates)
    driver.quit()




if __name__ == '__main__' : 
    concatenate_bac_files()

    




