#!/usr/bin/env python
# coding: utf-8

#Libraries: pre-installed in Anaconda
import requests
from requests.adapters import HTTPAdapter, Retry
from time import sleep
from bs4 import BeautifulSoup
import re
import numpy as np

##########################
### [2a] Data Sourcing ###
##########################

def scrape_psle_page(html_school_id: str, timeout: int, retries: int):
    '''Webscrapes summary examination results data from one NECTA PSLE response
    
    Parameters:
        html_school_id (str): e.g. 'shl_ps1104063.htm'
        timeout (int): timeout in seconds for requests.get (HTTP GET)
        retries (int): number of HTTP connection retries
    
    Returns:
        Raw unprocessed page data (dict)
    '''

    #Scrape HTML from response > parse with Beautiful Soup
    #URL = f"https://onlinesys.necta.go.tz/results/2022/psle/results/shl_ps1104063.htm"
    URL = f"https://onlinesys.necta.go.tz/results/2022/psle/results/{html_school_id}"
    
    #Needed for full dataset webscraping
    s = requests.Session()
    r = Retry(total=retries, backoff_factor=1)
    s.mount('https://', HTTPAdapter(max_retries=r))  
    
    #DEBUG CODE
    while True:
        try:
            response = s.get(URL, timeout=timeout)
            #response = requests.get(URL, timeout=timeout)
            with open('textout/http_responses.txt', 'a') as f:
                f.write(f"{URL} {response.status_code} {response.reason}\n")
            break
        except requests.exceptions.ConnectionError as error:
            with open('textout/http_responses.txt', 'a') as f:
                f.write(f"{URL} {response.status_code} {response.reason} {error}\nSleeping for 10 seconds...\n")
            sleep(10)
            continue
    
    soup = BeautifulSoup(response.content, "html.parser")

    #initialize dictionary of school data
    page = dict()
    
    #(i) Find/regex school information
    school = soup.find('h3').get_text(strip=True)
    #m = re.search('(.*\S)\s*PRIMARY SCHOOL -\s+(\w+)', school)
    m = re.search('(.*\S)\s*(PRIMARY SCHOOL|SEMINARY)\W+(\w+)', school)
    if (m != None):
        page['school_name'] = m[1].upper() #match TAMISEMI uppercase names
        page['school_id'] = m[3]
    else:
        with open('textout/regex_mismatches.txt', 'a') as f:
            f.write(f"[scrape_psle_page]: No regex school result at {URL}\n")

    #(ii) Find/regex school overall results
    results = soup.find('font').get_text(strip=True)
    m = re.search('WALIOFANYA MTIHANI\D*(\d+)\s*WASTANI WA SHULE\D*([\d.]+)\s*DARAJA\W+(\w)', results)
    if (m != None):
        page['num_students'] = int(m[1]) #number of students who "sat for exam"
        page['average_300'] = float(m[2])
        page['grade'] = m[3]
    else:
        with open('textout/regex_mismatches.txt', 'a') as f:
            f.write(f"[scrape_psle_page]: No regex students result at {URL}\n")
    
    #(iii) Find/regex gender x grade results
    #grades = soup.find('table') #ERROR: </table> after 'JINSI'

    grades = soup.find_all('tr', limit=4) #WORKAROUND for a broken first table
    for tr in grades[1:]: #rows
        for i, td in enumerate(tr.find_all('td')): #column-data
            if i == 0: #index column of row
                idx = td.get_text(strip=True)
                page[idx] = []
            else:
                page[idx].append(int(td.get_text(strip=True)))
    
    return page

def assign_grade(marks: int):
    '''Assigns grade based on range of mark
    
    Parameters:
        marks (int): mark from 0 to 300
    Returns:
        Grade from A-E (str)
    '''
    if marks < 60.5:
        return 'E'
    elif marks < 120.5:
        return 'D'
    elif marks < 180.5:
        return 'C'
    elif marks < 240.5:
        return 'B'
    else:
        return 'A'

def calc_approx_marks_SD(x):
    '''Calculates approximate (using mid-points) marks Standard Deviation (SD)
    Notes:
        A [240.5,300], B [180.5,240.5), C [120.5,180.5), D [60.5,120.5), E [0,60.5)
        
    Parameters:
        x (list of int): list of total students with results at each grade level in order A-E
    Returns:
        Standard Deviation (float) of results (out of 300) approximated by grade level mid-points
    '''
    #approximate using mid-point of grade range
    A_to_E = [[270.25], [210.5], [150.5], [90.5], [30.25]]
    student_grades = list()
    for i in range(5):
        student_grades += A_to_E[i] * x[i]
    #print(student_grades)
    return np.std(student_grades)

def calc_pbr_average(x):
    """Calculate average Pupil-to-Book Ratio (PRB) from all class and subject PBRs for one school
    
    Parameters:
        x (Series): DataFrame row with specific PBR data columns
    Returns:
        Average (float) of all pupil-to-book ratios where pupils and books are non-zero integers"""
    
    pbr_list = list()
    #first project use of list comprehension!
    for p in [col for col in x.index if 'Pupils' in col]:
        if x[p] > 0:
            m = re.search('(^Std [1-7])-Pupils', p) #get 'Std N'   
            for b in [col for col in x.index if (m[1] in col and 'Pupils' not in col and 'PBR' not in col)]:
                if x[b] > 0:
                    pbr_list.append(x[p] / x[b]) #calc PBR per book
    if pbr_list:
        return np.mean(pbr_list)
    else:
        return np.nan #empty list (no books reported)

def get_ages_stats(x):
    '''Calculate statistics based on per-age/gender student counts data for one school
    Notes:
        Assumes Below 6 = 5, Above 13 = 14 (minimal SD)
    
    Parameters:
        x (Series): DataFrame row of TAMISEMI ages data
    Returns:
        Standard deviation, mean, and median (tuple of floats) of student ages
    '''
    student_ages = list()
    for col in x.index:
        if col.startswith('Below'): #boys and girls
            m = re.search('^Below\s+(\d+)', col)
            age = int(m[1]) - 1
            student_ages += [age] * x[col] #value (age) * value_count (#students)
        elif col.startswith('Above'):
            m = re.search('^Above\s+(\d+)', col)
            age = int(m[1]) + 1
            student_ages += [age] * x[col]
        elif col[0].isdigit():
            m = re.search('^(\d+)', col)
            age = int(m[1])
            student_ages += [age] * x[col]
    #my first project use of a tuple!
    if student_ages:
        return np.std(student_ages), np.mean(student_ages), np.median(student_ages)
    else:
        return np.nan, np.nan, np.nan

##########################
### [2b] Data Cleaning ###
##########################   

#Ref: https://machinelearningmastery.com/how-to-use-statistics-to-identify-outliers-in-data/
def outliers_check_sd(col, n):
    '''Finds outliers based on number of Standard Deviations away from mean
    
    Parameters:
        col (Series): DataFrame column of one feature in dataset
        n (int): number of SDs for outlier identification
    Returns:
        List of boolean indicating whether data point is an outlier
    '''
    cut_off = np.std(col) * n
    lower = np.mean(col) - cut_off
    upper = np.mean(col) + cut_off
    outliers = col.apply(lambda x : x < lower or x > upper)
    return outliers

def outliers_check_iqr(col):
    '''Finds outliers based on Interquartile Range (IQR) Method away (1.5*IQR away from 25% and 75% quantiles)
    
    Parameters:
        col (Series): DataFrame column of one feature in dataset
        n (int): number of SDs for outlier identification
    Returns:
        List of boolean indicating whether data point is an outlier
    '''
    iqr = np.quantile(col, 0.75) - np.quantile(col, 0.25)
    cut_off = 1.5 * iqr
    lower = np.quantile(col, 0.25) - cut_off #could go negative, that is OK
    upper = np.quantile(col, 0.75) + cut_off
    outliers = col.apply(lambda x : x < lower or x > upper)
    return outliers

#####################
### [2c] Features ###
#####################

def extract_council_type(x, u):
    '''Extracts a new categorical feature of urban/rural based on council name
    
    Parameters:
        x (string): council name where urban councils contain an urban acronym 
        u (list of strings): urban acronym strings to regex match for
    Returns:
        Urban or rural council type (str)
    '''
    m = re.search('.*\s+([A-Z]+)', x)
    if (m != None) and (m[1] in u):
        return 'urban'
    else:
        return 'rural'

def calc_d(y, x):
    '''Calculates distance between two sets of coordinates
    
    Parameters:
        x (tuple of two floats): set of coordinates (latitude, longitude)
        y (tuple of two floats): set of coordinates (latitude, longitude)
    Returns:
        Distance (float) between two points
    '''
    a = x[0] - y[0]
    b = x[1] - y[1]
    c = np.hypot(a,b) #get distance between points
    return c if c > 0 else np.nan #don't populate distance from itself = 0

def find_closest_d(x, s_y):
    '''Finds distance between two sets of coordinates
    Notes:
        Exhaustive!
    
    Parameters:
        x (tuple of two floats): set of coordinates (latitude, longitude)
        s_y (Series of tuples of two floats): other sets of coordinates to compare x to
    Returns:
        Distance (float) between x and closest coordinate in s_y
    '''
    s_d = s_y.apply(calc_d, args=(x,))
    return s_d.idxmin(), s_d.min()
