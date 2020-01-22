#!/usr/bin/env python
# coding: utf-8

# In[83]:


def database():
    import sqlite3
    conn = sqlite3.connect('inf510_hadj_db.db')
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS LA_County ')
    cur.execute('CREATE TABLE LA_County (item_id INTEGER, item_type TEXT, data_type TEXT, foreign_key INTEGER)')
    cur.execute('DROP TABLE IF EXISTS Wells')
    cur.execute('CREATE TABLE Wells (object_id INTEGER, api INTEGER, lat REAL, lon REAL)')
    cur.execute('DROP TABLE IF EXISTS Schools')
    cur.execute('CREATE TABLE Schools (object_id INTEGER, cds_code INTEGER, lat REAL, lon REAL, level TEXT, pb_priv_bie TEXT)')
    cur.execute('DROP TABLE IF EXISTS Census_Tracts')
    cur.execute('CREATE TABLE Census_Tracts (object_id INTEGER, geo_id INTEGER, ci_score INTEGER)')
    conn.close()
    conn = sqlite3.connect('inf510_hadj_db.db')
    cur = conn.cursor()


# In[84]:


#cur.execute("SELECT name FROM sqlite_master")
#result = cur.fetchall()
#print(set(result))


# In[91]:


def Census_Tracts(dictionary):
    conn = sqlite3.connect('inf510_hadj_db.db')
    cur = conn.cursor()
    geo_ids = dictionary['Tract_1']
    for x in range(len(geo_ids)):
        count = 0
        cur.execute('SELECT geo_id FROM Census_Tracts')
        for row in cur:
            if row[0] != geo_ids[x]:
                count +=1
                continue
        if count != 0:
            pass
        else:
            cur.execute('INSERT INTO Census_Tracts (geo_id, ci_score) VALUES ( ?, ? )', ( geo_ids[x], dictionary['CIscore'][x]) )
    conn.commit()
    conn.close()
def Wells(dictionary):
    conn = sqlite3.connect('inf510_hadj_db.db')
    cur = conn.cursor()
    apis = dictionary['api']
    for x in range(len(apis)):
        count = 0
        cur.execute('SELECT api FROM Wells')
        for row in cur:
            if row[0] != apis[x]:
                count +=1
                continue
        if count != 0:
            pass
        else:
            cur.execute('INSERT INTO Wells (api, lat, lon) VALUES ( ?, ?, ? )', ( apis[x], dictionary['lat'][x], dictionary['lon'][x]) )
    conn.commit()
    conn.close()
def Schools(dictionary):
    conn = sqlite3.connect('inf510_hadj_db.db')
    cur = conn.cursor()
    codes = dictionary['CDS_Code']
    for x in range(len(codes)):
        count = 0
        cur.execute('SELECT cds_code FROM Schools')
        for row in cur:
            if row[0] != codes[x]:
                count +=1
                continue
        if count != 0:
            pass
        else:
            cur.execute('INSERT INTO Schools (cds_code, lat, lon, level, pb_prv_bie) VALUES ( ?, ?, ?, ?, ? )', ( codes[x], dictionary['Lat'][x], dictionary['Lon'][x], dictionary['Level_'][x], dictionary['Pb_Prv_BIE'][x]) )
    conn.commit()
    conn.close()
def LA_County():
    conn = sqlite3.connect('inf510_hadj_db.db')
    cur = conn.cursor()
    cur.execute('SELECT object_id FROM Census_Tracts')
    for row in cur:
        cur.execute('INSERT INTO LA ( item_type, data_type, foreign_key) VALUES ( ?, ?, ? )', ( 'Census Tract', 'Polygon Attribute', object_id) )
        conn.commit()
    cur.execute('SELECT object_id FROM Wells')
    for row in cur:
        cur.execute('INSERT INTO LA ( item_type, data_type, foreign_key) VALUES ( ?, ?, ? )', ( 'Well', 'Point Feature', object_id) )
        conn.commit()
    cur.execute('SELECT object_id FROM Schools')
    for row in cur:
        cur.execute('INSERT INTO LA ( item_type, data_type, foreign_key) VALUES ( ?, ?, ? )', ( 'School', 'Point Feature', object_id))
        conn.commit()
    conn.close()


# In[96]:


def Load():
    Database()
    Schools(school_data)
    Wells(wells)
    Census_Tracts(ejsm_data)
    LA_County()


# In[97]:


def Load_Local():
    import csv
    wells = {"api" : [], "lat" : [], "lon" : []}
    with open('well_latlon_data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if row[0] == "api":
                continue
            else:
                wells["api"].append(row[0])
                wells["lat"].append(row[1])
                wells["lon"].append(row[2])
    school_data = {"CDS_Code" : [], "Lat" : [], "Lon" : [], "Pb_Prv_BIE" : [], "Level_": []}
    with open('school_data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if row[0] == "CDS_Code":
                continue
            else:
                school_data["CDS_Code"].append(row[0])
                school_data["Lat"].append(row[1])
                school_data["Lon"].append(row[2])
                school_data["Level_"].append(row[3])
                school_data["Pb_Prv_BIE"].append(row[4])
    ejsm_data = {"Tract_1" : [], "CIscore" : []}
    with open('ejsm_data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if row[0] == "Tract_1":
                continue
            else:
                school_data["Tract_1"].append(row[0])
                school_data["CIscore"].append(row[1])
    Load()


# In[1]:


def Load_Schools():
    import requests
    url = 'https://services1.arcgis.com/4ZKi1B1zTblbwgWB/ArcGIS/rest/services/California_School_Campus_Database_2018_(CSCD)/FeatureServer/0/query?outFields=*&where=1%3D1'
    html_response = requests.get(url)
    ed_records = list()
    get_params = {
        'f': 'json',
        'token': 'llYgI7XHeOlln3KoQVmBH-pJI2-Hwzyhv7aNMrYmuON_YmjrvSSD0YqX9CE_KmdvwPLFe38Ct8kgiotIeuJrpoOvt0L_Lm98sxComZgSQo57OY5Ggq3ZignUCBkrf9rgFuIQmBBVZxKJoKYtyAt9X-Qzwm4J2qSF7C_jmoWy_qWjrT5Wy2AbTFLr92SrAFHPBk7WiLyHk_FAEvvlYHWBbF0rLL1L6Vaw2CYxdrFU-YY.'  
        }
    try:
        cal_ed = requests.get(url, params=get_params)
        cal_ed.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
    else:
        print(f'{cal_ed.url} was successfully retrieved with status code {cal_ed.status_code}')
        cal_school_centroids = cal_ed.json()
        print(cal_school_centroids)
        print(f"Retrieved {len(cal_school_centroids['features'])} records.")
        ed_records.extend(cal_school_centroids['features'])
    school_records = []
    for x in range(len(ed_records)):
        school_records.append(ed_records[x]['attributes'])
    to_extract = ('CDSCode','Status', 'County','Level_','Pb_Prv_BIE', 'Lat', 'Long')
    import csv
    school_data = {}
    for column in to_extract:
        school_data[column] = []
    for item in school_records:
        if item['County'] != 'Los Angeles':
            continue
        if item['Status'] != 'Active':
            continue
        for column in to_extract:
            school_data[column].append(item[column])
    #school_data
    #Completeness
    #for column in to_extract:
    #    print(len(school_data[column]))
    import csv
    with open('school_data.csv', mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=to_extract)
        writer.writeheader()
        for x in range(len(school_data['County'])):
            writer.writerow({to_extract[0]: school_data[to_extract[0]][x], to_extract[1]: school_data[to_extract[1]][x], to_extract[2]: school_data[to_extract[2]][x], to_extract[3]: school_data[to_extract[3]][x], to_extract[4]: school_data[to_extract[4]][x],to_extract[5]: school_data[to_extract[5]][x], to_extract[6]: school_data[to_extract[6]][x]})
    csv_file.close()
def Load_EJSM():
    import requests
    url = 'https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/EJSM_Scores/FeatureServer/0/query?outFields=*&where=1%3D1'
    html_response = requests.get(url)
    records = list()
    get_params = {
        'f': 'json',
        'token': 'llYgI7XHeOlln3KoQVmBH-pJI2-Hwzyhv7aNMrYmuON_YmjrvSSD0YqX9CE_KmdvwPLFe38Ct8kgiotIeuJrpoOvt0L_Lm98sxComZgSQo57OY5Ggq3ZignUCBkrf9rgFuIQmBBVZxKJoKYtyAt9X-Qzwm4J2qSF7C_jmoWy_qWjrT5Wy2AbTFLr92SrAFHPBk7WiLyHk_FAEvvlYHWBbF0rLL1L6Vaw2CYxdrFU-YY.',
        'id': 0
        }
    try:
        ejsm = requests.get(url, params=get_params)
        ejsm.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
    else:
        print(f'{ejsm.url} was successfully retrieved with status code {ejsm.status_code}')
        allcensustracts = ejsm.json()
        print(allcensustracts)
        print(f"Retrieved {len(allcensustracts['features'])} records.")
        records.extend(allcensustracts['features'])
    ejsm_records = []
    for x in range(len(records)):
        ejsm_records.append(records[x]['attributes'])
    to_extract = ('Tract_1','CIscore','Shape__Area','Shape__Length')
    import csv
    ejsm_data = {}
    for column in to_extract:
        ejsm_data[column] = []
    for item in ejsm_records:
        #verification that these are LA county tracts
        #if str(item['Tract_1'])[0:4] != '6037':
        #    print(item['OBJECTID'], item['Tract_1'])
        #    continue
        #verification that there are no duplicates
        #y = set(ejsm_data['Tract_1'])
        #if item['Tract_1'] in y:
        #    print(item['OBJECTID'])
        for column in to_extract:
            #verification that there is no missing data
            #if item[column] == "" or item[column] == " ":
            #    print(item['OBJECTID'])
            ejsm_data[column].append(item[column])
    #ejsm_data
    #data completeness validation
    #for column in to_extract:
    #    print(len(ejsm_data[column]))
    #this extracts data from ejsm_data dictionary to csv file
    with open('ejsm_data.csv', mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=to_extract)
        writer.writeheader()
        for x in range(len(ejsm_data['Tract_1'])):
            writer.writerow({to_extract[0]: ejsm_data[to_extract[0]][x], to_extract[1]: ejsm_data[to_extract[1]][x], to_extract[2]: ejsm_data[to_extract[2]][x], to_extract[3]: ejsm_data[to_extract[3]][x],})
    csv_file.close()
def Load_Wells():
    import csv
    well_api = []
    with open('well_apis.csv', encoding = "utf-8-sig") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            try:
                while row[-1] == "":
                    row.pop()
                if row[0] == 'CA Well Results [Active Wells only]':
                    continue
                if row[0] == ' County:Los Angeles 037':
                    continue
                if row[0] == 'District #':
                    continue
                well_api.append(row[6])
            except:
                continue
    print('There are currently ', len(well_api), 'active wells in Los Angeles County.')
    print(well_api)
    import requests
    from bs4 import BeautifulSoup
    wells = {'lat': [], 'lon':[]}
    #ones_that_got_away_200 = []
    #ones_that_got_away_sorry = []
    for element in range(len(well_api)):
        url = f'https://secure.conservation.ca.gov/WellSearch/Details?api={well_api[element]}&District=&County=037&Field=&Operator=&Lease=&APINum=&address=&ActiveWell=true&ActiveOp=true&Location=&sec=&twn=&rge=&bm=&PgStart=0&PgLength=10&SortCol=6&SortDir=asc&Command=Search'
        r = requests.get(url)
        #if '200' not in repr(requests.get(url)):
        #    ones_that_got_away_200.append(well_api[element])
        #    continue
        soup = BeautifulSoup(r.content, 'lxml')
        #if 'Sorry!,Well Information not found using API Number' in repr(soup):
        #    ones_that_got_away_sorry.append(well_api[element])
        #    continue
        main_table = soup.find('div', {"class" : "panel-body"})
        lat = main_table.find_all('div', {"class" : "row bottomMargin"})[3].select('div[class="col-sm-1"]')[0].contents[4].strip()
        #try:
        #    float(lat)
        #except:
        #    print(well_api[element],'_',lat)
        lon = main_table.find_all('div', {"class" : "row bottomMargin"})[3].select('div[class="col-sm-2"]')[2].contents[4].strip()
        #try:
        #    float(lon)
        #except:
        #    print(well_api[element],'_',lon)
        #try:
        wells['lat'].append(lat)
        wells['lon'].append(lon)
        #except:
        #    continue
    #print(wells)
    #validation of data completeness
    #len(wells['lon'])
    #len(wells['lat'])
    to_extract = ('api','lat','lon')
    with open('well_latlon_data.csv', mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=to_extract)
        writer.writeheader()
        for x in range(len(wells['lat'])):
            writer.writerow({to_extract[0]: well_api[x], to_extract[1]: wells[to_extract[1]][x], to_extract[2]: wells[to_extract[2]][x],})
    csv_file.close()


# In[2]:


def Load_Remote():
    Load_Schools()
    Load_EJSM()
    Load_Wells()
    Load()


# In[47]:





# In[ ]:




