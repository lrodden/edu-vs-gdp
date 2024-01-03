import requests, json, csv, os, time


class RetrieveData():

    def __init__(self):
        # API key from https://census.gov
        self.CENSUS_KEY = '2181665eddf7d99ec09349a573ddd5a4beeb918c'
        
        # API key from https://apps.bea.gov/API/signup/
        self.BEA_KEY = '084F6B76-36BE-431F-8F2A-54429DF5E04C' # '895C8114-0D34-4F09-9984-4347056D0B48'  # 

        # API key from https://data.bls.gov/registrationEngine/
        self.BLS_KEY = '4d24ae1296b14e789b1f20b0edda25f8'

        # API root for census dataset
        # reference: https://www.census.gov/data/developers/data-sets/acs-1year.html
        self.CENSUS_URL = f'https://api.census.gov/data'

        # API root for BEA (GDP) dataset
        # reference: https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf
        self.BEA_URL = f'https://apps.bea.gov/api/data'

        # API root for BLS (employment) dataset
        # reference: https://www.bls.gov/developers/
        self.BLS_URL = f'https://api.bls.gov/publicAPI/v2/timeseries/data/'
        
        # available years for dataset
        self.YEARS = [
            '2005', '2006', '2007', '2008', '2009', '2010', 
            '2011', '2012', '2013', '2014', '2015', '2016',  
            '2017', '2018', '2019', '2020', '2021', '2022'
        ]

        # available states for dataset
        self.STATES = [ 
            'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
            'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
            'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
            'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
            'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY'
        ]




    def get_ed_levels(self):

        # relevant variables in dataset 
        ed_levels = [
            {
                'desc': 'population',
                'label': 'B06009_001E',
                'level': 0
            },
            {
                'desc': 'no_high_school', 
                'label': 'B06009_002E', 
                'level': 1 
            },
            {
                'desc': 'high_school', 
                'label': 'B06009_003E', 
                'level': 2 
            },
            {
                'desc': 'some_college', 
                'label': 'B06009_004E', 
                'level': 3 
            },
            {
                'desc': 'bachelors', 
                'label': 'B06009_005E', 
                'level': 4 
            },
            {
                'desc': 'graduate', 
                'label': 'B06009_006E', 
                'level': 5 
            },
        ]
        
        # serializing API request data
        params = {
            'get': 'group(B06009)', # can use group 7009 or 6009
            'for': 'us:1',
            'key': self.CENSUS_KEY
        }

        # initializing empty timeserise dataset
        ed_levels_by_year = {}
        for YEAR in self.YEARS:
            ed_levels_by_year[YEAR] = {}


        # looping through available years
        for YEAR in self.YEARS:
            res = requests.get(f'{self.CENSUS_URL}/{YEAR}/acs/acs1', params=params)
            data = None
            try:
                data = res.json()
            except:
                print(f'Skipping YEAR {YEAR}')
            
            if data:
                i = 0
                # looping through labels 
                # to find correct index 
                for n in data[0]:
                    for m in ed_levels:
                        if n == m['label']:
                            # adding total from raw data
                            total = data[1][i]
                            ed_levels_by_year[YEAR][m['desc']] = int(total)
                            ed_levels_by_year[YEAR]['year'] = YEAR
                    i += 1

            print(ed_levels_by_year[YEAR])

        # reformat data
        data = []
        for key in ed_levels_by_year:
            data.append(ed_levels_by_year[key])

        # adding fields for csv
        fields = [
            'year', 'population', 'no_high_school', 'high_school', 
            'some_college', 'bachelors', 'graduate'
        ]
        
        # saving data as csv
        with open(f'Education vs GDP/exports/education_levels.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)

        # returning data
        return ed_levels_by_year



    def get_gdp(self):

        # initializing empty timeserise dataset
        gdp_by_year = {}
        for YEAR in self.YEARS:
            gdp_by_year[YEAR] = {}


        # request data by year
        for YEAR in self.YEARS:

            # reference for 'NIPA' table names 
            # - T10101 (Percet change of GPD) 

            # serializing API request data
            params = {
                'UserID': self.BEA_KEY,
                'method': 'GetData', 
                'DataSetName': 'NIPA', 
                'TableName': 'T10101', 
                'Frequency': 'A',
                'Year': YEAR, 
                'ResultFormat': 'JSON',
            }
            
            # send request
            res = requests.get(f'{self.BEA_URL}', params=params)
            data = None
            try:
                data = res.json()
            except Exception as e:
                print(e)
                print(f'Skipping YEAR {YEAR}')

            # cleaning & adding data
            data_list = data['BEAAPI']['Results']['Data']
            for n in data_list:
                if n['SeriesCode'] == 'A191RL':
                    percent_change = n['DataValue']
                    gdp_by_year[YEAR]['gdp_percent_change'] = float(percent_change)


            # reference for 'REGIONAL' table names 
            # - SQGDP2 (Gross domestic product (GDP) by state) 
            # - SQGDP9 (Real GDP by state)
            # - SQGDP11 (Contributions to percent change in real GDP)

            # getting industry contributions for 
            # 'Finance' & 'Real estate' by state 
            # then compiling by year
            contribution_finance = 0
            contribution_realestate = 0
            for state in self.STATES:
                params = {
                    'UserID': self.BEA_KEY,
                    'method': 'GetData', 
                    'DataSetName': 'REGIONAL', 
                    'TableName': 'SQGDP11', 
                    'Frequency': 'A',
                    'Year': YEAR, 
                    'ResultFormat': 'JSON',
                    'GeoFips': state,
                    'LineCode': 'ALL'
                }
                
                # send request
                res = requests.get(f'{self.BEA_URL}', params=params)
                data = res.json()
                
                # cleaning data
                data_list = data['BEAAPI']['Results']['Data']
                
                # extract all quarter data from 'Finance and insurance' (SQGDP11-51) 
                # and 'Real estate and rental and leasing' (SQGDP11-56) category 
                state_contribution_finance = 0
                state_contribution_realestate = 0
                for n in data_list:
                    if n['Code'] == 'SQGDP11-51':
                        state_contribution_finance += float(n['DataValue'])
                    if n['Code'] == 'SQGDP11-56': 
                        state_contribution_realestate += float(n['DataValue'])

                # compiling contributions from states
                contribution_finance += state_contribution_finance
                contribution_realestate += state_contribution_realestate

            # cleaning & adding contribution data by year
            gdp_by_year[YEAR]['contribution_finance'] = contribution_finance
            gdp_by_year[YEAR]['contribution_realestate'] = contribution_realestate
            gdp_by_year[YEAR]['year'] = YEAR
            print(gdp_by_year[YEAR])

            # sleeping 60 seconds to comply
            # with API rate limits
            time.sleep(60)       

        # reformat data
        data = []
        for key in gdp_by_year:
            data.append(gdp_by_year[key])

        # adding fields for csv
        fields = [
            'year', 'gdp_percent_change', 'contribution_finance', 
            'contribution_realestate', 
        ]
        
        # saving data as csv
        with open(f'Education vs GDP/exports/gdp_changes.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)

        # returning data
        return gdp_by_year




    def get_unemployment(self):

        # initializing empty timeserise dataset
        unemp_by_year = {}
        for YEAR in self.YEARS:
            unemp_by_year[YEAR] = {}

        # request params  
        params = {
            'registrationkey': self.BLS_KEY,
            'seriesid': 'LNS14000000', 
            'startyear': self.YEARS[0],
            'endyear': self.YEARS[-1]
        }

        # send request
        res = requests.post(f'{self.BLS_URL}', data=params)
        data = res.json()
        
        # cleaning data
        data_list = data['Results']['series'][0]['data']
        year_rates = []
        i = 0

        # looping through available years
        # and averaging unemployment rate by year
        for n in data_list:

            # getting year and rate
            year = n['year']
            value = float(n['value'])

            if i == 0:
                year_rates.append(value)

            # checking if last in serise
            if data_list[-1] != n:
                if year != data_list[i+1]['year'] and len(year_rates) != 0:
                    # avg year_rates
                    avg_unemp_rate = sum(year_rates)/len(year_rates)
                    
                    # add to dataset
                    unemp_by_year[year]['year'] = year
                    unemp_by_year[year]['avg_unemployment_rate'] = avg_unemp_rate
                    print(unemp_by_year[year])
                    
                    # reset year_rates 
                    year_rates = []
                
                elif i != 0:
                    # add monthly rate to year_rates
                    year_rates.append(value)

            else:
                if len(year_rates) != 0:
                    # avg year_rates
                    avg_unemp_rate = sum(year_rates)/len(year_rates)
                    
                    # add to dataset
                    unemp_by_year[year]['year'] = year
                    unemp_by_year[year]['avg_unemployment_rate'] = avg_unemp_rate
                    print(unemp_by_year[year])
                    
                    # reset year_rates 
                    year_rates = []

            i += 1


        # reformat data
        data = []
        for key in unemp_by_year:
            data.append(unemp_by_year[key])

        # adding fields for csv
        fields = [
            'year', 'avg_unemployment_rate'
        ]
        
        # saving data as csv
        with open(f'Education vs GDP/exports/unemp_changes.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)

        # returning data
        return unemp_by_year




# RetrieveData().get_gdp()
# RetrieveData().get_ed_levels()
# RetrieveData().get_unemployment()