import pandas as pd
companies = pd.read_csv('companies.txt',encoding='ISO-8859-1',sep='\t')
companies.permalink = companies.permalink.str.encode('ISO-8859-1').str.decode('ascii', 'ignore')
companies.name = companies.name.str.encode('ISO-8859-1').str.decode('ascii', 'ignore')
# print(companies.head())

rounds2 = pd.read_csv('rounds2.csv',encoding='ISO-8859-1')
rounds2.company_permalink = rounds2.company_permalink.str.encode('ISO-8859-1').str.decode('ascii', 'ignore')
# print(rounds2.head())

#How many unique companies are present in rounds2?
rounds2['company_permalink'] = rounds2['company_permalink'].str.lower()
# print(len(rounds2['company_permalink'].unique()))

# How many unique companies are present in companies?
companies['permalink'] = companies['permalink'].str.lower()
# print(len(companies['permalink'].unique()))

#Are there any companies in the rounds2 file which are not present in companies?
temp1 = pd.DataFrame(rounds2.company_permalink.unique())
temp2 = pd.DataFrame(companies.permalink.unique())
# print(temp2.equals(temp1))

set(companies['permalink'].unique()).difference(set(rounds2['company_permalink'].unique()))

#Merge the two data frames so that all variables (columns) in the companies frame are added to the rounds2 data frame. Name the merged frame master_frame.
master_frame = pd.merge(rounds2, companies, how = 'left', left_on = 'company_permalink', right_on = 'permalink')
# print(len(master_frame.index))


# Inspecting the Null values , column-wise
master_frame.isnull().sum(axis=0)

#Inspecting the Null values percentage , column-wise
# print(round(100*(master_frame.isnull().sum()/len(master_frame.index)), 2))

# Dropping unnecessary columns
master_frame = master_frame.drop(['funding_round_code', 'funding_round_permalink', 'funded_at','permalink', 'homepage_url',
                                 'state_code', 'region', 'city', 'founded_at','status'], axis = 1)

#Inspecting the Null values percentage again after deletion, column-wise
# print(round(100*(master_frame.isnull().sum()/len(master_frame.index)), 2))


#Dropping rows based on null columns
master_frame = master_frame[~(master_frame['raised_amount_usd'].isnull() | master_frame['country_code'].isnull() |
                             master_frame['category_list'].isnull())]


## Task 2: Funding Type Analysis
#Observing the unique funding_round_type
# print(master_frame.funding_round_type.value_counts())

#Retaining the rows with only four investment types
## 1,2,3,4
master_frame = master_frame[(master_frame['funding_round_type'] == 'venture')
                            | (master_frame['funding_round_type'] == 'seed')
                            | (master_frame['funding_round_type'] == 'angel')
                            | (master_frame['funding_round_type'] == 'private_equity')]
# print(master_frame.head())

#Converting $ to million $.
master_frame['raised_amount_usd'] = master_frame['raised_amount_usd']/1000000
# print(master_frame.head())

#calculating average investment amount for each of the four funding types.
# print(round(master_frame.groupby('funding_round_type').raised_amount_usd.mean(), 2))

#Retaining rows with only venture type. As Spark Funds wants to invest between 5 to 15 million USD per investment round
master_frame = master_frame[master_frame['funding_round_type'] == 'venture']

#Dropping the column 'funding_round_type' as it is going to be venture type this point forward
master_frame = master_frame.drop(['funding_round_type'], axis = 1)
# print(master_frame)


## Task 3: Country Analysis
#top 9 countries
top9 = master_frame.pivot_table(values = 'raised_amount_usd', index = 'country_code', aggfunc = 'sum')
top9 = top9.sort_values(by = 'raised_amount_usd', ascending = False)
top9 = top9.iloc[:9, ]
# print(top9)

#Retaining rows with only USA, GBR and IND country_codes. As SparksFunds wants to invest in only top three English speaking countries.
master_frame = master_frame[(master_frame['country_code'] == 'USA')
                            | (master_frame['country_code'] == 'GBR')
                            | (master_frame['country_code'] == 'IND')]


## Task 4: Sector Analysis 1

# Subtask 4.1: Extract the primary sector of each category
#Extracting the primary vector value
master_frame['category_list'] = master_frame['category_list'].apply(lambda x: x.split('|')[0])

# Subtask 4.2: Map each primary sector to one of the eight main sectors
#Reading mapping.csv file
mapping = pd.read_csv('mapping.csv')
mapping.category_list = mapping.category_list.replace({'0':'na', '2.na' :'2.0'}, regex=True)
# print(mapping.head())

#Reshaping the mapping dataframe to merge with the master_frame dataframe. Using melt() function to unpivot the table.
mapping = pd.melt(mapping, id_vars =['category_list'], value_vars =['Manufacturing','Automotive & Sports',
                                                              'Cleantech / Semiconductors','Entertainment',
                                                             'Health','News, Search and Messaging','Others',
                                                             'Social, Finance, Analytics, Advertising'])
mapping = mapping[~(mapping.value == 0)]
mapping = mapping.drop('value', axis = 1)
mapping = mapping.rename(columns = {"variable":"main_sector"})
# print(mapping.head())

master_frame = master_frame.merge(mapping, how = 'left', on ='category_list')
# print(master_frame.head())

#List of primary sectors which have no main sectors in the master_frame
# print(master_frame[master_frame.main_sector.isnull()].category_list.unique())

#Number of rows with NaN masin_sector value
# print(len(master_frame[master_frame.main_sector.isnull()]))

#Retaining the rows which have main_sector values
master_frame = master_frame[~(master_frame.main_sector.isnull())]
# print(len(master_frame.index))

## Task 5: Sector Analysis 2

# Subtask 5.1: Create DataFrames D1, D2, D3 based on three countries
D1 = master_frame[(master_frame['country_code'] == 'USA') &
             (master_frame['raised_amount_usd'] >= 5) &
             (master_frame['raised_amount_usd'] <= 15)]
D1_gr = D1[['raised_amount_usd','main_sector']].groupby('main_sector').agg(['sum', 'count']).rename(
    columns={'sum':'Total_amount','count' : 'Total_count'})
D1 = D1.merge(D1_gr, how='left', on ='main_sector')
# print(D1.head())

D2 = master_frame[(master_frame['country_code'] == 'GBR') &
             (master_frame['raised_amount_usd'] >= 5) &
             (master_frame['raised_amount_usd'] <= 15)]
D2_gr = D2[['raised_amount_usd','main_sector']].groupby('main_sector').agg(['sum', 'count']).rename(
    columns={'sum':'Total_amount','count' : 'Total_count'})
D2 = D2.merge(D2_gr, how='left', on ='main_sector')
# print(D2.head())

D3 = master_frame[(master_frame['country_code'] == 'IND') &
             (master_frame['raised_amount_usd'] >= 5) &
             (master_frame['raised_amount_usd'] <= 15)]
D3_gr = D3[['raised_amount_usd','main_sector']].groupby('main_sector').agg(['sum', 'count']).rename(
    columns={'sum':'Total_amount','count' : 'Total_count'})
D3 = D3.merge(D3_gr, how='left', on ='main_sector')
# print(D3.head())

# Subtask 5.2: Sector-wise Investment Analysis

#Total number of investments (count)
# print(D1.raised_amount_usd.count())
# print(D2.raised_amount_usd.count())
# print(D3.raised_amount_usd.count())

#Total amount of investment (USD)
# print(round(D1.raised_amount_usd.sum(), 2))
# print(round(D2.raised_amount_usd.sum(), 2))
# print(round(D3.raised_amount_usd.sum(), 2))

#Top sector, second-top, third-top for D1 (based on count of investments)
#Number of investments in the top, second-top, third-top sector in D1
# print(D1_gr)

#Top sector, second-top, third-top for D2 (based on count of investments)
#Number of investments in the top, second-top, third-top sector in D2
# print(D2_gr)

#Top sector, second-top, third-top for D2 (based on count of investments)
#Number of investments in the top, second-top, third-top sector in D3
# print(D3_gr)

#For the top sector USA , which company received the highest investment?
company = D1[D1['main_sector']=='Others']
company = company.pivot_table(values = 'raised_amount_usd', index = 'company_permalink', aggfunc = 'sum')
company = company.sort_values(by = 'raised_amount_usd', ascending = False).head()
# print(company.head(1))

#For the second top sector USA , which company received the highest investment?
company = D1[D1['main_sector']=='Social, Finance, Analytics, Advertising']
company = company.pivot_table(values = 'raised_amount_usd', index = 'company_permalink', aggfunc = 'sum')
company = company.sort_values(by = 'raised_amount_usd', ascending = False).head()
# print(company.head(1))

#For the top sector GBR , which company received the highest investment?
company = D2[D2['main_sector']=='Others']
company = company.pivot_table(values = 'raised_amount_usd', index = 'company_permalink', aggfunc = 'sum')
company = company.sort_values(by = 'raised_amount_usd', ascending = False).head()
# print(company.head(1))

#For the second top sector GBR , which company received the highest investment?
company = D2[D2['main_sector']=='Social, Finance, Analytics, Advertising']
company = company.pivot_table(values = 'raised_amount_usd', index = 'company_permalink', aggfunc = 'sum')
company = company.sort_values(by = 'raised_amount_usd', ascending = False).head()
# print(company.head(1))

#For the top sector IND , which company received the highest investment?
company = D3[D3['main_sector']=='Others']
company = company.pivot_table(values = 'raised_amount_usd', index = 'company_permalink', aggfunc = 'sum')
company = company.sort_values(by = 'raised_amount_usd', ascending = False).head()
# print(company.head(1))

#For the second top sector IND , which company received the highest investment?
company = D3[D3['main_sector']=='News, Search and Messaging']
company = company.pivot_table(values = 'raised_amount_usd', index = 'company_permalink', aggfunc = 'sum')
company = company.sort_values(by = 'raised_amount_usd', ascending = False).head()
# print(company.head(1))

