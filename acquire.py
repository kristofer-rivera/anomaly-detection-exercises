import pandas as pd
import os
from env import get_db_url
from sklearn.impute import SimpleImputer

def acquire_curriculum_logs(use_cache = True):
    
    filename = 'curriculum_logs.csv'
    
    if os.path.exists(filename) and use_cache:
        print('Using cached .csv file...')
        return pd.read_csv(filename)
    
    else:
        print('Acquiring data from SQL server...')
        query = """SELECT * FROM logs LEFT JOIN cohorts ON logs.user_id = cohorts.id"""
        df = pd.read_sql(query, get_db_url('curriculum_logs'))
        
        df.to_csv(filename)
        
        return df
    
def acquire_logs(use_cache = True):
    
    filename = 'ogs.csv'
    
    if os.path.exists(filename) and use_cache:
        print('Using cached .csv file...')
        return pd.read_csv(filename)
    
    else:
        print('Acquiring data from SQL server...')
        query = """SELECT * FROM logs;"""
        df = pd.read_sql(query, get_db_url('curriculum_logs'))
        
        df.to_csv(filename)
        
        return df
    
columns_strategy = {
'mean' : [
       'home_sqft',
    'structure_tax_value',
        'assessed_value',
        'land_tax_value',
        'tax_amount'
    ],
    'most_frequent' : [
         'year_built'
     ],
     'median' : [
         'lot_sqft',
         'building_quality'
     ]
 }

def get_single_units(df):
    single_unit = [261, 262, 263, 264, 266, 268, 273, 276, 279]
    df = df[df.propertylandusetypeid.isin(single_unit)]
    return df

def handle_missing_values(df, prop_required_column = .5, prop_required_row = .75):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df

def remove_outliers(df, k, col_list):
    ''' remove outliers from a list of columns in a dataframe 
        and return that dataframe
    '''
    
    for col in col_list:

        q1, q3 = df[col].quantile([.25, .75])  # get quartiles
        
        iqr = q3 - q1   # calculate interquartile range
        
        upper_bound = q3 + k * iqr   # get upper bound
        lower_bound = q1 - k * iqr   # get lower bound

        # return dataframe without outliers
        
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
        
    return df

def impute_missing_values(df, columns_strategy):
    
    for strategy, columns in columns_strategy.items():
        imputer = SimpleImputer(strategy = strategy)
        imputer.fit(df[columns])

        df[columns] = imputer.transform(df[columns])
    
    
    return df

    
def acquire(use_cache=True):
    '''
    This function takes in no arguments, uses the imported get_db_url function to establish a connection 
    with the mysql database, and uses a SQL query to retrieve telco data creating a dataframe,
    The function caches that dataframe locally as a csv file called zillow.csv, it uses an if statement to use the cached csv
    instead of a fresh SQL query on future function calls. The function returns a dataframe with the telco data.
    '''
    filename = 'zillow.csv'

    if os.path.isfile(filename) and use_cache:
        print('Using cached csv...')
        return pd.read_csv(filename)
    else:
        print('Retrieving data from mySQL server...')
        df = pd.read_sql('''
    SELECT
        prop.*,
        predictions_2017.logerror,
        predictions_2017.transactiondate,
        air.airconditioningdesc,
        arch.architecturalstyledesc,
        build.buildingclassdesc,
        heat.heatingorsystemdesc,
        landuse.propertylandusedesc,
        story.storydesc,
        construct.typeconstructiondesc
    FROM properties_2017 prop
    JOIN (
        SELECT parcelid, MAX(transactiondate) AS max_transactiondate
        FROM predictions_2017
        GROUP BY parcelid) pred USING(parcelid)
    JOIN predictions_2017 ON pred.parcelid = predictions_2017.parcelid AND pred.max_transactiondate = predictions_2017.transactiondate
    LEFT JOIN airconditioningtype air USING (airconditioningtypeid)
    LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid)
    LEFT JOIN buildingclasstype build USING (buildingclasstypeid)
    LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid)
    LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid)
    LEFT JOIN storytype story USING (storytypeid)
    LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid)
    WHERE prop.latitude IS NOT NULL AND prop.longitude IS NOT NULL AND transactiondate <= '2017-12-31';''' , get_db_url('zillow'))
        print('Caching data as csv file for future use...')
        df.to_csv(filename, index=False)
    return df


def prepare_zillow(df):
    '''Prepare zillow for data exploration.'''
    df = get_single_units(df)
    columns_to_drop = ['parcelid', 'id', 'calculatedbathnbr', 'finishedsquarefeet12', 'fullbathcnt', 
                   'heatingorsystemtypeid', 'propertycountylandusecode', 'propertylandusetypeid', 
                   'propertyzoningdesc', 'rawcensustractandblock', 'unitcnt', 'assessmentyear', 'transactiondate']
    df = df.drop(columns = columns_to_drop)
    df = handle_missing_values(df)
    df.heatingorsystemdesc.fillna('None', inplace=True)
    df = df.dropna(subset=['regionidcity', 'regionidzip', 'censustractandblock'])
    # remove outliers
    cols = ['taxvaluedollarcnt', 'calculatedfinishedsquarefeet', 'landtaxvaluedollarcnt', 
        'structuretaxvaluedollarcnt', 'taxamount']
    df = remove_outliers(df, 1.5, cols)
    # Creating a new county column using fips values 
    counties = {6037:'Los Angeles', 6059:'Orange', 6111:'Ventura'}
    df['county'] = df.fips.map(counties)
   # rename columns for clarity and readability
    df = df.rename(columns={'bedroomcnt': 'bedrooms', 'bathroomcnt':'bathrooms', 'roomcnt':'rooms',
                            'heatingorsystemdesc':'heating_system', 'propertylandusedesc':'land_use', 'yearbuilt':'year_built',
                            'calculatedfinishedsquarefeet':'home_sqft',
                             'taxvaluedollarcnt':'assessed_value','landtaxvaluedollarcnt':'land_tax_value',
                            'structuretaxvaluedollarcnt':'structure_tax_value',
                            'taxamount':'tax_amount', 'buildingqualitytypeid':'building_quality', 
                            'lotsizesquarefeet': 'lot_sqft'})
    #Converting certain
    df.fips = df.fips.astype(object)
    df.regionidcity = df.regionidcity.astype(object)
    df.regionidcounty = df.regionidcounty.astype(object)
    df.regionidzip = df.regionidzip.astype(object)
    df.censustractandblock = df.censustractandblock.astype(object)
 
    df = impute_missing_values(df, columns_strategy)
    
    df['age'] = df.year_built.max() - df.year_built
    
    
    return df
    
    
