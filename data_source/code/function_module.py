import pandas as pd

#Function to count missing values and drop the column if missing value more than threshold
def remove_missing_value_col(df, treshold):
    under_threshold = []
    print("REMOVING MISSING VALUE UNDER THRESHOLD")
    for column in df.columns:
        percent = df[column].isna().sum() / len(df[column]) * 100
        print(column + ': ' + str(percent))

        if percent > treshold:
            under_threshold.append(column)
    
    print('Removed columns = ' + str(under_threshold))
    df = df.drop(columns=under_threshold)
    df.reset_index()
    return df

def count_missing_value_col(df):
    print("COLUMN NAME AND ITS PERCENTAGE OF MISSING VALUE")
    for column in df.columns:
        percent = df[column].isna().sum() / len(df[column]) * 100
        print(column + ': ' + str(percent))

#Function to count duplicate data
def count_duplicate(df):
    print("DUPLICATE DATA CHECKING")
    n_dup = len(df[df.duplicated(keep=False)])
    print('Duplicate data: ' + str(n_dup) + ' out of ' + str(len(df)))

def remove_duplicate(df):
    print(str(len(df[df.duplicated(keep='first')])) + " DATA WILL BE REMOVED")
    df = df.drop_duplicates(keep = 'first')
    return df

#Function to check unique value of every column
def unique_val(df):
    for column in df.columns:
        uniq_val = df[column].unique().tolist()
        if len(uniq_val) > 50:
            print("Col " + column + ' have unique val as much ' + str(len(uniq_val)))
        else:
            print("Col " + column + ' have unique val as much ' + str(len(uniq_val)))
            print(uniq_val)

def validation_process(df, table_name):
    #Calculate Percentage of Missing Value
    print(f"========== Start {table_name} Pipeline Validation ==========")
    # check data shape
    n_rows = df.shape[0]
    n_cols = df.shape[1]

    print(f"The {table_name}table has {n_rows} rows and {n_cols} columns")
    count_missing_value_col(df)
    count_duplicate(df)
    unique_val(df)
    print("========== End Pipeline Validation ==========")