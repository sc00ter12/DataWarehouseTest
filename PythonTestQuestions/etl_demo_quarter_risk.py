# imports section
import sys
import pandas as pd
from sqlalchemy import create_engine
import re
import numpy as np


# globals
SQL_SERVER_NAME = "127.0.0.1"
SQL_DATABASE_NAME = "PersonDatabase"
DEMO_TABLE_NAME = "Demographic"
RISK_TABLE_NAME = "QuarterRisk"


def get_filename_details(file_name):
    file_name_wo_ext = file_name.split('.')[0]
    provider_group = file_name_wo_ext[0:-6].strip()
    file_date = file_name_wo_ext[-6:].strip()
    return provider_group, file_date


def demographic_data(df_demo):
    # only first initial for middle name
    df_demo = df_demo.astype({"MiddleName": str})
    df_demo["MiddleName"] = df_demo["MiddleName"].apply(lambda row: row[:1])

    # update sex values
    df_demo = df_demo.astype({"Sex": int})
    df_demo = df_demo.astype({"Sex": str})
    mask_male = (df_demo['Sex'] == '0')
    mask_female = (df_demo['Sex'] == '1')
    df_demo.ix[mask_male, 'Sex'] = 'M'
    df_demo.ix[mask_female, 'Sex'] = 'F'

    # save to sql
    save_to_sql(df_demo, DEMO_TABLE_NAME)


def risk_data(df_risk):
    # we need to remove any records where the risk has not increased
    mask_greater_than = (df_risk["RiskIncreasedFlag"] == "Yes")
    df_increase = df_risk[mask_greater_than].copy()
    cols = df_increase.columns.tolist()
    qtr_1_name = cols[0][-1:]
    qtr_2_name = cols[1][-1:]

    # adjust columns so we remove "Q1" (etc)
    cols_clean = [re.sub('[Q0-9]+', '', x) for x in cols]
    df_increase.columns = cols_clean

    # now we need to unpivot records (basically combine them)
    df_qtr_1 = df_increase.iloc[:, [0, 2, 5, 6]].copy()
    df_qtr_2 = df_increase.iloc[:, [1, 3, 5, 6]].copy()

    # add the Quarter column
    df_qtr_1["Quarter"] = qtr_1_name
    df_qtr_2["Quarter"] = qtr_2_name

    # append them back as one DF, and then reorder the columns
    df_combined = df_qtr_1.append(df_qtr_2)
    df_combined_reorder = df_combined[["ID", "Quarter", "Attributed", "Risk", "FileDate"]]

    # save to sql
    save_to_sql(df_combined_reorder, RISK_TABLE_NAME)


def save_to_sql(df, table_name):
    con = create_engine(
        ''.join(["mssql+pyodbc://@",
                 SQL_SERVER_NAME,
                 "/",
                 SQL_DATABASE_NAME,
                 "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server"])
    )
    df.to_sql(table_name, con, if_exists='append', index=False)


def find_location_of_text(df, text_str, row_start_index, column_max_index):
    # will iterate over columns and rows to see if we have a cell that matches our expected value
    # NOTE: double return so we can properly break out of nested for loops
    for i in range(0, df.shape[1]):
        for j in range(row_start_index, df.shape[0]):
            if str(df.iloc[j, i]) == text_str:
                return tuple((j, i))
        if i >= column_max_index:
            break
    return tuple((-1, -1))


def get_formatted_dfs(df):
    # lets get the location of our main headers
    demographic_details_loc = find_location_of_text(df, "Demographics", 0, df.shape[1])
    risk_details_loc = find_location_of_text(df, "Quarters", 0, df.shape[1])

    # lets determine the max length of rows we should have (when does the data end??)
    last_row_loc = find_location_of_text(df, str(np.nan), demographic_details_loc[0], demographic_details_loc[1])

    # if we didn't get an NAN at the end, likely means the data goes to end. set as such
    if last_row_loc[0] == -1:
        last_row_loc = tuple((df.shape[0], 0))

    # now we should be able to subset df's appropriately (will reset columns based on the value in the correct row)
    df_demo = df.iloc[demographic_details_loc[0] + 2:last_row_loc[0],
                      demographic_details_loc[1]:risk_details_loc[1]]
    df_demo.columns = df.iloc[demographic_details_loc[0] + 1,
                              demographic_details_loc[1]:risk_details_loc[1]]
    df_demo.columns = [re.sub('[^A-Za-z0-9]+', '', x) for x in df_demo.columns.tolist()]
    df_demo.reset_index(drop=True, inplace=True)

    df_risk = df.iloc[risk_details_loc[0] + 2:last_row_loc[0],
                      risk_details_loc[1]:]
    df_risk.columns = df.iloc[risk_details_loc[0] + 1,
                              risk_details_loc[1]:]
    df_risk["ID"] = df_demo["ID"].values
    df_risk.columns = [re.sub('[^A-Za-z0-9]+', '', x) for x in df_risk.columns.tolist()]
    df_risk.reset_index(drop=True, inplace=True)

    return df_demo, df_risk


def main(file_name):
    try:
        df = pd.read_excel(file_name,
                           sheet_name=0)
    except Exception as ex:
        print(ex)
        return

    # get formatted dataframes (detect where data is)
    df_demo, df_risk = get_formatted_dfs(df)

    # append the columns for the Provider Group and Date
    provider_group, file_date = get_filename_details(file_name)
    df_demo["ProviderGroup"] = provider_group
    df_demo["FileDate"] = file_date
    df_risk["FileDate"] = file_date

    # pass a subsetted DF for just Demographics
    demographic_data(df_demo)

    # pass a subsetted DF for just Risks (and Quarters)
    risk_data(df_risk)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # file name not passed in, use default
        main("Privia Family Medicine 113018.xlsx")
    else:
        main(sys.argv[1])
