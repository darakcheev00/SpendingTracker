import os
import sys
import csv
import pandas as pd
from datetime import datetime
import hashlib

class General:
    def __init__(self):
        self.dataColumnNames = ["Date", "Transaction", "Category", "Amount Spent", "Amount Received"]
        self.importedColumnNames = ["Date", "Transaction", "Amount Spent", "Amount Received"]
        
        self.df_data = None
        self.lastTransDate = datetime(1900, 1, 1) # default old date
        self.load()
        
    def load_category_map(self):
        # load the map of known categories and common transactions per category from json file
        pass
        
    def load(self):
        # load data about existing transactions from local file
        # data file has date, transaction full name, category, spent, received

        dataFile = "data.csv"
        # if no data.csv file, create one
        if not os.path.isfile(dataFile):
            with open(dataFile, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.dataColumnNames)
                writer.writeheader()
        
        self.df_data = pd.read_csv(dataFile, parse_dates=["Date"], date_format={"Date":"%Y-%m-%d"})
        
        # read date of last transaction
        if len(self.df_data) > 0:
            self.lastTransDate = self.df_data.iloc[0,0]
        
        # print(self.df_data.head())
        print(self.lastTransDate)
        
    def readDate(self, dateString):
        return datetime.strptime(dateString, "%Y-%m-%d")
    
    def import_new(self, newFile):
        # import new transactions and merge with old. Avoid duplicates
        imports_dir = "./imports_dropbox"
        df_imported = pd.read_csv(os.path.join(imports_dir, newFile), names=self.importedColumnNames, parse_dates=["Date"], date_format={"Date":"%Y-%m-%d"})

        df_new_categorized = self.categorize(self.removeDuplicates(df_imported))
        
        self.saveNewTransactions(df_new_categorized)
        # print(self.df_data.head())
    
    def removeDuplicates(self, df_new):
        df_filtered = df_new[df_new['Date'] >= self.lastTransDate].copy()
        
        # pull transactions from lastDate from df_data, remove Category col and remove duplicates. This prevents removing dups on entire data
        df_data_last_day = self.df_data[self.df_data['Date'] == self.lastTransDate].drop(columns=['Category'])
        set_of_hashes = set(self.hash_row(r) for _, r in df_data_last_day.iterrows())
        
        for i in range(len(df_filtered)-1, -1, -1):
            row = df_filtered.iloc[i]
            currDate = row['Date']
            if currDate == self.lastTransDate:
                if self.hash_row(row) in set_of_hashes:
                    print(f"dropping row {i}")
                    df_filtered.drop(i, inplace=True)
            elif currDate > self.lastTransDate:
                break
            
        return df_filtered
    
    def hash_row(self, row):
        row_string = row.to_string(index=False)
        return hashlib.md5(row_string.encode()).hexdigest()
    
    def categorize(self,df_new):
        # go through all entries and auto-categorize all the uncategorized entries
        
        # add a category column
        
        return df_new
    
    def saveNewTransactions(self, df_new):
        self.df_data = pd.concat([df_new, self.df_data], ignore_index=True)
        self.df_data.to_csv("data.csv",index=False)
    
    def showStats(self):
        # display counts of 
        #   - categorized, uncategorized
        #   - counts by category
        #   - top 5 spenders in each category
        pass
    
if __name__ == "__main__":
    g1 = General()
    g1.import_new("new_transactions.csv")