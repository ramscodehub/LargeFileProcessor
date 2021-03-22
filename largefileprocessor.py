import pandas as pd
import csv
import pymysql

class largeFileProcessor():

    def __init__(self, hostAddress, userName, password, csvfileName):
        self.hostAddress = hostAddress
        self.userName = userName
        self.password = password
        self.csvfileName = csvfileName


    def readCsvToDataframe(self):
        """        
        Reads a given csv file and converts it into\n
        a DataFrame.
        """
        self.dataframe = pd.read_csv(self.csvfileName)

    
    def getDataframeAsListOfTuples(self):
        """
        Returns Dataframe as a listOftuples

        listOfTuples = [
            (row['name'][0], row['sku'][0], row['description'][0]),
            (row['name'][1], row['sku'][1], row['description'][1]),
            (row['name'][len(df)-1], row['sku'][len(df)-1], row['description'][len(df)-1])
        ]
        """
        self.listOfTuples = []
        # open file in read mode
        with open(self.csvfileName, 'r') as read_obj:
            # pass the file object to reader() to get the reader object
            csv_reader = csv.reader(read_obj)
            # Iterate over each row in the csv using reader object
            for count, row in enumerate(csv_reader):
                # row variable is a list that represents a row in csv
                if count != 0:
                    # not including a header i.e. dataframe columns
                    self.listOfTuples.append((row[0], row[1], row[2]))

        
    def connectsToSQLdb(self):
        """

        Connects a MySQL Server (Amazon RDS)
        with the help of given login credentials.

        """
        # self.connectionCursor holds the reference to the mysql database
        # to use LOAD DATA LOCAL INFILE set local_infile = 1.
        self.dbConnection = pymysql.connect(
            host = self.hostAddress,
            user = self.userName,
            port = 3306,
            passwd = self.password,
            local_infile = 1
        )
        self.connectionCursor = self.dbConnection.cursor()

        print("connection established to mysql aws RDS")
    

    def createDatabase(self):
        """
        Creates a database in a database Instance
        """
        # since a db instance can contain multiple user created databases,
        # create a db for products while running the script the first time

        createDatabaseQuery = ''' CREATE DATABASE productsdb '''
        self.connectionCursor.execute(query = createDatabaseQuery)

        print("created database named productsdb")

    
    def createTable(self):
        """
        Creates a Table in the current Database
        """
        # create a table named products in the productsdb
        createTableQuery = ''' CREATE TABLE products(
            `name` VARCHAR(256) NOT NULL,
            `sku` VARCHAR(100) NOT NULL UNIQUE,
            `description` VARCHAR(1024) DEFAULT NULL
        ) '''
        self.connectionCursor.execute(query = ''' USE productsdb''')
        self.connectionCursor.execute(query = createTableQuery)

        print("created table products in products db")
    
    def insertValues(self):
        """
        Inserts Values to a products table
        """

        print("inserting values to products table...")

        insertQuery = ''' INSERT INTO products(
            `name`,
            `sku`,
            `description`
        ) VALUES(%s, %s, %s) ON DUPLICATE KEY UPDATE `description` = VALUES(`description`)
        '''
        
        self.connectionCursor.executemany(insertQuery, self.listOfTuples)
        self.connectionCursor.connection.commit()

        print("inserted values to products table")
    
    def aggreagateProducts(self):
        # created a new table(productAggregation) with name and no of products
        # commit and close connection to database after creating a aggregation table
        aggregateQuery = ''' CREATE TABLE `productAggregation`
        AS SELECT name,count(name) as Count from products group by name '''

        self.connectionCursor.execute(query = aggregateQuery)
        self.connectionCursor.connection.commit()
        # close the connections to database
        self.connectionCursor.close()
        self.dbConnection.close()

        print("created table productAggregation")


def main():
    obj = largeFileProcessor(
        hostAddress="hostAddressEndPoint",
        userName = "userName",
        password = "passwd",
        csvfileName = "products.csv")

    obj.readCsvToDataframe()
    obj.getDataframeAsListOfTuples()
    obj.connectsToSQLdb()
    obj.createDatabase()
    obj.createTable()
    obj.insertValues()
    obj.aggreagateProducts()


if __name__ == "__main__":
    main()

