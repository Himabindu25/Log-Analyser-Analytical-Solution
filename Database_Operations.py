from pymongo import MongoClient
from Analyse_Logs import AnalyseLogs


class DatabaseOperations:

    # Declaration of global variables
    database = collection = client = ''

    # Connection to the local MongoDB
    def db_connect(self):

        try:

            # Database connection string
            self.client = MongoClient('localhost', 27017)

            # Change the DATABASE and COLLECTION name accordingly
            self.database = self.client.Test_DB                              # Database Name: Test_DB

            self.collection = self.database.Test                        # Collection Name: Test

            print('Database Connection Successful')

        except Exception as ex:

            print('Error: {}.\nCannot connect to database.'.format(ex))

    # To close the database connection
    def db_close(self):

        self.client.close()                                         # Closes the database connection

    # To create database/collection and insert data into it
    def db_insert(self, insert_string, file_name):

        # Convert data to JSON format to insert into database/collection
        insert_data = {'file name': file_name,
                       'file content': insert_string}

        record_id = self.collection.insert_one(insert_data)

        return record_id

    # Convert the content of fle into a single string to store in MongoDB
    @staticmethod
    def file_content(file_path):

        con_str = ''                                             # String variable to store data that is to be inserted

        file_name = r'{}'.format(file_path).replace('\\', '/').split('/').pop()     # To extract the file name

        file_read = open(file_path, 'r')

        for lines in file_read:

            con_str += lines

        return con_str, file_name                           # Returns the concatenated string & the extracted file name

    # Retrieve all records from the database/collection
    def read_all_rec(self):

        return self.collection.find({})

    # Retrieve record from the database/collection by file name
    # def read_by_field(self, file_name):
    #
    #     db_record = self.collection.find({'file name': file_name})          # Extracts the required row from database
    #
    #     for data_dict in db_record:                                         # Converts the extracted row to dictionary
    #
    #        return data_dict['file content'].split('\n')                    # Returns the 'file content' string as list


if __name__ == '__main__':

    db_ops = DatabaseOperations()
    a_log = AnalyseLogs()
    db_ops.db_connect()

    # Menu driven program for CRUD operations
    while True:

        print('\nChoose from the following database operations:\n1. Create/Insert data into Database/Collections\n'
              '2. Read from existing records and extract important data\n3. Exit Menu\n')

        try:
            pick = int(input('Choice: '))

            if pick == 1:

                try:

                    insert_str, f_name = db_ops.file_content(input('File path to be inserted: ').replace('\\', '/'))

                    print('Inserted into Database: ' + str(db_ops.db_insert(insert_str, f_name)))

                except Exception as e:

                    print('Error: {}.\nFile path not found.'.format(e))

            elif pick == 2:

                count = 0

                print('\nFiles present in database: ')
                cursor = db_ops.read_all_rec()          # Cursor having all documents present in database/collection

                print("\nLogs read from the database/collections: ")
                # Print the content of cursor one by one
                for records in cursor:

                    ext_file_name = records['file name']

                    count += 1
                    print('\n'+str(count)+'. '+ext_file_name)

                    data_list = db_ops.read_by_field(ext_file_name)

                    a_log.extract_keyword(data_list)
                    a_log.extarct_steps(data_list)
                    a_log.extract_utility(data_list)
                    a_log.extract_condcode(data_list)
                    a_log.extract_lib(data_list)
                    a_log.step_division(data_list)
                    a_log.extract_abend(data_list)
                    a_log.extract_reason(data_list)
                    a_log.extract_abend_info(data_list)
                    a_log.extract_job_info(data_list)

                cursor.close()  # Close Cursor

            elif pick == 3:
                break

            else:
                print('Invalid Choice')

        except Exception as e:

            print('Error: {}.\nChoice should be integer.'.format(e))

    db_ops.db_close()
