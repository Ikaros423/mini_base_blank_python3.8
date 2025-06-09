# -----------------------
# main_db.py
# author: Jingyu Han   hjymail@163.com
# modified by: Ning Wang, Yidan Xu
# -----------------------------------
# This is the main loop of the program
# ---------------------------------------

import struct
import sys
import ctypes
import os

import head_db  # the main memory structure of table schema
import schema_db  # the module to process table schema
import storage_db  # the module to process the storage of instance

import query_plan_db  # for SQL clause of which data is stored in binary format
import lex_db  # for lex, where data is stored in binary format
import parser_db  # for yacc, where data is stored in binary format
import common_db  # the global variables, functions, constants in the program
import query_plan_db  # construct the query plan and execute it

PROMPT_STR = '\nInput your choice  \n1:add a new table structure and data \n2:delete a table structure and data\
\n3:view a table structure and data \n4:delete all tables and data \n5:select from where clause\
\n6:delete a row according to field keyword \n7:update a row according to field keyword \n. to quit):\n'


# --------------------------
# the main loop, which needs further implementation
# ---------------------------

def main():
    # main loops for the whole program
    print('main function begins to execute')

    # The instance data of table is stored in binary format, which corresponds to chapter 2-8 of textbook

    schemaObj = schema_db.Schema()  # to create a schema object, which contains the schema of all tables
    dataObj = None
    choice = input(PROMPT_STR)

    while True:

        if choice == '1':  # add a new table and lines of data
            tableName = input(f'\033[34mplease enter your new table name:\033[0m')
            if isinstance(tableName, str):
                tableName = tableName.encode('utf-8')
            #  tableName not in all.sch
            insertFieldList = []
            if tableName.strip() not in schemaObj.get_table_name_list():
                # Create a new table
                dataObj = storage_db.Storage(tableName)

                insertFieldList = dataObj.getFieldList()

                schemaObj.appendTable(tableName, insertFieldList)  # add the table structure
            else:
                dataObj = storage_db.Storage(tableName)

                # to the students: The following needs to be further implemented (many lines can be added)
                record = []
                Field_List = dataObj.getFieldList()
                for x in Field_List:
                    s = f'Input field name is: {x[0].strip()}  field type is: {x[1]} field maximum length is: {x[2]}\n'
                    record.append(input(s))

                if dataObj.insert_record(record):  # add a row
                    print(f'\033[32mOK!\033[0m')
                else:
                    print(f'\033[31mWrong input!\033[0m')

                del dataObj

            choice = input(PROMPT_STR)





        elif choice == '2':  # delete a table from schema file and data file

            schemaObj.viewTableNames()  # view all the table names in the schema file
            table_name = input(f'\033[34mplease input the name of the table to be deleted:\033[0m')
            if isinstance(table_name,str):
                table_name=table_name.encode('utf-8')
            if schemaObj.find_table(table_name.strip()):
                if schemaObj.delete_table_schema(
                        table_name):  # delete the schema from the schema file
                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.delete_table_data(table_name.strip())  # delete table content from the table file
                    del dataObj

                else:
                    print(f'\033[31mthe deletion from schema file fail\033[0m')


            else:
                print(f'\033[31mthere is no table {table_name} in the schema file\033[0m')


            choice = input(PROMPT_STR)



        elif choice == '3':  # view the table structure and all the data

            schemaObj.viewTableNames() # view all the table names in the schema file
            table_name = input(f'\033[34mplease input the name of the table to be displayed:\033[0m')
            if isinstance(table_name,str):
                table_name=table_name.encode('utf-8')
            if table_name.strip():
                if schemaObj.find_table(table_name.strip()):
                    schemaObj.viewTableStructure(table_name)  # to be implemented

                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.show_table_data()  # view all the data of the table
                    del dataObj
                else:
                    print(f'\033[31mtable name is None\033[0m')

            choice = input(PROMPT_STR)



        elif choice == '4':  # delete all the table structures and their data
            table_name_list = list(schemaObj.get_table_name_list())
            # to be inserted here -> to delete from data files
            for i in range(len(table_name_list)):
                table_name = table_name_list[i]
                table_name.strip()

                if table_name:
                    stObj = storage_db.Storage(table_name)
                    stObj.delete_table_data(table_name.strip())  # delete table data
                    del stObj

            schemaObj.deleteAll()  # delete schema from schema file

            choice = input(PROMPT_STR)


        elif choice == '5':  # process SELECT FROM WHERE clause
            print('#        Your Query is to SQL QUERY                  #')
            sql_str = input(f'\033[34mplease enter the select from where clause:\033[0m')
            lex_db.set_lex_handle()  # to set the global_lexer in common_db.py
            parser_db.set_handle()  # to set the global_parser in common_db.py

            try:
                common_db.global_syn_tree = common_db.global_parser.parse(sql_str.strip(),
                                                                          lexer=common_db.global_lexer)  # construct the global_syn_tree
                #reload(query_plan_db)
                query_plan_db.construct_logical_tree()
                query_plan_db.execute_logical_tree()
            except:
                print(f'\033[31mWRONG SQL INPUT!\033[0m')
            print('#----------------------------------------------------#')
            choice = input(PROMPT_STR)


        elif choice == '6':  # delete a line of data from the storage file given the keyword

            schemaObj.viewTableNames() # view all the table names in the schema file
            table_name = input(f'\033[34mplease input the name of the table to be deleted from:\033[0m')

            # to the students: to be inserted here, delete the line from data files
            if isinstance(table_name,str):
                table_name=table_name.encode('utf-8')
            if table_name.strip():
                if schemaObj.find_table(table_name.strip()):
                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.show_table_data()  # view all the data of the table

                    field_keyword = input(f'\033[34mplease input the field name and the corresponding keyword (fieldname:keyword):\033[0m')
                    if dataObj.delete_record(field_keyword):
                        print(f'\033[32mdelete record success!\033[0m')
                        dataObj.show_table_data()  # view all the data of the table
                    else:
                        print(f'\033[31mdelete record fail!\033[0m')

                    del dataObj
                else:
                    print(f'\033[33mtable name is None\033[0m')

            choice = input(PROMPT_STR)


        elif choice == '7':  # update a line of data given the keyword

            schemaObj.viewTableNames()  # view all the table names in the schema file
            table_name = input(f'\033[34mplease input the name of the table:\033[0m')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8').strip()
                if schemaObj.find_table(table_name.strip()):
                    dataObj = storage_db.Storage(table_name)
                    dataObj.show_table_data()
                    field_keyword = input(f'\033[34mplease input the field name and the corresponding keyword (fieldname:keyword):\033[0m')
                    if dataObj.delete_record(field_keyword):  # delete the record first
                        print(f'\033[32mdelete old record success!\033[0m')

                        record = []
                        Field_List = dataObj.getFieldList()
                        print(f'\033[34mPlease input the new record data:\033[0m')
                        for x in Field_List:
                            s = f'Input field name is: {x[0].strip()}  field type is: {x[1]} field maximum length is: {x[2]}\n'
                            record.append(input(s))

                        if dataObj.insert_record(record):  # add a row
                            print(f'\033[32mupdate record success!\033[0m')
                            dataObj.show_table_data()  # view all the data of the table
                        else:
                            print(f'\033[31mupdate record fail!\033[0m')
                    else:
                        print(f'\033[31mdelete old record fail!\033[0m')

                    del dataObj
                else:
                    print(f'\033[33mtable name is None\033[0m')

            choice = input(PROMPT_STR)



        elif choice == '.':
            print('main loop finishies')
            del schemaObj
            break

        else:
            print(f'\033[31mWrong input! Please input again!\033[0m')
            choice = input(PROMPT_STR)

    print('main loop finish!')


if __name__ == '__main__':
    main()
