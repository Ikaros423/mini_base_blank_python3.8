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
import log_db  # the module to process the transaction log, which is stored in binary format
import transaction_db  # the module to process the transaction

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

    # --- 1. 初始化日志和事务管理器 ---
    log_manager = log_db.LogManager()
    
    # --- 2. 系统启动时执行恢复 (关键步骤) ---
    # 这将确保数据库从任何潜在的崩溃中恢复到一致的状态
    log_manager.recover()

    # --- 3. 初始化事务管理器 ---
    transaction_manager = transaction_db.TransactionManager(log_manager)

    # The instance data of table is stored in binary format, which corresponds to chapter 2-8 of textbook

    schemaObj = schema_db.Schema()  # to create a schema object, which contains the schema of all tables
    dataObj = None
    choice = input(PROMPT_STR)

    while True:

        if choice == '1':  # add a new table and lines of data
            tx_id = None
            try:
                # --- 开始事务 ---
                tx_id = transaction_manager.begin_transaction()

                tableName = input(f'\033[34mplease enter your new table name:\033[0m')
                if isinstance(tableName, str):
                    tableName = tableName.encode('utf-8')
                
                # 创建 Storage 实例时传入 log_manager
                # 模式修改本身也应该是事务性的，但为简化，此处只将数据插入设为事务性
                dataObj = storage_db.Storage(tableName, log_manager)

                #  tableName not in all.sch
                insertFieldList = []
                if tableName.strip() not in schemaObj.get_table_name_list():

                    insertFieldList = dataObj.getFieldList()
                    schemaObj.appendTable(tableName, insertFieldList)  # add the table structure
                    print(f'\033[32mTable schema created.\033[0m')
                else:
                    # to the students: The following needs to be further implemented (many lines can be added)
                    record = []
                    Field_List = dataObj.getFieldList()
                    for x in Field_List:
                        s = f'Input field name is: {x[0].strip()}  field type is: {x[1]} field maximum length is: {x[2]}\n'
                        record.append(input(s))

                    # 传递事务ID
                    if dataObj.insert_record(record, tx_id):
                        print(f'\033[32mRecord prepared for insertion.\033[0m')
                    else:
                        raise Exception("Wrong input for record!")

                # --- 提交事务 ---
                transaction_manager.commit(tx_id)
                print(f'\033[32mTransaction {tx_id} committed successfully!\033[0m')

            except Exception as e:
                print(f'\033[31mError: {e}\033[0m')
                if tx_id:
                    # --- 中止事务 ---
                    transaction_manager.abort(tx_id)
                    print(f'\033[31mTransaction {tx_id} aborted and rolled back.\033[0m')
            finally:
                if dataObj:
                    del dataObj
                choice = input(PROMPT_STR)


        elif choice == '2':  # delete a table from schema file and data file
            tx_id = None
            try:
                schemaObj.viewTableNames()  # view all the table names in the schema file
                table_name = input(f'\033[34mplease input the name of the table to be deleted:\033[0m')
                if isinstance(table_name,str):
                    table_name=table_name.encode('utf-8')
                if schemaObj.find_table(table_name.strip()):
                    # --- 开始事务 ---
                    tx_id = transaction_manager.begin_transaction()  

                    # 假设 delete_table_schema 方法和 delete_table_data 方法都支持事务
                    if schemaObj.delete_table_schema(table_name):  # delete the schema from the schema file
                        dataObj = storage_db.Storage(table_name, log_manager)  # create an object for the data of table
                        dataObj.delete_table_data(table_name.strip())  # delete table content from the table file
                        del dataObj

                        transaction_manager.commit(tx_id)
                        print(f'\033[32mTable {table_name.decode()} and its data deleted. Transaction {tx_id} committed.\033[0m')

                    else:
                        raise Exception("Deletion from schema file failed!")

                else:
                    print(f'\033[31mthere is no table {table_name} in the schema file\033[0m')

            except Exception as e:
                print(f'\033[31mError: {e}\033[0m')
                if tx_id:
                    transaction_manager.abort(tx_id)
                    print(f'\033[31mTransaction {tx_id} aborted.\033[0m')
            finally:
                choice = input(PROMPT_STR)


        elif choice == '3':  # view the table structure and all the data

            if len(schemaObj.get_table_name_list()) > 0:
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
            else:
                print(f'\033[31mThere is no table in the schema file\033[0m')

            choice = input(PROMPT_STR)


        elif choice == '4':  # delete all the table structures and their data
            tx_id = None
            try:
                tx_id = transaction_manager.begin_transaction()
                table_name_list = schemaObj.get_table_name_list()
                for table_name in table_name_list:
                    table_name = table_name.strip()
                    if table_name:
                        stObj = storage_db.Storage(table_name, log_manager)
                        stObj.delete_table_data(table_name)
                        del stObj
                schemaObj.deleteAll() # 模式修改
                transaction_manager.commit(tx_id)
                print(f'\033[32mAll tables deleted. Transaction {tx_id} committed.\033[0m')
            except Exception as e:
                print(f'\033[31mError: {e}\033[0m')
                if tx_id:
                    transaction_manager.abort(tx_id)
                    print(f'\033[31mTransaction {tx_id} aborted.\033[0m')
            finally:
                choice = input(PROMPT_STR)

        elif choice == '5':  # process SELECT FROM WHERE clause
            print('#        Your Query is to SQL QUERY                  #')
            sql_str = input(f'\033[34mplease enter the select from where clause:\033[0m')

            ''' 
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
            '''
            lex_db.set_lex_handle()
            parser_db.set_handle()
            common_db.global_syn_tree = None
            common_db.global_logical_tree = None
            query_plan_db.common_db.global_syn_tree = None  # 确保引用正确

            try:
                print("stripped_str:"+sql_str.strip())
                lex_db.tokenize_sql(sql_str.strip())  # 先打印token流

                # 解析SQL构建语法树
                common_db.global_syn_tree = common_db.global_parser.parse(
                    sql_str.strip(), lexer=common_db.global_lexer
                )

                # 打印语法树
                print("\nSyntax Tree:")
                common_db.show(common_db.global_syn_tree)

                # 构建查询计划
                print("\nBuilding Query Plan...")
                query_plan_db.construct_logical_tree()

                # 执行查询计划
                print("\nExecuting Query...")
                query_plan_db.execute_logical_tree()

            except Exception as e:
                print(f'Error processing SQL: {str(e)}')

            print('#----------------------------------------------------#')
            choice = input(PROMPT_STR)

        elif choice == '6':  # delete a line of data from the storage file given the keyword
            tx_id = None
            try:
                schemaObj.viewTableNames()
                table_name = input(f'\033[34mplease input the name of the table to be deleted from:\033[0m')
                if isinstance(table_name,str):
                    table_name=table_name.encode('utf-8')
                
                if table_name.strip() and schemaObj.find_table(table_name.strip()):
                    dataObj = storage_db.Storage(table_name, log_manager)
                    dataObj.show_table_data()

                    field_keyword = input(f'\033[34mplease input the field name and the corresponding keyword (\033[33mfieldname:keyword\033[34m):\033[0m')
                    
                    tx_id = transaction_manager.begin_transaction()
                    if dataObj.delete_record(field_keyword, tx_id):
                        transaction_manager.commit(tx_id)
                        print(f'\033[32mDelete record success! Transaction {tx_id} committed.\033[0m')
                        dataObj.show_table_data()
                    else:
                        raise Exception("Record not found or delete failed.")
                    del dataObj
                else:
                    print(f'\033[33mTable name is None or does not exist\033[0m')
            except Exception as e:
                print(f'\033[31mError: {e}\033[0m')
                if tx_id:
                    transaction_manager.abort(tx_id)
                    print(f'\033[31mTransaction {tx_id} aborted.\033[0m')
            finally:
                choice = input(PROMPT_STR)


        elif choice == '7':  # update a line of data given the keyword
            tx_id = None
            try:
                schemaObj.viewTableNames()
                table_name = input(f'\033[34mplease input the name of the table:\033[0m')
                if isinstance(table_name, str):
                    table_name = table_name.encode('utf-8').strip()

                if table_name and schemaObj.find_table(table_name):
                    dataObj = storage_db.Storage(table_name, log_manager)
                    dataObj.show_table_data()
                    field_keyword = input(f'\033[34mplease input the field name and the corresponding keyword (\033[33mfieldname:keyword\033[34m):\033[0m')
                    
                    tx_id = transaction_manager.begin_transaction()
                    # 1. 执行删除
                    if not dataObj.delete_record(field_keyword, tx_id):
                        raise Exception("Old record not found for update.")
                    print(f'\033[32mOld record marked for deletion in transaction {tx_id}.\033[0m')

                    # 2. 执行插入
                    record = []
                    Field_List = dataObj.getFieldList()
                    print(f'\033[34mPlease input the new record data:\033[0m')
                    for x in Field_List:
                        s = f'Input field name is: {x[0].strip()}  field type is: {x[1]} field maximum length is: {x[2]}\n'
                        record.append(input(s))

                    if not dataObj.insert_record(record, tx_id):
                        raise Exception("New record insertion failed.")
                    
                    # 3. 提交整个事务
                    transaction_manager.commit(tx_id)
                    print(f'\033[32mUpdate success! Transaction {tx_id} committed.\033[0m')
                    dataObj.show_table_data()
                    del dataObj
                else:
                    print(f'\033[33mTable name is None or does not exist\033[0m')
            except Exception as e:
                print(f'\033[31mError: {e}\033[0m')
                if tx_id:
                    transaction_manager.abort(tx_id)
                    print(f'\033[31mTransaction {tx_id} aborted.\033[0m')
            finally:
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
