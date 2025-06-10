import os
import log_db
import transaction_db
import storage_db

def test_committed_transaction_survives_crash():
    print("--- Running Test: Committed Transaction ---")
    
    # 1. 初始化管理器
    log_manager = log_db.LogManager()
    transaction_manager = transaction_db.TransactionManager(log_manager)
    
    table_name = b'mytable' 
    storage = storage_db.Storage(table_name, log_manager)

    # 2. 开始事务
    tx_id = transaction_manager.begin_transaction()
    print(f"Transaction {tx_id} started.")

    # 3. 准备并执行插入操作
    new_record = ['value1', '123', 'True'] 
    print(f"Inserting record: {new_record}")
    storage.insert_record(new_record, tx_id)
    
    # 4. 提交事务 
    transaction_manager.commit(tx_id)
    print(f"Transaction {tx_id} committed.")

    # 5. 模拟系统立即崩溃！
    print("--- SIMULATING CRASH ---")
    os._exit(1) # 程序在此处暴力退出

def test_uncommitted_transaction_rolls_back():
    print("--- Running Test: Uncommitted Transaction ---")
    log_manager = log_db.LogManager()
    transaction_manager = transaction_db.TransactionManager(log_manager)
    table_name = b'mytable' 
    storage = storage_db.Storage(table_name, log_manager)

    tx_id = transaction_manager.begin_transaction()
    print(f"Transaction {tx_id} started.")

    new_record = ['value2', '999', 'False']
    print(f"Inserting uncommitted record: {new_record}")
    storage.insert_record(new_record, tx_id) # 日志已写入，数据文件可能也已被修改
    storage.show_table_data()  # 显示当前表数据
    
    # 没有 commit() 调用
    
    print("--- SIMULATING CRASH before commit ---")
    os._exit(1)


if __name__ == '__main__':
    # test_committed_transaction_survives_crash()
    test_uncommitted_transaction_rolls_back()