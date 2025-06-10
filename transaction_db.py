# -----------------------------------------------------------------------
# transaction_db.py
# Author: Chenhui Zhang zch13773107361@163.com
# -----------------------------------------------------------------------
# 事务管理模块 (Transaction Manager)
# 负责管理事务的整个生命周期，包括：
# 1. 创建唯一的事务ID
# 2. 维护活动事务列表
# 3. 提供 begin, commit, abort 接口
# 4. 协调 LogManager 写入事务边界记录 (BEGIN, COMMIT, ABORT)
# -----------------------------------------------------------------------

import time
import threading
import log_db 

# -----------------------
# 初始化事务管理器
#
# :param log_manager: 一个 LogManager 的实例，用于写入日志
# -----------------------
class TransactionManager:
    def __init__(self, log_manager):
        if not isinstance(log_manager, log_db.LogManager):
            raise TypeError("log_manager must be an instance of LogManager")
        
        self.log_manager = log_manager
        self.active_transactions = set()
        
        # 使用线程锁来保证事务ID生成的原子性
        self._lock = threading.Lock()
        
        # 初始化事务ID,使用当前时间戳,确保每次程序重启时ID都是唯一的
        self.next_transaction_id = int(time.time())
        
        print("TransactionManager initialized.")

    # -----------------------
    # 生成一个唯一的、单调递增的事务ID
    # -----------------------
    def _generate_tx_id(self):
        with self._lock:
            self.next_transaction_id += 1
            return self.next_transaction_id

    # -----------------------
    # 开始一个新的事务
    #
    # :return: 新创建的事务ID
    # -----------------------
    def begin_transaction(self):
        # 1. 生成一个新的事务ID
        transaction_id = self._generate_tx_id()
        
        # 2. 将新事务加入活动事务列表
        self.active_transactions.add(transaction_id)
        
        # 3. 记录 BEGIN 日志
        self.log_manager.log(transaction_id, log_db.RECORD_TYPE_BEGIN)
        
        print(f"[TX Manager] Transaction {transaction_id} started.")
        return transaction_id

    # -----------------------
    # 提交一个事务
    # 
    # :param transaction_id: 要提交的事务ID
    # -----------------------
    def commit(self, transaction_id):
        if transaction_id not in self.active_transactions:
            print(f"[TX Manager] Warning: Attempted to commit a non-active transaction {transaction_id}.")
            return

        # 1. 记录 COMMIT 日志
        self.log_manager.log(transaction_id, log_db.RECORD_TYPE_COMMIT)
        
        # 2. 从活动事务列表中移除
        self.active_transactions.remove(transaction_id)
        
        print(f"[TX Manager] Transaction {transaction_id} committed.")

    # -----------------------
    # 中止（回滚）一个事务
    # 
    # :param transaction_id: 要中止的事务ID
    # -----------------------
    def abort(self, transaction_id):
        if transaction_id not in self.active_transactions:
            print(f"[TX Manager] Warning: Attempted to abort a non-active transaction {transaction_id}.")
            return

        # 1. 记录 ABORT 日志，告知恢复系统，这个事务的所有修改都应该被撤销
        self.log_manager.log(transaction_id, log_db.RECORD_TYPE_ABORT)
        
        # 2. 从活动事务列表中移除
        self.active_transactions.remove(transaction_id)
        
        print(f"[TX Manager] Transaction {transaction_id} aborted.")