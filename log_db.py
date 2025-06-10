# -----------------------------------------------------------------------
# log_db.py
# Author: Chenhui Zhang zch13773107361@163.com
# -----------------------------------------------------------------------
# 日志管理模块 (Log Manager)
# 负责实现预写日志 (Write-Ahead Logging, WAL) 机制，保障事务的原子性和持久性
#
# 核心功能:
# 1. 定义和管理统一的事务日志文件 (transaction.log)
# 2. 提供写入不同类型日志记录 (BEGIN, COMMIT, INSERT, UPDATE等) 的接口
# 3. 实现系统崩溃后的恢复逻辑 (recover)，包括分析(Analysis)、重做(Redo)和撤销(Undo)阶段
# -----------------------------------------------------------------------

import os
import struct
from common_db import BLOCK_SIZE 
import storage_db 

# --- 日志记录类型常量 ---
RECORD_TYPE_BEGIN = 0
RECORD_TYPE_COMMIT = 1
RECORD_TYPE_ABORT = 2
RECORD_TYPE_INSERT = 3
RECORD_TYPE_DELETE = 4 

# --- 日志记录头部格式 ---
# <Record_Length (I)> <Transaction_ID (Q)> <Record_Type (B)>
# I: unsigned int (4 bytes) - 整条记录的长度
# Q: unsigned long long (8 bytes) - 事务ID
# B: unsigned char (1 byte) - 记录类型
LOG_HEADER_FORMAT = '!IQB'
LOG_HEADER_SIZE = struct.calcsize(LOG_HEADER_FORMAT)

# -----------------------
#   管理事务日志的读写和恢复
# -----------------------
class LogManager:

    # -----------------------
    #   初始化日志管理器，打开或创建日志文件
    # -----------------------
    def __init__(self, log_file_name="transaction.log"):
        self.log_file_name = log_file_name
        # 以二进制追加读写模式打开文件
        self.log_file = open(self.log_file_name, 'ab+')
        print(f"LogManager initialized. Log file: '{self.log_file_name}'")

    # -----------------------
    # 构造一条日志记录并将其持久化到磁盘
    # input:
    #    :param transaction_id: 当前事务的ID
    #    :param record_type: 日志记录的类型 (e.g., RECORD_TYPE_BEGIN)
    #    :param payload: 日志的具体内容 (二进制数据)
    # -----------------------
    def log(self, transaction_id, record_type, payload=b''):
        # 1. 构造日志记录
        record_length = LOG_HEADER_SIZE + len(payload)
        header = struct.pack(LOG_HEADER_FORMAT, record_length, transaction_id, record_type)
        log_record = header + payload

        # 2. 写入日志文件
        self.log_file.write(log_record)

        # 3. 强制刷盘 (关键步骤)
        # 确保日志从操作系统缓存写入物理磁盘，实现持久性
        self.log_file.flush()
        os.fsync(self.log_file.fileno())

    # -----------------------
    # 执行恢复算法，在系统启动时调用
    # 分为三个阶段：Analysis, Redo, Undo
    # -----------------------
    def recover(self):
        print("--- Starting Recovery Process ---")
        self.log_file.seek(0)
        log_data = self.log_file.read()

        # --- 1. 分析阶段 (Analysis) ---
        print("[Recovery] Phase 1: Analysis...")
        committed_transactions = set()
        active_transactions = set()
        pos = 0
        while pos < len(log_data):
            # 解析头部
            record_length, tx_id, record_type = struct.unpack_from(LOG_HEADER_FORMAT, log_data, pos)
            
            if record_type == RECORD_TYPE_BEGIN:
                active_transactions.add(tx_id)
            elif record_type == RECORD_TYPE_COMMIT:
                active_transactions.remove(tx_id)
                committed_transactions.add(tx_id)
            elif record_type == RECORD_TYPE_ABORT:
                active_transactions.remove(tx_id)
            
            # 移动到下一条记录
            pos += record_length

        print(f"[Analysis] Committed Transactions: {committed_transactions}")
        print(f"[Analysis] Unfinished (Active) Transactions to be undone: {active_transactions}")

        # --- 2. 重做阶段 (Redo) ---
        print("\n[Recovery] Phase 2: Redo...")
        pos = 0
        while pos < len(log_data):
            record_length, tx_id, record_type = struct.unpack_from(LOG_HEADER_FORMAT, log_data, pos)
            payload_pos = pos + LOG_HEADER_SIZE
            payload = log_data[payload_pos : pos + record_length]

            if tx_id in committed_transactions:
                # 只对已提交事务的修改操作进行重做
                if record_type in [RECORD_TYPE_INSERT, RECORD_TYPE_DELETE]:
                    print(f"  Redoing operation from log for committed transaction {tx_id}...")
                    self._redo_op(record_type, payload)

            pos += record_length
        print("[Redo] Phase completed.")

        # --- 3. 撤销阶段 (Undo) ---
        print("\n[Recovery] Phase 3: Undo...")
        undo_records = []
        pos = 0
        while pos < len(log_data):
             record_length, tx_id, record_type = struct.unpack_from(LOG_HEADER_FORMAT, log_data, pos)
             if tx_id in active_transactions:
                 undo_records.append((record_length, tx_id, record_type, pos))
             pos += record_length

        # 从后往前遍历需要撤销的记录
        for record in reversed(undo_records):
            record_length, tx_id, record_type, pos = record
            payload_pos = pos + LOG_HEADER_SIZE
            payload = log_data[payload_pos : pos + record_length]
            
            print(f"  Undoing operation from log for active transaction {tx_id}...")
            self._undo_op(record_type, payload)
        
        # 为所有被中止的事务写入一条ABORT记录，以便日志保持干净
        for tx_id in active_transactions:
            self.log(tx_id, RECORD_TYPE_ABORT)

        print("[Undo] Phase completed.")
        print("--- Recovery Process Finished ---")

    # -----------------------
    # 一个辅助函数，用于从payload中解析出通用信息
    # -----------------------
    def _parse_payload(self, payload):
        try:
            # 解析表名
            table_name_len, = struct.unpack_from('!I', payload, 0)
            offset = 4 + table_name_len
            table_name_bytes, = struct.unpack_from(f'!{table_name_len}s', payload, 4)
            table_name = table_name_bytes.decode('utf-8')

            # 解析位置和记录内容
            block_id, slot_id, record_data_len = struct.unpack_from('!III', payload, offset)
            offset += 12
            record_data_bytes, = struct.unpack_from(f'!{record_data_len}s', payload, offset)
            
            return table_name, block_id, slot_id, record_data_bytes
        except struct.error as e:
            print(f"  [Error] Failed to parse payload: {e}")
            return None, None, None, None


    # -----------------------
    # 重做（Redo）：将日志中的操作重新执行一遍，确保数据写入文件。
    # -----------------------
    def _redo_op(self, record_type, payload):
        table_name, block_id, slot_id, record_data_bytes = self._parse_payload(payload)
        if table_name is None:
            return

        storage = storage_db.Storage(table_name.encode('utf-8'))

        if record_type == RECORD_TYPE_INSERT:
            # Redo一个INSERT操作：强制在指定位置插入记录
            print(f"  REDO INSERT on table '{table_name}' at Block {block_id}, Slot {slot_id}")
            storage._force_insert_at(block_id, slot_id, record_data_bytes)

        elif record_type == RECORD_TYPE_DELETE:
            # Redo一个DELETE操作：强制在指定位置设置删除标记
            print(f"  REDO DELETE on table '{table_name}' at Block {block_id}, Slot {slot_id}")
            storage._force_delete_at(block_id, slot_id)


    # -----------------------
    # 撤销（Undo）：执行与日志记录相反的操作，回滚未提交的修改。
    # -----------------------
    def _undo_op(self, record_type, payload):
        table_name, block_id, slot_id, record_data_bytes = self._parse_payload(payload)
        if table_name is None:
            return

        storage = storage_db.Storage(table_name.encode('utf-8'))

        if record_type == RECORD_TYPE_INSERT:
            # Undo一个INSERT操作：删除它
            print(f"  UNDO INSERT on table '{table_name}' by deleting at Block {block_id}, Slot {slot_id}")
            storage._force_delete_at(block_id, slot_id)

        elif record_type == RECORD_TYPE_DELETE:
            # --- 关键修改 ---
            # Undo一个DELETE操作：调用新的 undelete 方法，恢复删除标记位
            print(f"  UNDO DELETE on table '{table_name}' by restoring flag at Block {block_id}, Slot {slot_id}")
            
            # 将 record_data_bytes 解码回元组，以便更新 storage 对象的内存列表
            try:
                record_str = record_data_bytes.decode('utf-8')
                original_tuple = tuple(record_str.split(','))
            except:
                original_tuple = None # 如果解析失败，传入None

            storage._force_undelete_at(block_id, slot_id, original_tuple)


    # -----------------------
    # 确保在对象销毁时关闭文件
    # -----------------------
    def __del__(self):
        if self.log_file:
            self.log_file.close()