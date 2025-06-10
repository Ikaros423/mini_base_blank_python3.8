# -----------------------------------------------------------------------
# storage_db.py
# Author: Jingyu Han  hjymail@163.com
# modified by: Chenhui Zhang zch13773103716@163.com
# -----------------------------------------------------------------------
# the module is to store tables in files
# Each table is stored in a separate file with the suffix ".dat".
# For example, the table named moviestar is stored in file moviestar.dat 
# -----------------------------------------------------------------------

# struct of file is as follows, each block is 4096
# ---------------------------------------------------
# block_0|block_1|...|block_n
# ----------------------------------------------------------------
from common_db import BLOCK_SIZE

# structure of block_0, which stores the meta information and field information
# ---------------------------------------------------------------------------------
# block_id                                # 0
# number_of_dat_blocks                    # at first it is 0 because there is no data in the table
# number_of_fields or number_of_records   # the total number of fields for the table
# -----------------------------------------------------------------------------------------


# the data type is as follows
# ----------------------------------------------------------
# 0->str,1->varstr,2->int,3->bool
# ---------------------------------------------------------------


# structure of data block, whose block id begins with 1
# ----------------------------------------
# block_id       
# number of records
# record_0_offset         # it is a pointer to the data of record
# record_1_offset
# ...
# record_n_offset
# ....
# free space
# ...
# record_n
# ...
# record_1
# record_0
# -------------------------------------------

# structre of one record
# -----------------------------
# pointer                     #offset of table schema in block id 0
# length of record            # including record head and record content
# time stamp of last update  # for example,1999-08-22
# is_deleted flag         # 0 means not deleted, 1 means deleted
# field_0_value
# field_1_value
# ...
# field_n_value
# -------------------------


import struct
import os
import ctypes
import tool
import log_db


# --------------------------------------------
# the class can store table data into files
# functions include insert, delete and update
# --------------------------------------------

class Storage(object):

    # ------------------------------
    # constructor of the class
    # input:
    #       tablename
    #       log_manager: the log manager to record the log
    # -------------------------------------
    def __init__(self, tablename, log_manager=None):
        # print "__init__ of ",Storage.__name__,"begins to execute"
        self.tableName = tool.tryToBytes(tablename)
        self.log_manager = log_manager  # 保存日志管理器的引用

        self.record_list = []
        self.record_Position = []
        self.delete_flag_list= []  # to judge whether the record is deleted or not

        if not os.path.exists(tablename + '.dat'.encode('utf-8')):  # the file corresponding to the table does not exist
            print('table file '.encode('utf-8') + tablename + '.dat does not exists'.encode('utf-8'))
            self.f_handle = open(tablename + '.dat'.encode('utf-8'), 'wb+')
            self.f_handle.close()
            self.open = False
            print(tablename + '.dat has been created'.encode('utf-8'))

        self.f_handle = open(tablename + '.dat'.encode('utf-8'), 'rb+')
        print('table file '.encode('utf-8') + tablename + '.dat has been opened'.encode('utf-8'))
        self.open = True

        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        self.f_handle.seek(0)
        self.dir_buf = self.f_handle.read(BLOCK_SIZE)

        self.dir_buf.strip()
        my_len = len(self.dir_buf)
        self.field_name_list = []
        beginIndex = 0

        if my_len == 0:  # there is no data in the block 0, we should write meta data into the block 0
            tablename = tool.tryToStr(tablename)  # ensure tablename is a string
            self.num_of_fields = input( "please input the number of fields in table " + tablename + ":")

            if int(self.num_of_fields) > 0:

                self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
                self.block_id = 0
                self.data_block_num = 0
                struct.pack_into('!iii', self.dir_buf, beginIndex, 0, 0,
                                 int(self.num_of_fields))  # block_id,number_of_data_blocks,number_of_fields

                beginIndex = beginIndex + struct.calcsize('!iii')

                # the following is to write the field name,field type and field length into the buffer in turn
                for i in range(int(self.num_of_fields)):
                    field_name = input("please input the name of field " + str(i) + " :")

                    if len(field_name) < 10:
                        field_name = ' ' * (10 - len(field_name.strip())) + field_name

                    while True:
                        field_type = input(
                            "please input the type of field(0-> str; 1-> varstr; 2-> int; 3-> boolean) " + str(
                                i) + " :")
                        if int(field_type) in [0, 1, 2, 3]:
                            break

                    # to need further modification here
                    field_length = input("please input the length of field " + str(i) + " :")
                    temp_tuple = (field_name, int(field_type), int(field_length))
                    self.field_name_list.append(temp_tuple)
                    if isinstance(field_name, str):
                        field_name = field_name.encode('utf-8')

                    struct.pack_into('!10sii', self.dir_buf, beginIndex, field_name, int(field_type),
                                     int(field_length))
                    beginIndex = beginIndex + struct.calcsize('!10sii')

                self.f_handle.seek(0)
                self.f_handle.write(self.dir_buf)
                self.f_handle.flush()

            else:
                print('the number of fields should be greater than 0')
                self.f_handle.close()
                self.open = False
                return

        else:  # there is something in the file

            self.block_id, self.data_block_num, self.num_of_fields = struct.unpack_from('!iii', self.dir_buf, 0)

            print('number of fields is ', self.num_of_fields)
            print('data_block_num', self.data_block_num)
            beginIndex = struct.calcsize('!iii')

            # the followins is to read field name, field type and field length into main memory structures
            for i in range(self.num_of_fields):
                field_name, field_type, field_length = struct.unpack_from('!10sii', self.dir_buf,
                                                                          beginIndex + i * struct.calcsize(
                                                                              '!10sii'))  # i means no memory alignment

                temp_tuple = (field_name, field_type, field_length)
                self.field_name_list.append(temp_tuple)
                print("the " + str(i) + "th field information (field name,field type,field length) is ", temp_tuple)
        # print self.field_name_list
        record_head_len = struct.calcsize('!ii10si')
        record_content_len = sum(map(lambda x: x[2], self.field_name_list))
        # print record_content_len

        Flag = 1
        while Flag <= self.data_block_num:
            self.f_handle.seek(BLOCK_SIZE * Flag)
            self.active_data_buf = self.f_handle.read(BLOCK_SIZE)
            self.block_id, self.Number_of_Records = struct.unpack_from('!ii', self.active_data_buf, 0)
            print('Block_ID=%s,   Contains %s data' % (self.block_id, self.Number_of_Records))
            # There exists record
            if self.Number_of_Records > 0:
                for i in range(self.Number_of_Records):
                    self.record_Position.append((Flag, i))

                    # 获取记录偏移量
                    offset = struct.unpack_from('!i', self.active_data_buf,
                                    struct.calcsize('!ii') + i * struct.calcsize('!i'))[0]
            
                    # 读取记录头部，检查删除标记
                    record_header = struct.unpack_from('!ii10si', self.active_data_buf, offset)
                    is_deleted = record_header[3]  # 第4个字段是删除标记
            
                    if not is_deleted:  # 只处理未删除的记录
                        self.delete_flag_list.append(False)  # 未删除标记为False
                        # 读取记录内容
                        record = struct.unpack_from('!' + str(record_content_len) + 's', 
                                          self.active_data_buf,
                                          offset + record_head_len)[0]
                
                        # 解析记录内容
                        tmp = 0
                        tmpList = []
                        for field in self.field_name_list:
                            t = record[tmp:tmp + field[2]].strip()
                            tmp = tmp + field[2]
                            if field[1] == 2:  # int类型
                                t = int(t)
                            if field[1] == 3:  # bool类型
                                t = bool(t)
                            tmpList.append(t)
                        self.record_list.append(tuple(tmpList))
                    else:
                        self.delete_flag_list.append(True)
                        self.record_list.append(None)  # 已删除的记录用None表示
            Flag += 1

    # ------------------------------
    # return the record list of the table
    # input:
    #       
    # -------------------------------------
    def getRecord(self):
        return self.record_list

    # --------------------------------
    # to insert a record into table
    # param insert_record: list
    # param transaction_id: the id of the transaction
    # return: True or False
    # -------------------------------
    def insert_record(self, insert_record, transaction_id=None):

        # example: ['xuyidan','23','123456']

        # step 1 : to check the insert_record is True or False

        tmpRecord = []
        for idx in range(len(self.field_name_list)):
            insert_record[idx] = insert_record[idx].strip()
            if self.field_name_list[idx][1] == 0 or self.field_name_list[idx][1] == 1:
                if len(insert_record[idx]) > self.field_name_list[idx][2]:
                    return False
                tmpRecord.append(tool.tryToBytes(insert_record[idx])) # convert to bytes
            if self.field_name_list[idx][1] == 2:
                try:
                    tmpRecord.append(int(insert_record[idx]))
                except:
                    return False
            if self.field_name_list[idx][1] == 3:
                try:
                    tmpRecord.append(bool(insert_record[idx]))
                except:
                    return False
            insert_record[idx] = ' ' * (self.field_name_list[idx][2] - len(insert_record[idx])) + insert_record[idx]

        # step2: Add tmpRecord to record_list ; change insert_record into inputstr
        inputstr = ''.join(insert_record)

        # Step3: To calculate MaxNum in each Data Blocks
        record_content_len = len(inputstr)
        record_head_len = struct.calcsize('!ii10si')  # 增加is_deleted字段
        record_len = record_head_len + record_content_len
        MAX_RECORD_NUM = (BLOCK_SIZE - struct.calcsize('!i') - struct.calcsize('!ii')) / (
                record_len + struct.calcsize('!i'))

        # Step4: To calculate new record Position
        if not len(self.record_Position):
            self.data_block_num += 1
            self.record_Position.append((1, 0))
        else:
            last_Position = self.record_Position[-1]
            if last_Position[1] == MAX_RECORD_NUM - 1:
                self.record_Position.append((last_Position[0] + 1, 0))
                self.data_block_num += 1
            else:
                self.record_Position.append((last_Position[0], last_Position[1] + 1))

        last_Position = self.record_Position[-1]

        # Step5: Write the log if log_manager is not None
        if self.log_manager:
            # 构造一个包含足够恢复信息的 payload
            # 格式: <table_name_len> <table_name> <block_id> <slot_id> <record_content>
            table_name_bytes = self.tableName
            record_data_bytes = inputstr.encode('utf-8')
            payload_format = f'!I{len(table_name_bytes)}sIII{len(record_data_bytes)}s'
            payload = struct.pack(
                payload_format,
                len(table_name_bytes),
                table_name_bytes,
                last_Position[0], # block_id
                last_Position[1], # slot_id
                len(record_data_bytes),   # 记录数据长度
                record_data_bytes         # 记录数据
            )
            self.log_manager.log(transaction_id, log_db.RECORD_TYPE_INSERT, payload)

        # Step6: Write new record into file xxx.dat
        # update data_block_num
        self.f_handle.seek(0)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data block head
        self.f_handle.seek(BLOCK_SIZE * last_Position[0])
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, last_Position[0], last_Position[1] + 1)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data offset
        offset = struct.calcsize('!ii') + last_Position[1] * struct.calcsize('!i')
        beginIndex = BLOCK_SIZE - (last_Position[1] + 1) * record_len
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + offset)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!i'))
        struct.pack_into('!i', self.buf, 0, beginIndex)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data
        record_schema_address = struct.calcsize('!iii')
        update_time = '2016-11-16'  # update time
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + beginIndex)
        self.buf = ctypes.create_string_buffer(record_len)
        struct.pack_into('!ii10si', self.buf, 0, record_schema_address, record_content_len, update_time.encode('utf-8'), 0)  # 0表示未删除
        struct.pack_into('!' + str(record_content_len) + 's', self.buf, record_head_len, inputstr.encode('utf-8'))
        self.f_handle.write(self.buf.raw)
        self.f_handle.flush()

        # 将新记录添加到内存列表中
        self.record_list.append(tuple(tmpRecord))
        self.delete_flag_list.append(False)

        return True

    # --------------------------------
    # to delete a record into table
    # prama fieldName_and_keyword: the format is fieldname:keyword
    # prama transaction_id
    # return: True or False
    # -------------------------------
    def delete_record(self, fieldName_and_keyword, transaction_id):
        """删除记录，参数格式为 fieldname:keyword"""
        if ':' not in fieldName_and_keyword:
            print('\033[31m参数格式错误，应为 fieldname:keyword\033[0m')
            return False

        field_name, keyword = fieldName_and_keyword.split(':', 1)
        # 确保字段名是bytes类型
        field_name = tool.tryToBytes(field_name)
        # keyword可以是字符串或数字，转换为字符串
        keyword = tool.tryToStr(keyword)

        # 找到字段索引
        field_idx = -1
        for idx, field in enumerate(self.field_name_list):
            name = tool.convertType(field_name, field[0])
            if name == field_name:
                field_idx = idx
                break

        if field_idx == -1:
            print(f'\033[31m未找到字段名 {field_name.decode()}\033[0m')
            return False

        # 遍历所有记录
        for i, rec in enumerate(self.record_list):
            if rec is None:  continue

            record_value = tool.convertType(keyword, rec[field_idx])
            if record_value == keyword:
                # 获取记录在文件中的位置
                block_id, record_id = self.record_Position[i]

                # 先写日志
                if self.log_manager:
                    # Payload 需要包含前像（被删除的记录内容），以便UNDO
                    # 格式: <table_name_len> <table_name> <block_id> <slot_id> <record_content>
                    table_name_bytes = self.tableName
                    # 将元组转换为可写入的字符串
                    record_content_str = ','.join(map(str, rec))
                    record_data_bytes = record_content_str.encode('utf-8')
                    
                    payload_format = f'!I{len(table_name_bytes)}sIII{len(record_data_bytes)}s'
                    payload = struct.pack(
                        payload_format,
                        len(table_name_bytes),
                        table_name_bytes,
                        block_id,
                        record_id,
                        len(record_data_bytes),  # 记录数据长度
                        record_data_bytes        # 记录数据
                    )
                    self.log_manager.log(transaction_id, log_db.RECORD_TYPE_DELETE, payload)

                # 计算记录头的位置
                offset = struct.unpack_from('!i', self.active_data_buf,
                                          struct.calcsize('!ii') + record_id * struct.calcsize('!i'))[0]
                # 设置删除标记
                self.f_handle.seek(BLOCK_SIZE * block_id + offset + struct.calcsize('!ii10s'))
                self.f_handle.write(struct.pack('!i', 1))  # 1表示已删除
                self.delete_flag_list[i] = True  # 更新内存中的删除标记
                self.record_list[i] = None  # 将记录设置为None，表示已删除
                return True  # 删除成功
           
        print(f'\033[31m未找到匹配的记录\033[0m')
        return False

    # --------------------------------
    # 仅供恢复时使用 (UNDO an INSERT)。
    # 在指定位置强制设置记录的删除标记为1，不记录日志。
    # 注意：此方法不会从内存列表中删除记录，只是标记为已删除。
    # param block_id: 记录所在的块ID。
    # param slot_id: 记录在块内的槽位ID (从0开始)。
    # --------------------------------
    def _force_delete_at(self, block_id, slot_id):
        print(f"  [Storage] Forcing delete at Block {block_id}, Slot {slot_id}")
        try:
            # 1. 计算并定位到目标块
            block_offset = block_id * BLOCK_SIZE
            self.f_handle.seek(block_offset)

            # 2. 将整个块读入内存缓冲区，以便安全修改
            block_buffer = bytearray(self.f_handle.read(BLOCK_SIZE))

            # 3. 从块的槽数组中，找到指向记录数据的偏移量
            slot_array_pos = struct.calcsize('!ii') + slot_id * struct.calcsize('!i')
            record_offset_in_block, = struct.unpack_from('!i', block_buffer, slot_array_pos)

            # 4. 计算删除标记在记录头中的位置
            # 记录头格式: '!ii10si' (ptr, len, timestamp, is_deleted)
            # is_deleted 标记位于 'ii10s'之后
            is_deleted_flag_offset = record_offset_in_block + struct.calcsize('!ii10s')

            # 5. 在内存缓冲区中修改删除标记
            struct.pack_into('!i', block_buffer, is_deleted_flag_offset, 1) # 1 表示已删除

            # 6. 将修改后的整个块写回文件
            self.f_handle.seek(block_offset)
            self.f_handle.write(block_buffer)
            self.f_handle.flush()

            # 7. 更新内存中的状态以保持一致
            # 找到全局记录列表中的位置并更新
            for i, pos in enumerate(self.record_Position):
                if pos == (block_id, slot_id):
                    self.delete_flag_list[i] = True
                    self.record_list[i] = None
                    break
                    
        except (IOError, struct.error) as e:
            print(f"\033[31m  [Storage Error] Failed during _force_delete_at: {e}\033[0m")


    # --------------------------------
    # 仅供恢复时使用 (UNDO a DELETE)。
    # 在指定位置将记录的删除标记从1恢复为0，不记录日志。
    # 注意：此方法不会从内存列表中删除记录，只是标记为未删除。
    # param block_id: 记录所在的块ID。
    # param slot_id: 记录在块内的槽位ID (从0开始)。
    # param original_record_tuple: 从日志中恢复的原始记录元组，用于更新内存。
    # --------------------------------
    def _force_undelete_at(self, block_id, slot_id, original_record_tuple):
        print(f"  [Storage] Forcing undelete at Block {block_id}, Slot {slot_id}")
        try:
            # 1. 计算并定位到目标块
            block_offset = block_id * BLOCK_SIZE
            self.f_handle.seek(block_offset)

            # 2. 将整个块读入内存缓冲区
            block_buffer = bytearray(self.f_handle.read(BLOCK_SIZE))

            # 3. 从槽数组中找到指向记录的偏移量
            slot_array_pos = struct.calcsize('!ii') + slot_id * struct.calcsize('!i')
            record_offset_in_block, = struct.unpack_from('!i', block_buffer, slot_array_pos)

            # 4. 计算删除标记在记录头中的位置
            is_deleted_flag_offset = record_offset_in_block + struct.calcsize('!ii10s')

            # 5. 在内存缓冲区中将删除标记恢复为 0
            struct.pack_into('!i', block_buffer, is_deleted_flag_offset, 0) # 0 表示未删除

            # 6. 将修改后的整个块写回文件
            self.f_handle.seek(block_offset)
            self.f_handle.write(block_buffer)
            self.f_handle.flush()

            # 7. 更新内存状态以保持一致
            for i, pos in enumerate(self.record_Position):
                if pos == (block_id, slot_id):
                    self.delete_flag_list[i] = False
                    self.record_list[i] = original_record_tuple
                    break

        except (IOError, struct.error) as e:
            print(f"\033[31m  [Storage Error] Failed during _force_undelete_at: {e}\033[0m")


    # -----------------------------------------------
    # 仅供恢复时使用 (REDO an INSERT or UNDO a DELETE)。
    # 在指定位置强制写入一条记录的完整内容，不记录日志。
    # 这会覆盖该槽位上任何现有数据。
    #
    # :param block_id: 记录所在的块ID。
    # :param slot_id: 记录在块内的槽位ID (从0开始)。
    # :param record_data_bytes: 记录的完整内容字节串。
    # ----------------------------------------------- 
    def _force_insert_at(self, block_id, slot_id, record_data_bytes):
        print(f"  [Storage] Forcing insert at Block {block_id}, Slot {slot_id}")
        try:
            # 1. 计算记录的总长度 (包括头部)
            record_content_len = len(record_data_bytes)
            record_head_len = struct.calcsize('!ii10si')
            record_len = record_head_len + record_content_len

            # 2. 计算并定位到目标块
            block_offset = block_id * BLOCK_SIZE
            self.f_handle.seek(block_offset)

            # 3. 检查块是否存在，如果不存在则创建一个新的空块
            if block_id > self.data_block_num:
                # 这是一个新块
                block_buffer = bytearray(BLOCK_SIZE)
                # 初始化块头 (block_id, num_records=0)
                struct.pack_into('!ii', block_buffer, 0, block_id, 0)
                # 更新文件元数据中的总块数
                self.data_block_num = block_id
                self.f_handle.seek(struct.calcsize('!i')) # 跳过 block_id=0
                self.f_handle.write(struct.pack('!i', self.data_block_num))
            else:
                # 块已存在，读入缓冲区
                block_buffer = bytearray(self.f_handle.read(BLOCK_SIZE))

            # 4. 计算记录数据在块内的起始偏移量 (从后往前放)
            record_offset_in_block = BLOCK_SIZE - (slot_id + 1) * record_len

            # 5. 在缓冲区中构建完整的记录 (头部 + 内容)
            record_schema_address = struct.calcsize('!iii') 
            update_time = b'1970-01-01' # 恢复时使用一个默认时间戳
            # 写入记录头: ptr, len, timestamp, is_deleted=0
            struct.pack_into('!ii10si', block_buffer, record_offset_in_block,
                            record_schema_address, record_content_len, update_time, 0) # 0 表示未删除
            # 写入记录内容
            struct.pack_into(f'!{record_content_len}s', block_buffer,
                            record_offset_in_block + record_head_len, record_data_bytes)

            # 6. 更新槽数组，写入指向新记录的指针
            slot_array_pos = struct.calcsize('!ii') + slot_id * struct.calcsize('!i')
            struct.pack_into('!i', block_buffer, slot_array_pos, record_offset_in_block)

            # 7. 更新块头中的记录数
            # 假设slot_id是连续的，所以新记录数是 slot_id + 1
            new_record_count = slot_id + 1
            struct.pack_into('!ii', block_buffer, 0, block_id, new_record_count)

            # 8. 将修改后的整个块写回文件
            self.f_handle.seek(block_offset)
            self.f_handle.write(block_buffer)
            self.f_handle.flush()
            
        except (IOError, struct.error) as e:
            print(f"\033[31m  [Storage Error] Failed during _force_insert_at: {e}\033[0m")


    def show_table_data(self):
        print('|    '.join(map(lambda x: x[0].decode('utf-8').strip(), self.field_name_list)))  # show the structure

        # the following is to show the data of the table
        for record, is_deleted in list(zip(self.record_list, self.delete_flag_list)):
            if not is_deleted:
                print(record)

    # --------------------------------
    # to delete  the data file
    # input
    #       table name
    # output
    #       True or False
    # -----------------------------------
    def delete_table_data(self, tableName):

        # step 1: identify whether the file is still open
        if self.open == True:
            self.f_handle.close()
            self.open = False

        # step 2: remove the file from os   
        tableName.strip()
        if os.path.exists(tableName + '.dat'.encode('utf-8')):
            os.remove(tableName + '.dat'.encode('utf-8'))

        return True

    # ------------------------------
    # get the list of field information, each element of which is (field name, field type, field length)
    # input:
    #       
    # -------------------------------------

    def getFieldList(self):
        return self.field_name_list

    # ----------------------------------------
    # destructor
    # ------------------------------------------------
    def __del__(self):  # write the metahead information in head object to file

        if self.open == True:
            self.f_handle.seek(0)
            self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
            struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
            self.f_handle.write(self.buf)
            self.f_handle.flush()
            self.f_handle.close()
