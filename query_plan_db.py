# ------------------------------------------------
# query_plan_db.py
# author: Jingyu Han  hjymail@163.com
# modified by:Shuting Guo shutingnjupt@gmail.com
# ------------------------------------------------


# ----------------------------------------------------------
# this module can turn a syntax tree into a query plan tree
# ----------------------------------------------------------

import common_db
import storage_db
import itertools

# --------------------------------
# to import the syntax tree, which is defined in parser_db.py
# -------------------------------------------
from common_db import global_syn_tree as syn_tree


class parseNode:
    def __init__(self):
        self.sel_list = []
        self.from_list = []
        self.where_list = []

    def get_sel_list(self):
        return self.sel_list

    def get_from_list(self):
        return self.from_list

    def get_where_list(self):
        return self.where_list

    def update_sel_list(self, self_list):
        self.sel_list = self_list

    def update_from_list(self, from_list):
        self.from_list = from_list

    def update_where_list(self, where_list):
        self.where_list = where_list


# --------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# to extract data from gloal variable syn_tree
# output:
#       sel_list
#       from_list
#       where_list
# --------------------------------
# Has Been Modified.
def extract_sfw_data():
    syn_tree = common_db.global_syn_tree
    print('extract_sfw_data begins to execute')
    if syn_tree is None:
        print('wrong')
        return [], [], []

    # common_db.show(syn_tree)
    PN = parseNode()
    destruct(syn_tree, PN)

    # 将where_list转换为更结构化的形式
    structured_where = []

    def parse_condition(condition):
        if condition.value == 'Expression':
            # 格式: [字段名, 运算符, 值]
            return [
                condition.children[0].children[0],  # 字段名
                condition.children[1].children[0],  # 运算符
                condition.children[2].children[0]  # 值
            ]
        elif condition.value == 'AND':
            # 处理AND连接的条件
            left = parse_condition(condition.children[0])
            right = parse_condition(condition.children[1])
            return [left, right]
        return condition.value

    structured_where = [parse_condition(cond) for cond in PN.where_list] if PN.where_list else []

    # 处理SELECT * 的情况
    if PN.sel_list == ['*']:
        PN.sel_list = []  # 表示选择所有字段

    return PN.get_sel_list(), PN.get_from_list(), structured_where


# ---------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# Query  : SFW
#   SFW  : SELECT SelList FROM FromList WHERE Condition
# SelList: TCNAME COMMA SelList
# SelList: TCNAME
#
# FromList:TCNAME COMMA FromList
# FromList:TCNAME
# Condition: TCNAME EQX CONSTANT
# ---------------------------------

def destruct(nodeobj, PN):
    if isinstance(nodeobj, common_db.Node):  # it is a Node object
        if nodeobj.children:
            if nodeobj.value == 'SelList':
                tmpList = []
                show(nodeobj, tmpList)
                PN.update_sel_list(tmpList)
            elif nodeobj.value == 'FromList':
                tmpList = []
                show(nodeobj, tmpList)
                PN.update_from_list(tmpList)
            elif nodeobj.value == 'Cond':
                tmpList = []
                show(nodeobj, tmpList)
                PN.update_where_list(tmpList)
            else:
                for i in range(len(nodeobj.children)):
                    destruct(nodeobj.children[i], PN)


def show(nodeobj, tmpList):
    if isinstance(nodeobj, common_db.Node):
        if not nodeobj.children:
            tmpList.append(nodeobj.value)
        else:
            for i in range(len(nodeobj.children)):
                show(nodeobj.children[i], tmpList)
    if isinstance(nodeobj, str):
        tmpList.append(nodeobj)


# ---------------------------
# input:
#       from_list
# output:
#       a tree
# -----------------------------------

def construct_from_node(from_list):
    if from_list:
        if len(from_list) == 1:
            temp_node = common_db.Node(from_list[0], None)
            return common_db.Node('X', [temp_node])
        elif len(from_list) == 2:
            temp_node_first = common_db.Node(from_list[0], None)
            temp_node_second = common_db.Node(from_list[1], None)

            return common_db.Node('X', [temp_node_first, temp_node_second])

        elif len(from_list) > 2:

            right_node = common_db.Node(from_list[len(from_list) - 1], None)

            return common_db.Node('X', [construct_from_node(from_list[0:len(from_list) - 1]), right_node])


# ---------------------------
# input:
#       where_list
#       from_node
# output:
#       a tree
# -----------------------------------
def construct_where_node(from_node, where_list):
    if from_node and len(where_list) > 0:
        return common_db.Node('Filter', [from_node], where_list)
    elif from_node and len(where_list) == 0:  # there is no where clause
        return from_node


# ---------------------------
# input:
#       sel_list
#       wf_node
# output:
#       a tree
# -----------------------------------
def construct_select_node(wf_node, sel_list):
    if wf_node and len(sel_list) > 0:
        return common_db.Node('Proj', [wf_node], sel_list)


# ----------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# to execute the query plan and return the result
# input
#       global logical tree
# ---------------------------------------------
# Has been modified.
def execute_logical_tree():
    
    if not common_db.global_logical_tree:
        print('No logical tree to execute')
        return

    # 获取查询计划信息
    projection_fields = []
    filter_conditions = []
    tables_to_scan = []

    def extract_plan_info(node):
        nonlocal projection_fields, filter_conditions, tables_to_scan
        if node.value == 'Project':
            # print("1")
            projection_fields = node.var if node.var else []

        elif node.value == 'Filter':
            # print("2")
            filter_conditions = node.var

        elif node.value == 'TableScan':
            #　print("3")
            if node.children and node.children[0].value == 'TableName':
                # 访问TableName节点存储的实际表名
                tables_to_scan.append(node.children[0].var)
            else:
                # 兼容旧格式
                tables_to_scan.append(node.children[0].value)

        for child in node.children:
            extract_plan_info(child)

    extract_plan_info(common_db.global_logical_tree)

    # 执行查询
    try:
        import tool
        # 加载表数据（单表查询）
        table_name = tables_to_scan[0]
        storage = storage_db.Storage(table_name.encode('utf-8'))

        # 获取字段列表
        all_fields = [tool.tryToStr(field[0]) for field in storage.field_name_list]
        field_types = {tool.tryToStr(field[0]): field[1] for field in storage.field_name_list}

        # 确定投影字段
        if not projection_fields:  # 对应SELECT *
            projection_fields = all_fields
        else:
            # 验证字段是否存在
            '''print("projection_fields:")
            for tmp in projection_fields:
                print(tmp + "|")
            print("all_fields:")
            for tmp in all_fields:
                print(tmp + "|")'''

            for field in projection_fields:
                if field not in all_fields:
                    raise Exception(f"Field '{field}' does not exist in table '{table_name}'")

        # 加载所有记录
        records = storage.record_list

        # 应用过滤条件
        def evaluate_condition(record, condition):
            if isinstance(condition, list) and len(condition) == 3:
                # 基本条件: [字段, 运算符, 值]
                field_name, operator, value = condition
                # print("In evaluate_condition():"+field_name+'|'+operator+'|'+value)
                # 获取字段索引
                if field_name not in field_types:
                    print(f"Field name '{field_name}' not found.")
                    return False

                # 获取记录中的值
                field_value = record[all_fields.index(field_name)]

                # 类型转换
                field_type = field_types[field_name]
                if field_type == 2:  # 整数类型
                    try:
                        value = int(value)
                    except:
                        pass
                elif field_type == 0 or field_type == 1:  # 字符串
                    try:
                        # value = tool.tryToStr(value)
                        field_value = tool.tryToStr(field_value)
                    except:
                        pass
                elif field_type == 3:  # bool
                    #print()
                    pass

                # print(field_type,value,field_value)
                print(type(field_value) , type(value))
                # 应用操作符
                if operator == '=':
                    return field_value == value
                elif operator == '>':
                    return field_value > value
                elif operator == '<':
                    return field_value < value
                elif operator == '>=':
                    return field_value >= value
                elif operator == '<=':
                    return field_value <= value
                elif operator == '!=':
                    return field_value != value
                else:
                    return False

            elif isinstance(condition, list) and len(condition) == 2:
                # AND连接的条件
                return evaluate_condition(record, condition[0]) and evaluate_condition(record, condition[1])

            else:
                return False

        # 过滤记录
        if filter_conditions:
            print("records:")
            for record in records:
                print(record.__str__()+"|")

            # print(type(filter_conditions))
            # print(type(filter_conditions[0]))
            # print(filter_conditions)
            # for cond in filter_conditions[0]:
            #    print(cond.__str__()+"|")
            if isinstance(filter_conditions[0][0], list):  # 如果查询中有AND，那这东西就是list of list of lists，否则是list of list
                filter_conditions = filter_conditions[0]  # 转化成list of list,len为1，0号内部多个元素
            records = [record for record in records if evaluate_condition(record, filter_conditions[0])]
            print("records under conditions")
            for record in records:
                print(record.__str__() + "|")

        # 应用投影
        result = []
        for record in records:
            if isinstance(record, tuple):
                # 将元组转换为字典以便字段访问
                record_dict = dict(zip(all_fields, record))
                result.append({field: record_dict[field] for field in projection_fields})
            else:
                result.append({field: record.get(field) for field in projection_fields})

        # 显示结果
        if not result:
            print("No results found.")
            return

        # 打印表头
        headers = list(result[0].keys())
        header_row = "| " + " | ".join(f"{header:<15}" for header in headers) + " |"
        separator = "-" * len(header_row)

        print(separator)
        print(header_row)
        print(separator)

        # 打印数据行
        for row in result:
            values = [str(row[field]) for field in headers]
            print("| " + " | ".join(f"{value:<15}" for value in values) + " |")

        print(separator)
        print(f"{len(result)} rows returned")

    except Exception as e:
        print(f"Error executing query: {str(e)}")

# --------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# to construct a logical query plan tree
# output:
#       global_logical_tree
# ---------------------------------
# Has Been Modified .
def construct_logical_tree():

    syn_tree = common_db.global_syn_tree
    if syn_tree:
        try:
            sel_list, from_list, where_list = extract_sfw_data()

            # 清理列表
            sel_list = [item for item in sel_list if item != ',']
            from_list = [item for item in from_list if item != ',']

            # 构建FROM子树 - 使用TableScan代替X
            if from_list:
                if len(from_list) == 1:
                    table_node = common_db.Node('TableName', [], from_list[0])
                    from_node = common_db.Node('TableScan', [table_node])
                else:
                    from_node = common_db.Node('Join', [])
                    for table in from_list:
                        table_node = common_db.Node('TableName', [], table)
                        from_node.children.append(common_db.Node('TableScan', [table_node]))
            else:
                from_node = common_db.Node('Empty', [])

            # 添加WHERE过滤
            if where_list:
                print("Where_list" )
                print(where_list)
                where_node = common_db.Node('Filter', [from_node], where_list)
            else:
                where_node = from_node

            # 添加SELECT投影
            if sel_list:
                common_db.global_logical_tree = common_db.Node('Project', [where_node], sel_list)
            else:
                common_db.global_logical_tree = where_node

            # 打印查询计划
            print("Generated Query Plan:")
            common_db.show(common_db.global_logical_tree, 0, '')

        except Exception as e:
            print(f"Error constructing logical tree: {str(e)}")
            common_db.global_logical_tree = None
    else:
        print('No syntax tree available to construct logical tree')
        common_db.global_logical_tree = None


# --------------------------------
# 增强的解析辅助函数
# --------------------------------
def destruct(nodeobj, PN):
    if isinstance(nodeobj, common_db.Node):
        if nodeobj.children:
            if nodeobj.value == 'SelList':
                # 收集SELECT字段或通配符
                tmpList = []
                extract_children(nodeobj, tmpList)
                PN.update_sel_list(tmpList)

            elif nodeobj.value == 'FromList':
                # 收集FROM表
                tmpList = []
                extract_children(nodeobj, tmpList)
                PN.update_from_list(tmpList)

            elif nodeobj.value == 'Cond' or nodeobj.value == 'Expression' or nodeobj.value == 'AND':
                # 收集WHERE条件
                PN.update_where_list([nodeobj])

            else:
                for child in nodeobj.children:
                    destruct(child, PN)


def extract_children(nodeobj, result_list):
    """递归提取所有子节点的值"""
    if isinstance(nodeobj, common_db.Node):
        if not nodeobj.children:
            if nodeobj.value != ',':
                result_list.append(nodeobj.value)
        else:
            for child in nodeobj.children:
                extract_children(child, result_list)
    elif isinstance(nodeobj, str):
        if nodeobj != ',':
            result_list.append(nodeobj)

