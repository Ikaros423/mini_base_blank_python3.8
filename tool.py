#-----------------------------------------------
# tool.py
# author: Chenhui Zhang  zch13773107361@163.com
#-----------------------------------------------
# 工具函数文件，提供了:
# 1. 字符串和字节串转换函数 (tryToBytes, tryToStr)
#-------------------------------------------

# ------------------------------------------------
# 将传入参数转换为bytes类型
# :param value: 任意类型的参数
# :return: 如果是str则转换为bytes，否则返回原值
# ------------------------------------------------
def tryToBytes(value):
    if isinstance(value, str):
        return value.encode('utf-8').strip()
    elif isinstance(value, bytes):
        return value.strip()
    return value

# ------------------------------------------------
# 将传入参数转换为str类型
# :param value: 任意类型的参数
# :return: 如果是bytes则解码为str，否则转换为str
# ------------------------------------------------
def tryToStr(value):
    if isinstance(value, bytes):
        return value.decode('utf-8').strip()
    return str(value).strip()

# ------------------------------------------------
# 将第二个参数转换为第一个参数的类型
# :param target: 目标对象(用于获取目标类型)
# :param value: 需要转换的值
# :return: 转换后的值
# ------------------------------------------------
def convertType(target, value):
    if isinstance(target, bytes):
        return tryToBytes(value)
    elif isinstance(target, str):
        return tryToStr(value)
    else:
        try:
            return type(target)(value)
        except (ValueError, TypeError):
            print(f'\033[31m无法将 {value} 转换为 {type(target)} 类型\033[0m')
            return value

if __name__ == '__main__':
    # 测试代码
    print(tryToBytes("Hello World"))  # b'Hello World'
    print(tryToStr(b'Hello World'))    # 'Hello World'
    print(f'value:{convertType(123, "456")},type:{type(convertType(123, "456"))}')      # 456
    print(f'value:{convertType("str", b"test")},type:{type(convertType("str", b"test"))}')   # 'test'
    print(f'value:{convertType(b"bytes", "test")},type:{type(convertType(b"bytes", "test"))}') # b'test'
    print(f'value:{convertType(0.0, "3.14")},type:{type(convertType(0.0, "3.14"))}')      # 3.14
    print(f'value:{convertType(True, "False")},type:{type(convertType(True, "False"))}')      # True
    print(f'value:{convertType("True", False)},type:{type(convertType("True", False))}')      # False
    print(f'value:{convertType(False, "")},type:{type(convertType(False, ""))}')      # False