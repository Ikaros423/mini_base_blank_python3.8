# -------------------------------
# lex_db.py
# author: Jingyu Han hjymail@163.com
# modified by:
# --------------------------------------------
# the module is responsible for
# (1) defining tokens used for parsing SQL statements
# (2) constructing a lex object
# -------------------------------
import ply.lex as lex
import common_db

tokens = ('SELECT', 'FROM', 'WHERE', 'AND', 'BOOL', 'TCNAME', 'EQX',
          'COMMA', 'CONSTANT', 'WILDCARD', 'STRING')


# the following is to defining rules for each token
def t_SELECT(t):
    r'select'
    return t


def t_FROM(t):
    r'from'
    return t


def t_WHERE(t):
    r'where'
    return t


def t_AND(t):
    r'and'
    return t


def t_BOOL(t):
    r'(true|True|False|false)'
    t.value = t.value.lower() == 'true'
    return t


def t_TCNAME(t):
    r'[a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)*'  # 更严格的命名规则
    # t.type = 'TCNAME'  # 确保始终设置type
    return t


def t_COMMA(t):
    r','
    return t


def t_EQX(t):
    r'[=]'
    return t


def t_CONSTANT(t):
    r'\b\d+\b'
    t.value = int(t.value)
    return t


def t_STRING(t):
    r"'(?:[^']|'')+'"  # 匹配单引号包围的字符串
    t.value = t.value[1:-1]  # 去除引号
    return t





'''
def t_SPACE(t):
    r'\s+'
    pass
'''

def t_WILDCARD(t):
    r'\*'
    return t




# --------------------------
# to cope with the error
# ------------------------

def t_error(t):
    try:

        print('wrong')

    except lex.LexError:
        print('wrong')

    else:
        print('wrong')


# ------------------------------------------
# to set the global_lexer in common_db.py
# -------------------------------------------
def set_lex_handle():
    common_db.global_lexer = lex.lex()
    if common_db.global_lexer is None:
        print('wrong when the global_lex is created')


t_ignore = ' \t\n'  # 直接忽略空白符
'''
def test():
    my_lexer=lex.lex()
    my_lexer.input("select f1,f2 from GOOD where f1='xx' and f2=5 ")
    while True:
        temp_tok=my_lexer.token()
        if temp_tok is None:
            break
        print temp_tok


test()
'''

def tokenize_sql(sql):
    lexer = lex.lex()
    lexer.input(sql)
    print(f"\nTokenizing: '{sql}'")
    for tok in lexer:
        print(f"Type: {tok.type:<10} Value: {tok.value}")
    lexer.input(sql)  # 重置供后续使用