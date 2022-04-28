from connection.mariadb import connection_177_ktspi


def get_cosmetic_keyword():
    conn, cur = connection_177_ktspi()
    sql_get_keyword = (
        "select keyword_en from KAN_CODE_KEYWORD where kan_code like '03%'"
    )
    cur.execute(sql_get_keyword)
    keyword = [i[0] for i in cur.fetchall() if i[0] != ""]
    print(len(keyword), keyword)
    return keyword


def get_cloth_keyword():
    conn, cur = connection_177_ktspi()
    sql_get_keyword = "select keyword_en from KAN_CODE_KEYWORD where kan_code like '09%' or kan_code like '11%'"
    cur.execute(sql_get_keyword)
    keyword = [i[0] for i in cur.fetchall() if i[0] != ""]
    print(len(keyword), keyword)
    return keyword
