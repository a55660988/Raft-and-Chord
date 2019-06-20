import sys
import hashlib
import requests
import traceback as tb


# def gen_hash(m):
#     return lambda x: int.from_bytes(hashlib.sha1(x.encode()).digest(), byteorder=sys.byteorder) & ((1 << m)-1)


def gen_hash(m):
    def func(x):
        try:
            return int.from_bytes(hashlib.sha1(x.encode()).digest(),
                                  byteorder=sys.byteorder) & ((1 << m)-1)
        except Exception as e:
            e = str(e)+''.join(tb.format_stack())
            raise Exception(e)
    return func


def get_public_ip():
    return requests.get('http://wgetip.com/').text


def in_range(a, b, c):
    if a == c:
        return True
    if a < c:
        return a < b < c
    else:
        return a < b or b < c
