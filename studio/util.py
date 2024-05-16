import hashlib


def hash16(obj):
    return hashlib.md5(str(obj).encode()).hexdigest()
