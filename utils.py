from typing import Iterable

def dec_to_base250(num: int) -> bytes:
    if num < 250:
        return (num+6).to_bytes(1, "big")

    base250 = b""
    while num:
        num, remains = divmod(num, 250)
        base250 = (remains+6).to_bytes(1, "big") + base250

    return base250

def base250_to_dec(base250: bytes) -> int:
    bit = len(base250)-1
    decimal = 0

    for n in base250:
        num = n-6
        decimal += num*(250**bit)
        bit -= 1

    return decimal


def dict_to_indexline(obj: dict[int, tuple[int]]) -> bytes:
    indexline = b""
    for tag, data_range in obj.items():
        indexline+=(
            str(tag).encode(encoding="utf-8")+b"\x03"
            +str(data_range[0]).encode(encoding="utf-8")+b"\x03"
            +str(data_range[1]).encode(encoding="utf-8")+b"\x03\x04"
        )
    return indexline+b"\n"

def indexline_to_dict(obj: bytes, sort_out: bool=False) -> dict[int, tuple[int]]:
    indexes = {}
    buffer = b""
    tag = b""
    data_range = []
    istag = True

    for b in obj:
        b = bytes([b])
        if b == b"\n":
            break
        elif b == b"\x03":
            if istag:
                tag = buffer
                istag = False
            else:
                data_range.append(buffer)
            buffer = b""
        elif b == b"\x04":
            indexes[int(tag)] = tuple(map(int, data_range))
            data_range.clear()
            istag = True
        else:
            buffer+=b
    
    if sort_out:
        indexes = dict(sorted(indexes.items(), key=lambda s: s[1][0]))

    return indexes

def iter_to_data(obj: Iterable) -> bytes:
    aset = []
    for d in obj:
        aset.append(str(d).encode(encoding="utf-8"))
    return b"\x03".join(aset)+b"\x03"