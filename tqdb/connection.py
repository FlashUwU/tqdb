"""
Notes:

backup_file/ops_file checking function will be call in main loop, and the first time of built of the connection

read one-by-one and write one by one can save the ram

decrease the latency of inserting data and fetching data

encrypts password
"""
from typing import Union, Generator, Iterable, Callable, TypeVar

import os, shutil

import tqdb.utils as tool


_D = TypeVar("_D")

class DataContent:
    dformat = ("tag", "content")

    def __init__(self, tag: int, content: Union[bytes, Iterable], /, data_format: tuple[str]=dformat) -> None:
        self.tag = tag

        self.replace(content)

        self.dict: dict[str, bytes] = {}
        self.format = data_format
        self._format_data()
    
    def __str__(self) -> str:
        return str(self.dict)

    def __getitem__(self, key: str) -> bytes:
        return self.dict[key]
    
    def _format_data(self):
        data_content = self.data.split(b"\x03")
        for i, key in enumerate(self.format):
            self.dict[key] = data_content[i]
    
    def replace(self, content: Union[bytes, Iterable]):
        if not isinstance(content, bytes):
            self.data = tool.iter_to_data(content)
        else: self.data = content

class Connection:
    def __init__(self, path: str, data_format: tuple[str]) -> None:
        self.path = path
        self.splited_path = path.split("/")
        self.data_format = data_format
        self.dformatlen = len(data_format)

        self.is_changed = False
        self.read_size = 100

        self.filename = self.splited_path[-1]
        self.init_filepath = "/".join(self.splited_path[:-1]+[self.filename+".init"])
        self.backup_filepath = "/".join(self.splited_path[:-1]+[self.filename+".backup"])
        self.ops_filepath = "/".join(self.splited_path[:-1]+[self.filename+".ops"])

        self.__initialize_datafiles()

        self.indexes: dict[int, tuple[int]] = {}
        self.indexes_size = 100

        self.cache: dict[int, DataContent] = {}
        self.cache_size = 10

        self.commit_size = 100 # operations commit size per write operation of data_file

        self._commit_ops()

    def __initialize_datafiles(self):
        # rename orginal data_file to init_file
        os.rename(self.path, self.init_filepath)

        def close_and_nameback():
            init_file.close()
            backup_file.close()
            data_file.close()
            os.remove(self.path)
            os.rename(self.init_filepath, self.path)
        
        def when_done():
            init_file.close()
            backup_file.close()
            data_file.close()
            os.remove(self.init_filepath)
        
        def solve_crash(solution: int):
            if solution:
                os.remove(self.path)
                shutil.copy(self.backup_filepath, self.path)
            else:
                os.remove(self.backup_filepath)
                shutil.copy(self.path, self.backup_filepath)
            self.__initialize_datafiles()

        self.indexlinelen = 0
        self.indexeslen = 0

        # copy one if backup_file not exists
        if not os.path.exists(self.backup_filepath):
            shutil.copy(self.init_filepath, self.backup_filepath)

        # open [init_file(read)] [backup_file(read)] [new_data_file(write)]
        init_file = open(self.init_filepath, "rb")
        backup_file = open(self.backup_filepath, "rb")
        data_file = open(self.path, "ab")

        # check the size of init_file and backup_file
        if os.path.getsize(self.init_filepath) != os.path.getsize(self.backup_filepath):
            close_and_nameback()
            solve_crash(int(input("Backup_File size is different with Org_File size, make a solution.\n(0) use org_file (1) use backup_file :")))
            return

        reading_indexline = True
        pointer = 0

        # write bytes to data_file from init_file with checking
        while True:
            bbyte = backup_file.read(1)
            if not (byte := init_file.read(1)):
                if reading_indexline:
                    data_file.write(b"\x05")
                    self.indexlinelen += 1
                break

            if bbyte != byte:
                close_and_nameback()
                solve_crash(int(input(f"Diff: {pointer}(location) Bkp[{bbyte}]-Org[{byte}], make a solution.\n(0) use org_file (1) use backup_file :")))
                return

            pointer+=1

            if reading_indexline:
                self.indexlinelen += 1

                if byte == b"\r":
                    continue

                elif byte == b"\x05":
                    reading_indexline = False

                data_file.write(byte)
                continue

            data_file.write(byte)

        when_done()
    
    def __append(self, op: list[bytes]):
        newdata = op[1]

        alldatalen = os.path.getsize(self.path)
        newindexstart = alldatalen-self.indexlinelen
        newdatalen = len(newdata)
        newindex = (
            op[0]+b"\x03"
            +(tool.dec_to_base250(newindexstart))+b"\x03"
            +(tool.dec_to_base250(newdatalen))+b"\x03\x04"
        )
        newindexlen = len(newindex)

        ro_filepath = "/".join(self.splited_path[:-1]+[self.filename+".ro"])
        os.rename(self.path, ro_filepath)

        ro_file = open(ro_filepath, "rb")
        data_file = open(self.path, "ab")

        reading_indexline = True

        while True:
            if not (byte := ro_file.read(1)):
                data_file.write(newdata)
                break

            if reading_indexline:
                if byte == b"\x05":
                    byte = newindex+b"\x05"
                    reading_indexline = False

                data_file.write(byte)
                continue

            data_file.write(byte)

        ro_file.close()
        data_file.close()
        os.remove(ro_filepath)

        self.indexlinelen += newindexlen
    
    def __change(self, op: list[bytes]) -> None:
        tag = tool.base250_to_dec(op[0])
        newdata = op[1]
        org_range = self.indexes.get(tag) or self._get_indexes(target=tag)
        dataat, olddatalen = org_range

        newdatalen = len(newdata)
        datalendiff = newdatalen-olddatalen
        self.indexlinelen += datalendiff

        ro_filepath = "/".join(self.splited_path[:-1]+[self.filename+".ro"])
        os.rename(self.path, ro_filepath)

        ro_file = open(ro_filepath, "rb")
        data_file = open(self.path, "ab")

        reading_indexline = True
        current_tag = 0
        newdataat = 0
        locat = 0
        pointer = 0

        is_target = False
        is_after_target = False
        buffer = b""

        while True:
            if not (byte := ro_file.read(1)):
                break

            if reading_indexline:
                if byte == b"\x05":
                    data_file.write(b"\x05")
                    reading_indexline = False
                    is_after_target = False
                elif byte == b"\x03":
                    if locat == 0:
                        current_tag = tool.base250_to_dec(buffer)
                        if current_tag == tag:
                            is_target = True
                    elif locat == 1:
                        if is_after_target:
                            newdataat = tool.base250_to_dec(buffer)+datalendiff
                            buffer = tool.dec_to_base250(newdataat)
                    elif locat == 2:
                        if is_target:
                            buffer = tool.dec_to_base250(newdatalen)

                        if newdataat:
                            try:
                                self.indexes[current_tag] = (newdataat, tool.base250_to_dec(buffer))
                            except: pass
                            newdataat = 0

                    data_file.write(buffer+b"\x03")
                    buffer = b""
                    locat += 1
                elif byte == b"\x04":
                    if is_target:
                        is_target = False
                        is_after_target = True
                    data_file.write(b"\x04")
                    locat = 0
                else: buffer += byte
                continue
            
            if is_after_target:
                data_file.write(byte)
                continue

            buffer += byte
            if byte == b"\x03":
                locat += 1

            if locat == self.dformatlen:
                if is_target:
                    buffer = newdata
                    is_target = False
                    try:
                        self.cache[tag] = DataContent(tag,newdata, self.data_format)
                    except: pass
                    is_after_target = True
                data_file.write(buffer)
                buffer = b""
                locat = 0

            if pointer == dataat:
                is_target = True

            pointer += 1

        ro_file.close()
        data_file.close()
        os.remove(ro_filepath)
    
    def __remove(self, op: list[bytes]) -> None:
        tag = tool.base250_to_dec(op[0])
        org_range = self.indexes.get(tag) or self._get_indexes(target=tag)
        dataat, datalen = org_range
        finallocat = dataat+datalen

        self.indexlinelen -= datalen

        ro_filepath = "/".join(self.splited_path[:-1]+[self.filename+".ro"])
        os.rename(self.path, ro_filepath)

        ro_file = open(ro_filepath, "rb")
        data_file = open(self.path, "ab")

        reading_indexline = True
        locat = 0
        pointer = 0

        is_target = False
        buffer = b""

        while True:
            if not (byte := ro_file.read(1)):
                break

            if reading_indexline:
                if byte == b"\x05":
                    data_file.write(b"\x05")
                    reading_indexline = False
                elif byte == b"\x03":
                    if is_target:
                        continue

                    if locat == 0:
                        t = tool.base250_to_dec(buffer)
                        if t == tag:
                            is_target = True
                            continue

                    data_file.write(buffer+b"\x03")
                    buffer = b""
                    locat += 1
                elif byte == b"\x04":
                    if is_target:
                        buffer = b""
                        is_target = False
                        locat = 0
                        continue
                    data_file.write(b"\x04")
                    locat = 0
                else: buffer += byte
                continue

            if dataat <= pointer <= finallocat:
                pointer += 1
                continue

            data_file.write(byte)
            pointer += 1


        ro_file.close()
        data_file.close()
        os.remove(ro_filepath)

    def _do_backup(self) -> None:
        if self.is_changed:
            data_file = open(self.path, "rb")
            backup_file = open(self.backup_filepath, "wb")
            backup_file.close()
            backup_file = open(self.backup_filepath, "ab")
            while True:
                if not (data := data_file.read(self.read_size)):
                    break

                backup_file.write(data)

    def _commit_ops(self) -> None:
        if not os.path.exists(self.ops_filepath):
            return

        buffer = b""
        op = []

        ops_file = open(self.ops_filepath, "rb")

        while True:
            if not (byte := ops_file.read(1)):
                ops_file.close()
                os.remove(self.ops_filepath)
                break

            if byte == b"\x04":
                op.append(buffer)
                buffer = b""
            elif byte == b"\x05":
                match op[0]:
                    case b"\x2b":
                        self.__append(op[1:])
                        self.is_changed = True
                    case b"\x3d":
                        self.__change(op[1:])
                        self.is_changed = True
                    case b"\x2d":
                        self.__remove(op[1:])
                        self.is_changed = True
                op.clear()
            else:
                buffer += byte

    def _indexes_scanner(self) -> Generator:
        with open(self.path, "rb") as data_file:
            buffer = b""
            aset = []

            while True:
                byte = data_file.read(1)

                if byte == b"\x05":
                    break

                if not byte:
                    raise Exception("wrong format of data-index line")

                if byte == b"\x03":
                    aset.append(buffer)
                    buffer = b""
                elif byte == b"\x04":
                    yield aset
                    aset.clear()
                else: buffer+=byte
    
    def _get_indexes(self, size: int=-1, target: Union[int,str] =None) -> Union[tuple[int], None]:
        for I, aset in enumerate(self._indexes_scanner()):
            if I == size:
                break

            tag = tool.base250_to_dec(aset[0])
            data_range = tuple(map(tool.base250_to_dec, aset[1:]))
            self.push(self.indexes, self.indexes_size, tag, data_range)

            if target == tag:
                return data_range
    
    def fetch_index(self, tag: int) -> Union[tuple[int], None]:
        if data_range := self.indexes.get(tag):
            return data_range
        
        if data_range := self._get_indexes(target=tag):
            self.push(self.indexes, self.indexes_size, tag, data_range)
            return data_range
    
    def push(self, box: dict, box_size: int, tag: int, thing) -> None:
        box_length = len(box)
        if box_length >= box_size:
            offset = (box_length-box_size)+1

            while offset:
                first_tag = next(iter(box))
                del box[first_tag]
                offset -= 1
        
        box[tag] = thing

    def fetch(self, tag: int, /, dataclass: Callable[[int, Union[bytes, Iterable], tuple[str]], _D]=DataContent) -> Union[DataContent, _D]:
        if data_content := self.cache.get(tag):
            return data_content

        if not (data_range := self.fetch_index(tag)):
            raise Exception(f"tag not found: {tag}")

        with open(self.path, "rb") as data_file:
            data_file.seek(self.indexlinelen+data_range[0])
            data_content = dataclass(tag, data_file.read(data_range[1]), self.data_format)

        self.push(self.cache, self.cache_size, tag, data_content)
        return data_content

    def insert(self, data_content: DataContent) -> None:
        tag = data_content.tag
        if tag in self.indexes or self._get_indexes(target=tag):
            raise Exception(f"tag number has already been use: {tag}")

        with open(self.ops_filepath, "ab") as data_file:
            op = (
                b"\x2b\x04"+
                tool.dec_to_base250(tag)+b"\x04"
                +data_content.data+b"\x04\x05"
            )
            data_file.write(op)

        self.push(self.cache, self.cache_size, tag, data_content)
    
    def replace(self, data_content: DataContent) -> None:
        tag = data_content.tag
        if not (self.indexes.get(tag) or self._get_indexes(target=tag)):
            raise Exception(f"tag number isn't exist: {tag}")

        with open(self.ops_filepath, "ab") as data_file:
            op = (
                b"\x3d\x04"+
                tool.dec_to_base250(tag)+b"\x04"
                +data_content.data+b"\x04\x05"
            )
            data_file.write(op)
    
    def delete(self, tag: int):
        if not (self.indexes.get(tag) or self._get_indexes(target=tag)):
            raise Exception(f"tag number isn't exist: {tag}")

        with open(self.ops_filepath, "ab") as data_file:
            op = (
                b"\x2d\x04"+
                tool.dec_to_base250(tag)+b"\x04\x05"
            )
            data_file.write(op)
        
        try:
            del self.cache[tag]
        except: pass
    
    def commit(self) -> None:
        self._commit_ops()
        self._do_backup()


def connect(path: str, data_format: tuple[str]=DataContent.dformat) -> Connection:
    tqdb = Connection(path, data_format)
    return tqdb