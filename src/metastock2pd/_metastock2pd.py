import os
import pandas as pd
import struct
import datetime

### This is a simpler version of https://github.com/themech/ms2txt and he should get all the credit for the heavy lifting in parsing the bytes

def fmsbin2ieee(data):
    """
    Convert an array of 4 bytes containing Microsoft Binary floating point
    number to IEEE floating point format (which is used by Python)
    """
    as_int = struct.unpack("i", data)
    if not as_int:
        return 0.0
    man = int(struct.unpack('H', data[2:])[0])
    if not man:
        return 0.0
    exp = (man & 0xff00) - 0x0200
    man = man & 0x7f | (man << 8) & 0x8000
    man |= exp >> 1

    data2 = bytes(data[:2])
    if type(data2) is str:
        # python2
        data2 += chr(man & 255)
        data2 += chr((man >> 8) & 255)
    else:
        # python3
        data2 += bytes([man & 255])
        data2 += bytes([(man >> 8) & 255])
    return struct.unpack("f", data2)[0]

def float2date(date):
    """
    Metastock stores date as a float number.
    Here we convert it to a python datetime.date object.
    """
    date = int(date)
    if date < 101:
        date = 101
    year = 1900 + (date // 10000)
    month = (date % 10000) // 100
    day = date % 100
    return datetime.datetime(year, month, day)

def int2date(date):
    year = (date // 10000)
    month = (date % 10000) // 100
    day = date % 100
    return datetime.datetime(year, month, day)

def float2time(time):
    """
    Metastock stores date as a float number.
    Here we convert it to a python datetime.time object.
    """
    time = int(time)
    hour = time // 10000
    minute = (time % 10000) // 100
    return datetime.time(hour, minute)

def paddedString(s, encoding):
    # decode and trim zero/space padded strings
    zeroPadding = 0
    if type(s) is str:
        #python 2
        zeroPadding = '\x00'
    end = s.find(zeroPadding)
    if end >= 0:
        s = s[:end]
    try:
        return s.decode(encoding).rstrip(' ')
    except Exception as e:
        print("Error while reading the stock name. Did you specify the correct encoding?\n" +
              "Current encoding: %s, error message: %s" % (encoding, e))
        raise
        
class Column:
    """
    This is a base class for classes reading metastock data for a specific
    columns. The read method is called when reading a decode the column
    value
    @ivar dataSize: number of bytes is the data file that is used to store
                    a single value
    @ivar name: column name
    """
    dataSize = 4
    name = None

    def __init__(self, name):
        self.name = name

    def read(self, bytes):
        """Read and return a column value"""


class DateColumn(Column):
    """A date column"""
    def read(self, bytes):
        """Convert from MBF to date string"""
        return float2date(fmsbin2ieee(bytes))

class TimeColumn(Column):
    """A time column"""
    def read(self, bytes):
        """Convert read bytes from MBF to time string"""
        return float2time(fmsbin2ieee(bytes))

class FloatColumn(Column):
    """
    A float column
    @ivar precision: round floats to n digits after the decimal point
    """
    precision = 2
    def read(self, bytes):
        """Convert bytes containing MBF to float"""
        return fmsbin2ieee(bytes)

class IntColumn(Column):
    """An integer column"""
    def read(self, bytes):
        """Convert MBF bytes to an integer"""
        return int(fmsbin2ieee(bytes))

# we map a metastock column name to an object capable reading it
knownMSColumns = {
    'date': DateColumn('Date'),
    'time': TimeColumn('Time'),
    'open': FloatColumn('Open'),
    'high': FloatColumn('High'),
    'low': FloatColumn('Low'),
    'close': FloatColumn('Close'),
    'volume': IntColumn('Volume'),
    'oi': IntColumn('Oi'),
}
unknownColumnDataSize = 4    # assume unknown column data is 4 bytes long

# for _ in range(self.last_rec - 1):


        
def metastock_read(filename, fields = 7):
    """
    reads a metastock .DAT file

    Parameters
    ----------
    filename : str
        location.
    fields : int, optional
        number of columns. The default is 7. Only 7 or 8 are supported

    :Returns:
    -------
    res : pd.DataFrame
        timeseries read
    """
    if fields == 7:
        columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'oi']
    elif fields == 8:
        columns = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'oi']
    else:
        raise ValueError('do not know how to read this number of columns %i'%fields)        
    with open(filename, 'rb') as file_handle:
        _ = struct.unpack("H", file_handle.read(2))[0]
        last_rec = struct.unpack("H", file_handle.read(2))[0]
        file_handle.seek((fields - 1) * 4, os.SEEK_CUR)    
        rows = []
        for _ in range(last_rec - 1):
            row = []
            for column in columns:
                col = knownMSColumns.get(column)
                if col is None:
                    file_handle.seek(unknownColumnDataSize, os.SEEK_CUR)
                else:
                    byte = file_handle.read(col.dataSize)
                    value = col.read(byte)
                    row.append(value)
            rows.append(row)
    res = pd.DataFrame(rows, columns = columns)
    if fields == 8:
        res['datetime'] = [datetime.datetime.combine(date, time) for date, time in zip(res['date'], res['time'])]
        res = res.set_index('datetime')
    elif fields == 7:
        res = res.set_index('date') 
    return res


def metastock_read_master(path):
    """
    returns a dataframe with a record per each file

    Parameters
    ----------
    path : str
        directory where master file is found.

    Returns
    -------
    res : pd.DataFrame
        metadata per each .DAT file to be read.

    :Example:
    ---------
    >>> path = 'D:/TradingData/Futures/Contracts/2-Year Note C'
    >>> res = metastock_read_master(path).iloc[:3]; res
    
    >>>                                             filename  length first_date  last_date freq      symbol  fields
    >>> 0  D:/TradingData/Futures/Contracts/2-Year Note C...      28 1990-06-22 1990-09-19    D  TU2__1990U       7
    >>> 1  D:/TradingData/Futures/Contracts/2-Year Note C...      28 1990-06-22 1990-12-19    D  TU2__1990Z       7
    >>> 2  D:/TradingData/Futures/Contracts/2-Year Note C...      28 1990-09-18 1991-03-19    D  TU2__1991H       7
    
    once you have that, you can use metastock_read to read a file
    
    >>> res = dictable(res)    
    >>> res = res(ts = metastock_read)
    """
    filename = os.path.join(path, 'master')
    with open(filename, 'rb') as file_handle:
        records = struct.unpack("H", file_handle.read(2))[0]
        rows = []
        for i in range(records):
            file_handle.seek( (i+1)*53)
            file_number = struct.unpack("B", file_handle.read(1))[0]
            fname = os.path.join(path, 'F%d.DAT' % file_number)
            file_handle.seek(2, os.SEEK_CUR)
            record_length = struct.unpack("B", file_handle.read(1))[0]
            fields = struct.unpack("B", file_handle.read(1))[0]
            file_handle.seek(2, os.SEEK_CUR)
            _ = file_handle.read(16)
            # stock_name = paddedString(name, self.encoding)
    
            file_handle.seek(2, os.SEEK_CUR)
            first_date = float2date(fmsbin2ieee(file_handle.read(4)))
            last_date = float2date(fmsbin2ieee(file_handle.read(4)))
    
            freq = struct.unpack("c", file_handle.read(1))[0].decode('ascii')
            file_handle.seek(2, os.SEEK_CUR)
            name = file_handle.read(14)
            symbol = paddedString(name, 'ascii')
            rows.append(dict(filename = fname, length = record_length, first_date = first_date, last_date = last_date, freq = freq, symbol = symbol, fields = fields))
    res = pd.DataFrame(rows)
    return res

    
