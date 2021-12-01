from metastock2pd import metastock_read, metastock_read_master, metastock_master, metastock_emaster, metastock_xmaster
import os

def test_metastock_read():
    filename = os.path.join(os.getcwd(), 'data/F1.dat')
    df = metastock_read(filename)
    assert len(df) == 62 and list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'oi']

def test_metastock_read_mwd():
    filename = os.path.join(os.getcwd(), 'data/F548.mwd')
    df = metastock_read(filename)
    assert len(df) == 251 and list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'oi']

def test_metastock_master():
    path = os.path.join(os.getcwd(), 'data')
    res = metastock_master(path)
    assert len(res) == 128 and set(res.fields) == {7}
  
def test_metastock_emaster():
    path = os.path.join(os.getcwd(), 'data')
    res = metastock_emaster(path)
    assert len(res) == 255 and set(res.fields) == {7}

def test_metastock_xmaster():
    path = os.path.join(os.getcwd(), 'data')
    res = metastock_xmaster(path)
    assert len(res) == 294 and 'fields' not in res.columns

def test_metastock_read_master():
    path = os.path.join(os.getcwd(), 'data')
    res = metastock_read_master(path)
    assert len(res) == 294 + 128
    

