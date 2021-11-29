from metastock2pd import metastock_read, metastock_read_master
import os

def test_metastock_read():
    filename = os.path.join(os.getcwd(), 'data/F1.dat')
    df = metastock_read(filename)
    assert len(df) == 62 and list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'oi']

def test_metastock_read_master():
    path = os.path.join(os.getcwd(), 'data')
    res = metastock_read_master(path)
    assert len(res) == 128 and set(res.fields) == {7}
  
