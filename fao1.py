import cudf as pd
import cupy as np
import os
os.mkdir('tempDir')
row_count = (50000*5)
count = 0
def myfunc(datelist): 
    global count
    for df in pd.read_csv("D:/Trade Data/RawData/FAO_Orders_%s.DAT.gz"%datelist, chunksize = row_count):
        df.columns = ['A']
        df = df[df['A'].str[48:54] == 'FUTSTK']
        if df.shape[0]!=0:
            count += 1
            df = df[df['A'].str[0:2] == 'RM']
            df = df[df['A'].str[2:6] == 'FAOb']
            df['Order#'] = df['A'].str[6:22].astype('category')
            df['Order Time'] = df['A'].str[22:36].values.astype(np.float)/65536
            df['B/S Ind.']   =  df['A'].str[36:37].astype('category')
            df['Activity Type'] = pd.to_numeric(df['A'].str[37:38], downcast = 'unsigned')
            df['Symbol'] = df['A'].str[38:48].astype('category')
            df['Combined']  = df['A'].str[105:108].astype('category')
            df['Expiry Date'] = df['A'].str[54:63].astype('category')
            df['Limit Price'] = pd.to_numeric(df['A'].str[89:97], downcast = 'unsigned')
            df['Volume Original'] = pd.to_numeric(df['A'].str[81:89] , downcast = 'unsigned')
            df['Volume Disclosed'] = pd.to_numeric(df['A'].str[89:97] , downcast = 'unsigned')
            df['AlgoInd.'] = pd.to_numeric(df['A'].str[109:110],downcast = 'unsigned')
            df['CIF'] = pd.to_numeric(df['A'].str[110:111], downcast = 'unsigned')
            del df['A']
            for symb, df1 in df.groupby('Symbol'):
                if os.path.isdir("tempDir/%s"%symb) == False:
                    os.mkdir('tempDir/%s'%symb)
                    df1.to_csv("tempDir/%s/%s_%d_%s.csv"%(symb,symb,count,datelist))
                else:
                    df1.to_csv("tempDir/%s/%s_%d_%s.csv"%(symb,symb,count,datelist))
        
        


        
