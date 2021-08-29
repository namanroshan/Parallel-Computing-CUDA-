import cudf as pd
import cupy as np
import os
import datetime
from datetime import datetime,timedelta
def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta
timer = [dt.strftime('%H:%M') for dt in datetime_range(datetime(1,1,1,9,20),datetime(1,1,1,15,35),timedelta(minutes=5))]
#TradeData Load Directly
def myfunc(datelist):
    dft = pd.read_csv("FAO_Trades_%s.DAT.gz"%datelist)
    dft.columns = ['A']
    dft = dft[dft['A'].str[46:52] == 'FUTSTK']
    dft = dft[dft['A'].str[0:2] == 'RM']
    dft = dft[dft['A'].str[2:6] == 'FAOb']
    dft['Trade#'] = dft['A'].str[6:22].astype('category')
    dft['Trade Time'] = dft['A'].str[22:36].values.astype(np.float)/65536
    dft['Symbol'] = dft['A'].str[36:46].astype('category')
    dft['Expiry Date'] = dft['A'].str[52:61].astype('category')
    dft['Trade Price'] = pd.to_numeric(dft['A'].str[71:79], downcast = 'unsigned')
    dft['Trade Qnty'] = pd.to_numeric(dft['A'].str[79:87] , downcast = 'unsigned')
    dft ['BOrder#'] = dft['A'].str[87:103].astype('category')
    dft ['SOrder#'] = dft['A'].str[105:121].astype('category')
    del dft['A']
    dft.index = pd.to_datetime(dft['Trade Time'], unit='s').dt.strftime('%H:%M:%S.%f')
    dft.index = pd.to_datetime(dft.index,format ='%H:%M:%S.%f')
    del dft.index.name    
    fbb = []
    for filename in os.listdir('tempDir'):
        li = []
        for frames in os.listdir('tempDir/%s'%filename):
            ddf = pd.read_csv("tempDir/%s/%s"%(filename,frames), index_col = 0)
            li.append(ddf)
        del ddf
        df = pd.concat(li)
        li.clear()
        df.index = pd.to_datetime(df['Order Time'], unit='s').dt.strftime('%H:%M:%S.%f')
        df.index = pd.to_datetime(df.index,format ='%H:%M:%S.%f')
        symb = df['Symbol'][0]
        dftt = dft[dft['Symbol'] == symb]
        df['AlgoInd.'] =  df['AlgoInd.'].replace(to_replace = 2, value = 0)
        df['AlgoInd.'] =  df['AlgoInd.'].replace(to_replace = 3, value = 1)
        for exp,df_exp in df.groupby('Expiry Date'):
            bob_df = pd.DataFrame(columns = ['Time','Expiry Date','Symbol','Total_Bid_Depth','Bid_Depth_Top5','Bid_Depth_Top10','Algo5_Bid_Depth','Algo10_Bid_Depth','Best_Bid','PA_Bid_Depth','CA_Bid_Depth','RA_Bid_Depth','PNA_Bid_Depth','CNA_Bid_Depth','RNA_Bid_Depth','Total_Ask_Depth','Ask_Depth_Top5','Ask_Depth_Top10','Algo5_Ask_Depth','Algo10_Ask_Depth','Best_Ask','PA_Ask_Depth','CA_Ask_Depth','RA_Ask_Depth','PNA_Ask_Depth','CNA_Ask_Depth','RNA_Ask_Depth'])
            dfttt = dftt[dftt['Expiry Date'] == exp]
            for ind in range(75):                                        
                order_book = df.between_time('09:15',timer[ind])
                trade_book = dfttt.between_time('09:15',timer[ind])
                buy_book = order_book[order_book['B/S Ind.'] == 'B']
                sell_book = order_book[order_book['B/S Ind.'] == 'S']
                buy_book.drop_duplicates(subset = 'Order#', keep='last', inplace = True)
                buy_book = buy_book[buy_book['Activity Type'] != 3]
                buy_book = buy_book[buy_book['Combined']== 'NNN']
                del buy_book['Combined']
                del buy_book['B/S Ind.']
                buy_book = buy_book.rename(columns = {'Order#':'BOrder#','Volume Original':'Bid Quantity','Limit Price':'Bid Price'})
                bb1 = buy_book[~buy_book['BOrder#'].isin(trade_book['BOrder#'])]
                bb2 = buy_book[buy_book['BOrder#'].isin(trade_book['BOrder#'])]
                merged = pd.merge(trade_book,bb2,how = 'inner', on =['BOrder#'])
                m1 = merged[merged['Trade Time']>merged['Order Time']]
                m2 = merged[~(merged['Trade Time']>merged['Order Time'])]
                m2 = m2[~m2['BOrder#'].isin(m1['BOrder#'])]
                finalm = m1.groupby(['BOrder#'],sort = False).agg({'Trade Qnty':'sum'})
                m1.drop_duplicates(subset='BOrder#',keep ='last',inplace = True)
                m2.drop_duplicates(subset='BOrder#',keep ='last',inplace = True)
                m1['Bid Quantity'] = (m1['Bid Quantity'] - finalm['Trade Qnty']).fillna(0)
                m1 = m1[m1['Bid Quantity']>0]
                m12 = pd.concat([m1,m2], sort = True)
                m12 = m12[['BOrder#','Order Time','Activity Type','Symbol_y','Expiry Date_y','Bid Price','Bid Quantity','AlgoInd.','CIF']]
                m12 = m12.rename(columns = {'Symbol_y':'Symbol','Expiry Date_y':'Expiry Date'})
                result = pd.concat([bb1,m12], sort = True).sort_values(by = 'Bid Price',ascending = False).reset_index(drop=True)

                #Sell Book
                sell_book.drop_duplicates(subset = 'Order#', keep='last', inplace = True)
                sell_book = sell_book[sell_book['Activity Type'] != 3]
                sell_book = sell_book[sell_book['Combined']== 'NNN']
                del sell_book['Combined']
                del sell_book['B/S Ind.']
                sell_book = sell_book.rename(columns = {'Order#':'SOrder#','Volume Original':'Ask Quantity','Limit Price':'Ask Price'})
                sb1 = sell_book[~sell_book['SOrder#'].isin(trade_book['SOrder#'])]
                sb2 = sell_book[sell_book['SOrder#'].isin(trade_book['SOrder#'])]
                merged2 = pd.merge(trade_book,sb2,how = 'inner', on =['SOrder#'])
                mt1 = merged2[merged2['Trade Time']>merged2['Order Time']]
                mt2 = merged2[~(merged2['Trade Time']>merged2['Order Time'])]
                mt2 = mt2[~mt2['SOrder#'].isin(mt1['SOrder#'])]
                finalm = mt1.groupby(['SOrder#'],sort = False).agg({'Trade Qnty':'sum'})
                mt1.drop_duplicates(subset='SOrder#',keep ='last',inplace = True)
                mt2.drop_duplicates(subset='SOrder#',keep ='last',inplace = True)
                mt1['Ask Quantity'] = (mt1['Ask Quantity'] - finalm['Trade Qnty']).fillna(0)
                mt1 = mt1[mt1['Ask Quantity']>0]
                mt12 = pd.concat([mt1,mt2], sort = True)
                mt12 = mt12[['SOrder#','Order Time','Activity Type','Symbol_y','Expiry Date_y','Ask Price','Ask Quantity','AlgoInd.','CIF']]
                mt12 = mt12.rename(columns = {'Symbol_y':'Symbol','Expiry Date_y':'Expiry Date'})
                result2 = pd.concat([sb1,mt12], sort = True).sort_values(by = 'Ask Price',ascending = True).reset_index(drop=True)
                #Final Order Book
                bob_df.loc[ind] = [timer[ind]] + [exp] + [symb] + [result['Bid Quantity'].sum()] + [result.iloc[:5,4].sum()] + [result.iloc[:10,4].sum()] + [result.iloc[:5,4].loc[result['AlgoInd.']==0].sum()] + [result.iloc[:10,4].loc[result['AlgoInd.']==0].sum()] + [result['Bid Price'][0]] +[result['Bid Quantity'][(result['AlgoInd.']==0) & (result['CIF'] == 1)].sum()] + [result['Bid Quantity'][(result['AlgoInd.']==0) & (result['CIF'] == 2)].sum()]+[result['Bid Quantity'][(result['AlgoInd.']==0) & (result['CIF'] == 3)].sum()]+[result['Bid Quantity'][(result['AlgoInd.']==1) & (result['CIF'] == 1)].sum()]+[result['Bid Quantity'][(result['AlgoInd.']==1) & (result['CIF'] == 2)].sum()]+[result['Bid Quantity'][(result['AlgoInd.']==1) & (result['CIF'] == 3)].sum()] +  [result2['Ask Quantity'].sum()] + [result2.iloc[:5,3].sum()] + [result2.iloc[:10,3].sum()] + [result2.iloc[:5,3].loc[result2['AlgoInd.']==0].sum()] + [result2.iloc[:10,3].loc[result2['AlgoInd.']==0].sum()] + [result2['Ask Price'][0]] +[result2['Ask Quantity'][(result2['AlgoInd.']==0) & (result2['CIF'] == 1)].sum()] + [result2['Ask Quantity'][(result2['AlgoInd.']==0) & (result2['CIF'] == 2)].sum()]+[result2['Ask Quantity'][(result2['AlgoInd.']==0) & (result2['CIF'] == 3)].sum()]+[result2['Ask Quantity'][(result2['AlgoInd.']==1) & (result2['CIF'] == 1)].sum()]+[result2['Ask Quantity'][(result2['AlgoInd.']==1) & (result2['CIF'] == 2)].sum()]+[result2['Ask Quantity'][(result2['AlgoInd.']==1) & (result2['CIF'] == 3)].sum()]                 

            fbb.append(bob_df)
            del bob_df
    res_csv = pd.concat(fbb)
    fbb.clear()
    res_csv['Symbol'] = res_csv['Symbol'].str.replace('b','')
    res_csv['Best_Bid'] = res_csv['Best_Bid'].values/100
    res_csv['Best_Ask'] = res_csv['Best_Ask'].values/100
    res_csv.to_csv("fao_lob_%s.csv"%datelist)

    