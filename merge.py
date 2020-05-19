import pandas as pd
import datetime as dt

for i in range(2005,2011):
    originalbalance = 0
    currentbalance = pd.DataFrame()
    for j in range(1,5):
        fileac = 'Acquisition_' + repr(i) + 'Q' + repr(j) + '.txt'
        filepe = 'Performance_' + repr(i) + 'Q' + repr(j) + '.txt'
        temp1 = pd.read_csv(fileac,sep = '|',header = None)
        temp2 = pd.read_csv(filepe,sep = '|',header = None)
        using1 = temp2.iloc[:,[0,1,4,10]]
        using1.columns = ['ID','Pay Date','Current Balance','Decline']
        using2 = temp1.iloc[:,[0,4,6]]
        using2.columns = ['ID','Original Balance','Original Date']
        period = []
        for k in range(1,13):
            if k < 10:
                temp = '0' + repr(k) + '/' + repr(i)
                period.append(temp)
            else:
                temp = repr(k) + '/' + repr(i)
                period.append(temp)
        period = pd.DataFrame(period)
        period.columns = ['Original Date']
        using2 = pd.merge(left = using2, right = period, on = 'Original Date')
        originalba = using2['Original Balance']
        originalbanum = sum(originalba)
        originalbalance += originalbanum
        using = pd.merge(left=using2,right = using1,on = 'ID')
        dataset = using
        del using, using1, using2
        dataset['Pay Date'] = pd.to_datetime(dataset['Pay Date'])
        temp3 = dataset.groupby('ID').max()
        temp3 = temp3['Pay Date']
        uniid = pd.DataFrame(temp3.index)
        temp3 = temp3.reset_index(drop = True)
        temp3 = pd.concat([uniid,temp3],axis = 1)
        temp3.columns = ['ID','Pay Date']
        dataset = dataset.drop('Original Date',axis = 1)
        dataset = dataset.reset_index(drop = True)
        dataset1 = pd.merge(left = temp3,right = dataset, on = ['ID','Pay Date'])
        dataset1 = dataset1[dataset1['Decline'] == '3']
        dataset1 = dataset1.drop_duplicates('ID',keep = 'last')
        dataset1 = dataset1.dropna()
        dataset1 = dataset1.reset_index(drop = True)
        lag = dt.timedelta(days = 30)
        for m in range(0,len(dataset1)):
            dataset1['Decline'][m] = int(dataset1['Decline'][m])
        dataset1['Decline'] = dataset1['Decline'] * 30
        datedefault = pd.to_datetime(dataset1['Pay Date']) - pd.to_timedelta(dataset1['Decline'],unit = 'D') - lag
        for l in range(0,datedefault.shape[0]):
            datedefault[l] = datedefault[l].year
        counts = datedefault.value_counts()
        year = pd.DataFrame(counts.index)
        datedefault = pd.DataFrame(datedefault)
        datedefault.columns = ['Year']
        dataset1 = pd.concat([dataset1,datedefault],axis = 1)
        curr = dataset1.groupby('Year').sum()['Current Balance']
        currentbalance = pd.concat([currentbalance,curr],axis = 1)
    currentbalance = currentbalance / originalbalance
    filename = repr(i) + 'Lossbalance3.csv'
    currentbalance.to_csv(filename,encoding = 'utf_8_sig')

acca = pd.DataFrame()
for i in range(2005,2016):
    for j in range(1,5):
        acname = 'Acquisition_' + repr(i) + 'Q' + repr(j) + '.txt'
        temp1 = pd.read_csv(acname,sep = '|',header = None)
        tempuse = temp1.iloc[:,[0,4,6,8,18]]
        tempuse.columns = ['ID','Original Balance','Original Date','OriginLTV','State']
        del temp1
        tempuse = tempuse[tempuse['State'] == 'CA']
        period = []
        for k in range(1,13):
            if k < 10:
                temp = '0' + repr(k) + '/' + repr(i)
                period.append(temp)
            else:
                temp = repr(k) + '/' + repr(i)
                period.append(temp)
        period = pd.DataFrame(period)
        period.columns = ['Original Date']
        using2 = pd.merge(left = tempuse, right = period, on = 'Original Date',how = 'inner')
        using2 = using2.reset_index(drop = True)
        acca = pd.concat([acca,using2],axis = 0,ignore_index=True)
# acca.to_csv('accaCA.csv',encoding = 'utf_8_sig')
acca = acca.sample(frac = 0.3)
peca = pd.DataFrame()
usingtime = []
for p in range(2005,2016):
    for j in range(1,13):
        if j < 10:
            date = '0' + repr(j) + '/01/' + repr(p)
        else:
            date = repr(j) + '/01/' + repr(p)
        usingtime.append(date)
usingtime = pd.DataFrame(usingtime)
usingtime.columns = ['Current Date']


for i in range(2005,2016):
    for j in range(1,5):
        acname = 'Performance_' + repr(i) + 'Q' + repr(j) + '.txt'
        temp1 = pd.read_csv(acname,sep = '|',header = None)
        tempuse = temp1.iloc[:,[0,1,4,10]]
        tempuse.columns = ['ID','Current Date','Current Balance','CLDS']
        tempuse = pd.merge(left = tempuse, right = acca, on = 'ID')
        #tempuse = pd.merge(left = tempuse, right = usingtime, on = 'Current Date')
        del temp1
        tempuse = tempuse.reset_index(drop = True)
        peca = pd.concat([peca,tempuse],axis = 0,ignore_index=True)
peca = peca.drop(['State'],axis = 1)
pegp = peca.groupby('ID')
pegp2 = pegp.groups
pelist = list(pegp2.keys())
period1 = []
for i in range(2005,2011):
    for k in range(1, 13):
        if k < 10:
            temp = '0' + repr(k) + '/' + repr(i)
            period1.append(temp)
        else:
            temp = repr(k) + '/' + repr(i)
            period1.append(temp)
period1 = pd.DataFrame(period1)
period1.columns = ['Original Date']
pe2005 = pd.merge(left = peca,right = period1,on = 'Original Date')
pe2005.to_csv('pecalifornia2005.csv',encoding = 'utf_8_sig')

period2 = []
for i in range(2011,2016):
    for k in range(1, 13):
        if k < 10:
            temp = '0' + repr(k) + '/' + repr(i)
            period2.append(temp)
        else:
            temp = repr(k) + '/' + repr(i)
            period2.append(temp)
period2 = pd.DataFrame(period2)
period2.columns = ['Original Date']
pe2010 = pd.merge(left = peca,right = period2,on = 'Original Date')
pe2010.to_csv('pecalifornia2010.csv',encoding = 'utf_8_sig')
del peca

pegp2005 = pe2005.groupby('ID')
pegp2005 = pegp2005.groups
pelist2005 = list(pegp2005.keys())
finishpe = pd.DataFrame()
for i in range(0,len(peid)):
    tempid = peid[i]
    temp = pe2005[pe2005['ID'] == tempid]
    temp = temp.reset_index(drop = True)
    nextstatus = temp['CLDS']
    nextstatus = nextstatus.drop([0],axis = 0)
    temp = pd.concat([temp,nextstatus],axis = 1,ignore_index = True)
    temp = temp.dropna()
    temp = temp.reset_index(drop = True)
    finishpe = pd.concat([finishpe,temp],axis = 0,ignore_index = True)
finishpe.columns = ['ID','Current Date','Current Balance','CLDS','Original Balance','Original Date','Original LTV','NLDS']

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib

matrix = pd.DataFrame([[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]])
data = pd.read_csv('peca2005.csv',low_memory=False)
for i in range(0,4):
    clds = i
    temp = data[data['CLDS'] == i]
    temp = temp.drop(['CLDS'],axis = 1)
    temp = temp.reset_index(drop = True)
    y = temp['NLDS']
    x = temp.drop(['NLDS'],axis = 1)
    abc = LogisticRegression(multi_class = 'multinomial')
    abc.fit(X=x,y = y)
    coeff = pd.DataFrame(abc.coef_)
    intercept = pd.DataFrame(abc.intercept_)
    formula = pd.concat([intercept,coeff],axis = 1)
    coeffpath = 'modelgroup'+repr(i)+'.csv'
    formula.to_csv(coeffpath,encoding = 'utf_8_sig')
    savepath = 'modelgroup'+repr(i)+'.m'
    joblib.dump(abc,savepath)

predata = pd.read_csv('peca2016.csv',low_memory = False)
for i in range(0,4):
    temp = predata[predata['CLDS'] == i]
    temp = temp.drop(['CLDS'],axis = 1)
    temp = temp.reset_index(drop = True)
    abc = joblib.load('modelgroup'+repr(i)+'.m')
    labels = abc.predict(temp)



















