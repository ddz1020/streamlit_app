import numpy as np
import pandas as pd
import datetime
import streamlit as st
import os
import re
import warnings
warnings.filterwarnings('ignore')

def data_clean(x):
    x = re.findall('[0-9\.]+',str(x))
    x = ''.join(x) if x else ''
    try:
        x = eval(x)
    except:
        x = 0
        
    return x

def columns_extract(new_data):
    extract_columns = ['Start Date','Portfolio name','Campaign Name','Match Type','Customer Search Term',
                       'Impressions','Clicks','Click-Thru Rate (CTR)','Spend','7 Day Conversion Rate',
                       '7 Day Total Orders (#)','14 Day Conversion Rate','14 Day Total Orders (#)']
    column_name_map = {
                      'Portfolio name':'ASIN',
                      '7 Day Total Orders (#)':'Orders',
                      '14 Day Total Orders (#)':'Orders',
                      '7 Day Conversion Rate':'CR',
                      '14 Day Conversion Rate':'CR'}
    new_data = new_data[[x for x in new_data.columns if x in extract_columns]]
    new_data = new_data.rename(columns = column_name_map)
    new_data[['Impressions','Clicks','Click-Thru Rate (CTR)','Spend','CR','Orders']] = new_data[['Impressions','Clicks','Click-Thru Rate (CTR)','Spend','CR','Orders']].applymap(lambda x:data_clean(x))
    return new_data
    
def backup_read(path):
    file = os.path.join(path,'data_backup.xlsx')
    if os.path.exists(file):
        backup = pd.read_excel(file)
    else:
        backup = pd.DataFrame(columns = ['Start Date','ASIN','Campaign Name','Match Type','Customer Search Term',
                       'Impressions','Clicks','Click-Thru Rate (CTR)','Spend','Orders','CR'])
    
    file2 = os.path.join(path,'data_backup365.xlsx')
    start_date90 = (datetime.datetime.now()- datetime.timedelta(days = 90)).strftime('%Y-%m-%d %H:%M:%S')
    start_date365 = (datetime.datetime.now()- datetime.timedelta(days = 365)).strftime('%Y-%m-%d %H:%M:%S')
    
    if os.path.exists(file2):
        backup365 = pd.read_excel(file2)
        backup365['Start Date'] = pd.to_datetime(backup365['Start Date'])
        backup90 = backup365[backup365['Start Date']>= start_date90]
        backup365 = backup365[backup365['Start Date']>= start_date365]
    else:
        backup['Start Date'] = pd.to_datetime(backup['Start Date'])
        backup90 = backup[backup['Start Date'] >= start_date90]
        backup365 = backup[backup['Start Date'] >= start_date365]
    
    backup90 = backup90.drop_duplicates()
    backup365 = backup365.drop_duplicates()
    
    return backup,backup90,backup365

def days90_count(insert_date,campaign_id,asin,keyword,impression,clicks,ctr,spend):
    res = [insert_date,campaign_id,asin,keyword,impression,clicks,ctr,spend] + [np.nan]*6
    return res

def days365_count(insert_date,campaign_id,asin,keyword,impression,clicks,ctr,spend,cr,order):
    res = [insert_date,campaign_id,asin,keyword] + [np.nan]*4 + [impression,clicks,ctr,spend,cr,order]
    return res

@st.cache_data()
def dataCombine(new_data,path):
    file_columns = [
        '导入日期','Campaign ID','ASIN','NEGATIVE TARGETING',
        '90天数据求总_Impressions','90天数据求总_Clicks','90天数据求总_CTR','90天数据求总_Spend',
         '365天数据求总_Impressions','365天数据求总_Clicks','365天数据求总_CTR','365天数据求总_Spend',
         '365天数据求总_CR','365天数据求总_Orders',]

    backup,backup90,backup365 = backup_read(path)
    latest90 = pd.concat([backup90,new_data])
    latest365 = pd.concat([backup365,new_data])
    asin_list = latest365['ASIN'].unique()
    result = pd.DataFrame()
    data = pd.DataFrame()
    insert_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for asin in asin_list:
        #计算满足365天统计条件的
        tmp_data365 = latest365[latest365['ASIN']==asin]
        tmp_data90 = latest90[latest90['ASIN']==asin]
        keywords = tmp_data365['Customer Search Term'].unique()
        for keyword in keywords:
            tmp_key_data365 = tmp_data365[tmp_data365['Customer Search Term'] == keyword]
            clicks = tmp_key_data365['Clicks'].sum()
            order = tmp_key_data365['Orders'].sum()
            cr = round(order/clicks,4) if clicks > 0 else 0
            if (clicks >= 20) & (cr < 0.05):
                data = pd.concat([data,tmp_key_data365])
                campaign_ids = tmp_key_data365['Campaign Name'].unique()
                impression = tmp_key_data365['Impressions'].sum()
                spend = tmp_key_data365['Spend'].sum()
                ctr = round(clicks/impression,4) if impression > 0 else 0
                res = [days365_count(insert_date,campaign_id,asin,keyword,impression,clicks,ctr,spend,cr,order) for campaign_id in campaign_ids]
                res = np.array(res).T
                tmp_res = pd.DataFrame(dict(zip(file_columns,res)))
                result = pd.concat([result,tmp_res])

            #计算满足90天统计条件的
            if not tmp_data90.empty:
                tmp_key_data90 = tmp_data90[tmp_data90['Customer Search Term'] == keyword]
                if not tmp_key_data90.empty:
                    condition3 = tmp_key_data90[tmp_key_data90['Match Type'] == 'EXACT']
                    if not condition3.empty:
                        clicks = condition3['Clicks'].sum()
                        order = condition3['Orders'].sum()
                        campaign_ids = condition3['Campaign Name'].unique()
                        if (clicks >= 20)&(order == 0):
                            data = pd.concat([data,condition3])
                            impression = condition3['Impressions'].sum()
                            spend = condition3['Spend'].sum()
                            ctr = round(clicks/impression,4) if impression > 0 else 0
                            res = [days90_count(insert_date,campaign_id,asin,keyword,impression,clicks,ctr,spend) for campaign_id in campaign_ids]
                            res = np.array(res).T
                            tmp_res = pd.DataFrame(dict(zip(file_columns,res)))
                            result = pd.concat([result,tmp_res])
                        else:
                            for campaign_id in campaign_ids:
                                condition2 = condition3[condition3['Campaign Name'] == campaign_id]
                                clicks = condition2['Clicks'].sum()
                                order = condition2['Orders'].sum()
                                if (clicks >= 10)&(order == 0):
                                    data = pd.concat([data,condition2])
                                    impression = condition2['Impressions'].sum()
                                    spend = condition2['Spend'].sum()
                                    ctr = round(clicks/impression,4) if impression > 0 else 0
                                    res = days90_count(insert_date,campaign_id,asin,keyword,impression,clicks,ctr,spend)
                                    tmp_res = pd.DataFrame(dict(zip(file_columns,res)),index = [0])
                                    result = pd.concat([result,tmp_res])
                                else:
                                    pass
                    else:
                        condition1 = tmp_key_data90[tmp_key_data90['Match Type']!='EXACT']
                        campaign_ids = condition1['Campaign Name'].unique()
                        for campaign_id in campaign_ids:
                            condition1_tmp = condition1[condition1['Campaign Name'] == campaign_id]
                            order = condition1_tmp['Orders'].sum()
                            if order == 0:
                                data = pd.concat([data,condition1_tmp])
                                impression = condition1_tmp['Impressions'].sum()
                                spend = condition1_tmp['Spend'].sum()
                                ctr = round(clicks/impression,4) if impression > 0 else 0
                                res = days90_count(insert_date,campaign_id,asin,keyword,impression,clicks,ctr,spend)
                                tmp_res = pd.DataFrame(dict(zip(file_columns,res)),index = [0])
                                result = pd.concat([result,tmp_res])
                            else:
                                pass
                else:
                    pass
            else:
                pass
            
    return backup,backup365,data,result

def backupSave(backup,backup365,data,path):
    backup = pd.concat([data,backup])
    backup = backup.drop_duplicates()
    backup.to_excel(os.path.join(path,'data_backup.xlsx'),index  = False,encoding = 'utf_8_sig')
    backup365 = pd.concat([data,backup365])   
    backup365 = backup365.drop_duplicates()
    backup365.to_excel(os.path.join(path,'data_backup365.xlsx'),index  = False,encoding = 'utf_8_sig')

def show():
    path = os.getcwd()
    #st.set_option('deprecation.showPyplotGlobalUse',False)
    st.title('数据更新')
    st.markdown(':apple: 可选多个文件一次性导入')
    uploaded_file = st.file_uploader('上传更新文件（支持xlsx格式文件）',accept_multiple_files = True)
    if uploaded_file:
        files = pd.DataFrame()
        filelist = []
        for file in uploaded_file:
            file = pd.read_excel(file)
            filelist.append(file)
            file = columns_extract(file)
            files = pd.concat([files,file])
        backup,backup365,data,result = dataCombine(files,path)
        page1,page2= st.tabs(['导入表','更新表'])
        with page1:
            for df in filelist:
                st.dataframe(df,hide_index = True,use_container_width = True)
        with page2:
            st.dataframe(result,hide_index = True,use_container_width = True)
            update = st.button('确认更新')
            if update == True:
                backupSave(backup,backup365,data,path)
                st.markdown(f'备份数据保存在：{path}下')
if __name__ =='__main__':
    show()

