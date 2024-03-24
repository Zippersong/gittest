

 多个评分+Y变量，单维度
 1、分箱,groupby(分箱)
 2、计数、额度
 3、rate
 
 双维度

import os
import pandas as pd
import pymysql
import json
import datetime
import time
from tqdm import tqdm, trange
from openpyxl import load_workbook
import ast 
import json
from decimal import Decimal
import pickle


os.chdir(r"D:\Work\out_data\PythonCode")
with open(r"JYLOAN_sc.json") as db_config:
    cnx_args = json.load(db_config)
cnx = pymysql.connect(**cnx_args)


# 授信环节
sql = """ 	 select a.user_id,a.flow_id, a.request_body,c.channel_source from 
                (
                 select user_id,flow_id,request_body,
                 ROW_NUMBER() over (PARTITION by flow_id order by create_time desc,update_time  desc)  as countid
                 FROM juin_loan_core_prd.risk_request_apply 
                 WHERE process_node IN (0,4) 
                 ) as a
										 left join juin_loan_core_prd.user_info as b on a.user_id=b.id
left join juin_loan_core_prd.white_list as c on b.id_number_md5=c.id_number
where   a.countid=1   """   #注意客户授信失败过了冻结期后的再授信问题，还有客户过了缓存期的提现调用三方问题
risk_request_apply=pd.read_sql(sql,cnx)



sql = """ 	select  规则名称,规则码 from juin_risk_operate.reject_code     """
reject_code_pzb=pd.read_sql(sql,cnx)


# 贷后表现：放款月,fpd,order_id,flow_id

sql="""
select
		a.flow_id,
		a.user_id,
		a.id as order_id, 
		loan_date,
		a.period,
		DATE_FORMAT(loan_date,'%Y-%m') as loan_mon,
		loan_amount,
		case when max_od_day >=7 then 1 else 0 end as dpd7,
		case when f_od>=7  then 1 else 0 end as fpd7_fz,
		case when DATEDIFF(NOW(),fpd.repayment_date)>=7 then 1 else 0 end fpd7_fm

from juin_loan_core_prd.order_record as a 
left join (SELECT*,case when status not in (0) then  DATEDIFF(IFNULL(settle_time,NOW()),repayment_date) end as f_od
						from juin_loan_core_prd.repayment_plan_period where period_number=1 ) as fpd on a.id=fpd.order_id
															
left join (SELECT order_id,
							 max( DATEDIFF(IFNULL(settle_time,NOW()),repayment_date) )  as max_od_day           
							from juin_loan_core_prd.repayment_plan_period
							where is_delete=0 and status not in (0) group by 1 order by 1) as od on a.id=od.order_id
where a.pay_status in (20) 
"""
dh_behave=pd.read_sql(sql,cnx)


# with open('D:\\Work\\Information\\zipper\\raw\\tables.pickle', 'rb') as f:
#     loaded_data = pickle.load(f)
 
# 解析urule入参第二代
tables = {}
def for_jiexi(risk_request_apply):
    table_list1=[
    'inputAntiFraudParameter',
    'inputApplyParameter',
    'inputThirdParameter']

    table_list2=[
    'inputBaiRongParameter',
    'inputDianHuaParameter',
    'inputIcekreditParameter',
    'inputOcrParameter',
    'inputRong360Parameter',
    'inputTdParameter',
    'inputTongDunParameter',
    'inputUnionPayParameter',
    'inputWeiYanParameter']


    for i in range(len(risk_request_apply)):
        print(i)
        try:
            temp_json = json.loads(risk_request_apply['request_body'][i])

            for table_list, table_prefix in [(table_list1, ''), (table_list2, 'n')]:
                for table_name in table_list:
                    if table_name in table_list1:
                        table_data = pd.DataFrame(temp_json[table_name], index=[i])
                    else:
                        if table_name in table_list2:
                            table_data = pd.DataFrame(temp_json['inputThirdParameter'][table_name], index=[i])

                    table_data['user_id'] = risk_request_apply['user_id'][i]
                    table_data['flow_id'] = risk_request_apply['flow_id'][i]
                    

                    if table_name not in tables:
                        tables[table_name] = table_data
                    else:
                        tables[table_name] = pd.concat([tables[table_name], table_data])
        except Exception as e:
            print(f"An error occurred in iteration {i}: {e}")
            continue

    return tables
for_jiexi(risk_request_apply)

# 导出到本地
with open('D:\\Work\\Information\\zipper\\raw\\tables_sx.pickle', 'wb') as f:
    pickle.dump(tables, f)
    
# 验证flow_id是否重复
a1=pd.DataFrame(tables['inputApplyParameter'])
# a1.duplicated(subset='flow_id').any()#判断pd是否有重复值

拼需要的字段：channel,

# 贷前标签：chnannel_source
a_temp=pd.DataFrame(tables['inputIcekreditParameter'])
a_temp_set=set(a_temp.user_id)
sql = """ 	select  id as user_id,id_number_md5 from juin_loan_core_prd.user_info     """
user_info=pd.read_sql(sql,cnx)
user_info=user_info[user_info.user_id.isin(a_temp_set)]
sql = """ 	select  id_number,channel_source from juin_loan_core_prd.white_list     """
white_list=pd.read_sql(sql,cnx)
user_info=pd.merge(user_info,white_list,how='left',left_on='id_number_md5',right_on='id_number')
user_info=user_info[['user_id','channel_source']]









