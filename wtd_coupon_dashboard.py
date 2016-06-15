import pandas as pd
import pyodbc
corp2=pyodbc.connect(dsn='corp')
corp2.autocommit=True
import logging



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# create a file handler
handler = logging.FileHandler('coup.log')
handler.setLevel(logging.INFO)

# create a logging format

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger

logger.addHandler(handler)
#unload pos_coup
try:
    corp2.execute("set isolation to dirty read;")
    logger.info('The isolation level has been set.')
except:
    logger.info('The isolation level was not set.')


try:
    df=pd.read_sql("""select item_id, sdate,edate,descr,auto_coup
    from pos_coup;"""
    ,corp2,parse_dates=['sdate','edate'],coerce_float=False)
    logger.info('pos_coup data was inserted.')
except:
    logger.info('pos_coup data was not inserted.')

df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Unloads\pos_coup.csv',index=False,sep='|')
#unload pos_coup_prch
try:
    df=pd.read_sql("""select item_id, prch_id, coup_item_type_id, prod_code, min_prc, valid_cat_id, max_prc
from pos_coup_prch;"""
    ,corp2,coerce_float=False)
    logger.info('pos_coup_prch data was inserted.')
except:
    logger.info('pos_coup_prch data was not inserted.')

df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Unloads\pos_coup_prch.csv',index=False,sep='|')
#unload pos_coup_disc

try:
    df=pd.read_sql("""select item_id, disc_id, coup_item_type_id, prod_code, min_prc, valid_cat_id, max_prc
from pos_coup_disc;"""
    ,corp2,coerce_float=False)
    logger.info('pos_coup_disc data was inserted.')
except:
    logger.info('pos_coup_disc data was not inserted.')

df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Unloads\pos_coup_disc.csv',index=False,sep='|')

#unload item_coll data
try:
    df=pd.read_sql("""select item_coll_id, descr
from item_coll"""
    ,corp2,coerce_float=False)
    logger.info('item_coll data was inserted.')
except:
    logger.info('item_coll data was not inserted.')

df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Unloads\item_coll.csv',index=False,sep='|')
#unload item_coll_dtl_data

try:
    df=pd.read_sql("""select item_coll_id, element_id, element_type_id, prod_code, valid_cat_id
from item_coll_dtl"""
    ,corp2,coerce_float=False)
    logger.info('item_coll_dtl data was inserted.')
except:
    logger.info('item_coll_dtl was not inserted.')

df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Unloads\item_coll_dtl.csv',index=False,sep='|')

#unload pos_coup_tndr

try:
    df=pd.read_sql("""select item_id
    from pos_coup_tndr
    group by 1"""
    ,corp2,coerce_float=False)
    logger.info('pos_coup_tndr data was inserted.')
except:
    logger.info('pos_coup_tndr was not inserted.')

df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Unloads\pos_coup_tndr.csv',index=False,sep='|')


nz=pyodbc.connect(dsn='NZSQL')
nz.autocommit=True
#load pos coup
try:
    nz.execute("""create temp table t_pos_coup as
select *
from external 'H:\\Adv_Mkt Dept\Generated_Reports\Unloads\pos_coup.csv'
(item_id bigint, sdate date, edate date, descr char(200),auto_coup integer)
using (
delimiter '|'
remotesource 'odbc'

skiprows 1
maxerrors 10
logdir 'C:\'
fillrecord);""")
    logger.info(' pos coup data was loaded')
except:
    pass
    logger.info(' pos coup data was not loaded')
#load pos_coup_prch
try:
    nz.execute("""create temp table t_pos_coup_prch as
select *
from external 'H:\\Adv_Mkt Dept\Generated_Reports\Unloads\pos_coup_prch.csv'
(item_id bigint, prch_id decimal(12,0), coup_item_type_id char(20),prod_code integer,min_prc decimal(12,2), valid_cat_id integer, max_prc decimal(12,2))
using (
delimiter '|'
remotesource 'odbc'

skiprows 1
maxerrors 10
logdir 'C:\'
fillrecord);""")
    logger.info(' pos coup prch data was loaded')
except:
    pass
    logger.info(' pos coup prch data was not loaded')

#load pos_coup_disc
try:
    nz.execute("""create temp table t_pos_coup_disc as
select *
from external 'H:\\Adv_Mkt Dept\Generated_Reports\Unloads\pos_coup_prch.csv'
(item_id bigint, disc_id decimal(12,0), coup_item_type_id char(20),prod_code integer,min_prc decimal(12,2), valid_cat_id integer, max_prc decimal(12,2))
using (
delimiter '|'
remotesource 'odbc'

skiprows 1
maxerrors 10
logdir 'C:\'
fillrecord);""")
    logger.info(' pos coup disc data was loaded')
except:
    pass
    logger.info(' pos coup disc data was not loaded')
#load item_coll
try:
    nz.execute("""create temp table t_item_coll as
select *
from external 'H:\\Adv_Mkt Dept\Generated_Reports\Unloads\item_coll.csv'
(item_coll_id bigint, descr char(100))
using (
delimiter '|'
remotesource 'odbc'

skiprows 1
maxerrors 10
logdir 'C:\'
fillrecord);""")
    logger.info('item coll data was loaded')
except:
    pass
    logger.info('item coll data was not loaded')
#load item_coll_dtl
try:
    nz.execute("""create temp table t_item_coll_dtl as
select *
from external 'H:\\Adv_Mkt Dept\Generated_Reports\Unloads\item_coll_dtl.csv'
(item_coll_id bigint, element_id decimal(12,0), element_type_id char(50), prod_code bigint, valid_cat_id bigint)
using (
delimiter '|'
remotesource 'odbc'
skiprows 1
maxerrors 10
logdir 'C:\'
fillrecord);""")
    logger.info('item coll dtl data was loaded')
except:
    pass
    logger.info('item coll dtl data was not loaded')
#load pos_coup_tndr
try:
    nz.execute("""create temp table t_pos_coup_tndr as
select *
from external 'H:\\Adv_Mkt Dept\Generated_Reports\Unloads\pos_coup_tndr.csv'
(item_id decimal(12,0))
using (
delimiter '|'
remotesource 'odbc'
skiprows 1
maxerrors 10
logdir 'C:\'
fillrecord);""")
    logger.info('pos_coup_tndr data was loaded')
except:
    pass
    logger.info('pos_coup_tndr data was not loaded')
# Create temp table to hold coupon data
try:
    nz.execute("""create temp table t_coup(
item_id decimal(12,0), coupon_description char(100), revenue decimal(12,2), item_cost decimal(12,2),


margin_dlr decimal(12,2), margin_pct decimal(12,2), total_transactions bigint, qty_redeemed bigint, coupon_cost decimal(12,0), start_Date date, end_date date)
;""")
    logger.info('t coup was loaded')
except:
    pass
    logger.info('t coup was not loaded')
#create lw table data to hold coupons
try:
    df_coup=pd.read_sql("""select coup_id
    from sales_fact_dtl d
where coup_id not in(select * from t_pos_coup_tndr)
and BUS_DATE between current_date - extract(dow from current_date)+1 and current_date-1
group by 1"""
    ,nz,coerce_float=False)
    logger.info('t coup was loaded')

except:
    pass
    logger.info('t coup was not loaded')

try:
    corp2.execute("delete from crm@dw_prd:uac_coup_wtd")
    logger.info('removed data from wtd table.')
except:
    logger.info('unable to remove data from wtd table.')

for x in df_coup.values.T.tolist()[0]:
    query="""insert into t_coup
select {x},
	(select descr from t_pos_coup where item_id={x}) coupon_description,
	(sum(d.NET_SALES_AMT)+sum(d.coup_amt)) as Revenue,
	sum(net_cost) as item_cost,
	sum(d.NET_SALES_AMT+d.coup_amt-d.net_cost) margin_dlr,
	(case when sum(d.NET_SALES_AMT + d.COUP_AMT)=0 then 0 else sum(d.NET_SALES_AMT+d.coup_amt-d.net_cost)  / (sum(d.NET_SALES_AMT)+sum(d.coup_amt))*100 end) margin_pct,
	count( distinct d.POS_TRANS_ID || d.STORE_ID) total_transactions,
	sum(case when d.COUP_ID={x} then d.qty_sold else 0 end) qty_redeemed,
	sum(d.COUP_AMT) coup_cost,
		(select sdate from t_pos_coup where item_id={x}) start_date,
			(select edate from t_pos_coup where item_id={x}) end_date

from sales_fact_dtl d

join lu_item i
	on(i.ITEM_KEY=d.item_key)
where  d.IS_OVRNG='N'
and d.IS_GOSHP_ID=0
and d.STORE_ID<>9000
and d.bus_Date between current_date - extract(dow from current_date)+1 and current_date-1
and d.POS_TRANS_ID not in(select pos_trans_id from buyback_fact_dtl where store_id = d.STORE_ID)
 and i.GRP_DEPT_ID not in (-2, -1, 13)
 and (i.item_id in(select prch_id from t_pos_coup_prch where coup_item_type_id='ITEM' and item_id={x})
    or d.coup_id={x}
    or i.item_id in(select disc_id from t_pos_coup_disc where coup_item_type_id='ITEM' and item_id={x})
    or d.posted_class in(select prch_id from t_pos_coup_prch where coup_item_type_id='PC'  and valid_cat_id in(-1) and item_id={x})
    or d.posted_class in(select disc_id from t_pos_coup_disc where coup_item_type_id='PC' and valid_cat_id in(-1) and item_id={x})
    or i.item_id in(select element_id from t_pos_coup_prch p join t_item_coll c on(p.prch_id=c.item_coll_id) join t_item_coll_dtl cd on(c.item_coll_id=cd.item_coll_id) where p.item_id={x} and p.coup_item_type_id='COLL'  and cd.element_type_id='ITEM' )
    or i.item_id in(select element_id from t_pos_coup_disc p join t_item_coll c on(p.disc_id=c.item_coll_id) join t_item_coll_dtl cd on(c.item_coll_id=cd.item_coll_id) where p.item_id={x} and p.coup_item_type_id='COLL'  and cd.element_type_id='ITEM' )
    or d.posted_class in(select element_id from t_pos_coup_prch p join t_item_coll c on(p.prch_id=c.item_coll_id) join t_item_coll_dtl cd on(c.item_coll_id=cd.item_coll_id) where p.item_id={x} and p.coup_item_type_id='COLL' and cd.element_type_id='PC' and p.valid_cat_id in(-1) )
    or d.posted_class in(select element_id from t_pos_coup_disc p join t_item_coll c on(p.disc_id=c.item_coll_id) join t_item_coll_dtl cd on(c.item_coll_id=cd.item_coll_id) where p.item_id={x} and p.coup_item_type_id='COLL' and cd.element_type_id='PC' and p.valid_cat_id in(-1))
    or i.VALID_CAT_ID in(select valid_cat_id from t_pos_coup_prch p where item_id={x} and p.VALID_CAT_ID not in(-1))
	 or i.VALID_CAT_ID in(select valid_cat_id from t_pos_coup_disc p where item_id={x} and p.VALID_CAT_ID not in(-1))
    or i.valid_cat_id in(select element_id from t_pos_coup_prch p join t_item_coll c on(p.prch_id=c.item_coll_id) join t_item_coll_dtl cd on(c.item_coll_id=cd.item_coll_id) where p.item_id={x} and p.coup_item_type_id='COLL'  and cd.element_type_id='PC' and p.valid_cat_id in(-1) )
    or i.valid_cat_id in(select element_id from t_pos_coup_disc p join t_item_coll c on(p.disc_id=c.item_coll_id) join t_item_coll_dtl cd on(c.item_coll_id=cd.item_coll_id) where p.item_id={x} and p.coup_item_type_id='COLL' and cd.element_type_id='PC' and p.valid_cat_id in(-1) )
)
and d.STORE_ID || d.POS_TRANS_ID in(select d.store_id ||
	d.pos_trans_id
from sales_fact_dtl d
join lu_item i
	on(d.ITEM_KEY=i.item_key)
where d.coup_id in({x})
and d.IS_OVRNG='N'
and d.IS_GOSHP_ID=0
and d.STORE_ID<>9000
and d.POS_TRANS_ID not in(select pos_trans_id from buyback_fact_dtl where store_id = d.STORE_ID)
 and i.GRP_DEPT_ID not in (-2, -1, 13)
 and d.BUS_DATE between current_date - extract(dow from current_date)+1 and current_date-1
group by 1
)

group by 1;"""
    query=query.format(**locals())
    nz.execute(query)
    logger.info('Data has been inserted for item ' + str(x))

#find tender items
try:
    df_coup=pd.read_sql("""select coup_id
    from sales_fact_dtl d
where coup_id  in(select * from t_pos_coup_tndr)
and BUS_DATE between current_date - extract(dow from current_date)+1 and current_date-1
group by 1"""
    ,nz,coerce_float=False)
    logger.info('t coup was loaded')

except:
    pass
    logger.info('t coup was not loaded')

#run analysis on tender coups
for x in df_coup.values.T.tolist()[0]:
    query="""insert into t_coup
select {x},
	(select descr from t_pos_coup where item_id={x}) coupon_description,
	(sum(d.NET_SALES_AMT)+sum(d.coup_amt)) as Revenue,
	sum(net_cost) as item_cost,
	sum(d.NET_SALES_AMT+d.coup_amt-d.net_cost) margin_dlr,
	(case when sum(d.NET_SALES_AMT + d.COUP_AMT)=0 then 0 else sum(d.NET_SALES_AMT+d.coup_amt-d.net_cost)  / (sum(d.NET_SALES_AMT)+sum(d.coup_amt))*100 end) margin_pct,
	count( distinct d.POS_TRANS_ID || d.STORE_ID) total_transactions,
	sum(case when d.COUP_ID={x} then d.qty_sold else 0 end) qty_redeemed,
	sum(d.COUP_AMT) coup_cost,
			(select sdate from t_pos_coup where item_id={x}) start_date,
			(select edate from t_pos_coup where item_id={x}) end_date

from sales_fact_dtl d

join lu_item i
	on(i.ITEM_KEY=d.item_key)
where  d.IS_OVRNG='N'
and d.IS_GOSHP_ID=0
and d.STORE_ID<>9000
and d.BUS_DATE between (select sdate from t_pos_coup p where p.item_id={x}) and
(select edate from t_pos_coup p where p.item_id={x})
and d.POS_TRANS_ID not in(select pos_trans_id from buyback_fact_dtl where store_id = d.STORE_ID)
 and i.GRP_DEPT_ID not in (-2, -1, 13)
 
and d.STORE_ID || d.POS_TRANS_ID in(select d.store_id ||
	d.pos_trans_id
from sales_fact_dtl d
join lu_item i
	on(d.ITEM_KEY=i.item_key)
where d.coup_id in({x})
and d.IS_OVRNG='N'
and d.IS_GOSHP_ID=0
and d.STORE_ID<>9000
and d.POS_TRANS_ID not in(select pos_trans_id from buyback_fact_dtl where store_id = d.STORE_ID)
 and i.GRP_DEPT_ID not in (-2, -1, 13)
 and d.BUS_DATE between current_date - extract(dow from current_date)+1 and current_date-1
group by 1
)
group by 1;"""
    query=query.format(**locals())
    nz.execute(query)
    logger.info('Data has been inserted for item ' + str(x))

#export the data into a dataframe
df_final=pd.read_sql("select * from t_coup", nz)
## convert to a list and insert rows one by one into informix
x=df_final.values.tolist()
for y in x[0:]:
    y1=int(y[0])
    y2=y[1]
    y3=y[2]
    y4=y[3]
    y5=y[4]
    y6=y[5]
    y7=y[6]
    y8=y[7]
    y9=y[8]
    y10=y[9]
    y11=y[10]
    

    query="""insert into 
    crm@dw_prd:uac_coup_wtd(item_id, coupon_description, revenue, item_cost, margin_dlr, margin_pct, total_transactions, 
    qty_redeemed, coupon_cost, start_date, end_date) values({y1},'{y2}',{y3},{y4},{y5},{y6},{y7},{y8},{y9},{y10},{y11})"""
    query=query.format(**locals())
    try:
        corp2.execute(query)
        logger.info('coup '+str(y1) + ' and ' + str(y2) + 'inserted')
    except:
        logger.info('coup '+str(y1) + ' and ' + str(y2) + 'did not work...')
	
