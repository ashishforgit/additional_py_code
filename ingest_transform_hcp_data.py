"""
nohup spark2-submit \
--master yarn \
--deploy-mode client \
--executor-memory 5G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=4 \
--conf spark.executor.cores=2 \
--files "application_hcp_trans_schema.json" \
ingest_transform_hcp_data.py \
--e Test-E2E \
&

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import types as T
from ConfigParser import RawConfigParser
from os.path import abspath
import json
import optparse
from getpass import getuser

config = RawConfigParser()
spark = None
config_set = None


def application_config():
    # Global
    config.add_section('Global')
    config.set('Global', 'application.name', 'HCP-Ingestion')
    config.set('Global', 'application.environment.choices', ['Test', 'Deploy', 'Test-E2E'])

    for section in config.get('Global', 'application.environment.choices'):
        config.add_section(section)

        if section == 'Test':
            # Local test
            config.set(section, 'hcp.transactions.data.source.path', abspath('../test/data/TCT_MI_Report_Daily_SAMPLE.txt'))
            config.set(section, 'hcp.transactions.historical.data.source.path', abspath('../test/data/sample_history_data_SAMPLE.TXT'))
            config.set(section, 'hcp.transactions.data.source.schema.path', abspath('application_hcp_trans_schema.json'))
            config.set(section, 'hcp.transactions.data.table', 'hcp_raw_txns')

            config.set(section, 'hcp.txns.base.raw.table', 'hcp_txns_base_raw')
            config.set(section, 'hcp.txns.base.raw.path', abspath('../test/hcp_txns_base_raw'))

            config.set(section, 'hcp.txns.base.table', 'hcp_txns_base')
            config.set(section, 'hcp.txns.base.path', abspath('../test/hcp_txns_base'))

            config.set(section, 'hcp.txns.new.table', None)
            config.set(section, 'hcp.txns.new.path', abspath('../test/hcp_txns_new'))

            config.set(section, 'dwh.fact.hotel.path', abspath('../test/data/dwh_fact_hotel_sample.parq'))
            config.set(section, 'dwh.dim.booking.path', abspath('../test/data/dwh_dim_booking_sample.parq'))
            config.set(section, 'dwh.dim.supplier.path', abspath('../test/data/dwh_dim_supplier_sample.parq'))
            config.set(section, 'dwh.dim.traveler.profile.path', abspath('../test/data/dwh_dim_traveler_profile_sample.parq'))
            config.set(section, 'dwh.dim.location.path', abspath('../test/data/dwh_dim_location_sample.parq'))

            config.set(section, 'hotel.hub.dwh.mapping.new.1.path', abspath('../test/hotel_hub_dwh_mapping_new_1'))
            config.set(section, 'hotel.hub.dwh.mapping.new.1.table', None)

            config.set(section, 'hcp.txns.new.fin.f.path', abspath('../test/hyt_hcp_txns_new_fin_f'))
            config.set(section, 'hcp.txns.new.fin.f.table', None)

            config.set(section, 'hcp.txns.new.fin.f.d2.path', abspath('../test/data/hyt_hcp_txns_new_fin_f_d2'))
            config.set(section, 'hcp.txns.new.fin.f.d2.table', None)

            config.set(section, 'hcp.txns.new.fin.f.d2.staging.path', abspath('../test/data/hyt_hcp_txns_new_fin_f_d2_staging'))
            config.set(section, 'hcp.txns.new.fin.f.d2.staging.table', None)

            config.set(section, 'hcp.txns.to.dwh.f.path', abspath('../test/hyt_hcp_txns_to_dwh_f'))
            config.set(section, 'hcp.txns.to.dwh.f.table', None)

        elif section == 'Test-E2E':
            # End-to-end test on cluster
            config.set(section, 'hcp.transactions.data.source.path', 'hdfs:///users/application/hcp_trans/TCT_MI_Report_*.TXT')
            config.set(section, 'hcp.transactions.historical.data.source.path', 'hdfs:///users/application/hcp_trans/sample_history_data_with_headers.TXT')
            config.set(section, 'hcp.transactions.data.source.schema.path', 'application_hcp_trans_schema.json')
            config.set(section, 'hcp.transactions.data.table', 'hcp_raw_txns')  # sch_dwh_rl.hcp_raw_txns

            config.set(section, 'hcp.txns.base.raw.table', 'sbx_dst.hyt_hcp_txns_base_raw')
            config.set(section, 'hcp.txns.base.raw.path', 'hdfs:///user/{}/hcp_txns_base_raw'.format(getuser()))

            config.set(section, 'hcp.txns.base.table', 'sbx_dst.hyt_hcp_txns_base')
            config.set(section, 'hcp.txns.base.path', 'hdfs:///user/{}/hcp_txns_base'.format(getuser()))

            config.set(section, 'hcp.txns.new.table', 'sbx_dst.hyt_hcp_txns_new')
            config.set(section, 'hcp.txns.new.path', 'hdfs:///user/{}/hyt_hcp_txns_new'.format(getuser()))

            config.set(section, 'dwh.fact.hotel.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/fact_hotel')
            config.set(section, 'dwh.dim.booking.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/dim_booking')
            config.set(section, 'dwh.dim.supplier.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/dim_supplier')
            config.set(section, 'dwh.dim.traveler.profile.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/dim_traveler_profile')
            config.set(section, 'dwh.dim.location.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/dim_location')

            config.set(section, 'hotel.hub.dwh.mapping.new.1.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/hyt_hotel_hub_dwh_mapping_new_1')
            config.set(section, 'hotel.hub.dwh.mapping.new.1.table', 'sbx_dst.hyt_hotel_hub_dwh_mapping_new_1')

            config.set(section, 'hcp.txns.new.fin.f.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/hyt_hcp_txns_new_fin_f')
            config.set(section, 'hcp.txns.new.fin.f.table', 'sbx_dst.hyt_hcp_txns_new_fin_f')

            config.set(section, 'hcp.txns.new.fin.f.d2.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/hyt_hcp_txns_new_fin_f_d2')
            config.set(section, 'hcp.txns.new.fin.f.d2.table', 'sbx_dst.hyt_hcp_txns_new_fin_f_d2')

            config.set(section, 'hcp.txns.new.fin.f.d2.staging.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/hyt_hcp_txns_new_fin_f_d2_staging')
            config.set(section, 'hcp.txns.new.fin.f.d2.staging.table', 'sbx_dst.hyt_hcp_txns_new_fin_f_d2_staging')

            config.set(section, 'hcp.txns.to.dwh.f.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/hyt_hcp_txns_to_dwh_f')
            config.set(section, 'hcp.txns.to.dwh.f.table', 'sbx_dst.hyt_hcp_txns_to_dwh_f')

        elif section == 'Deploy':
            # Deployment/production
            config.set(section, 'hcp.transactions.data.source.path', 'hdfs:///users/application/hcp_trans/TCT_MI_Report_*.TXT')
            config.set(section, 'hcp.transactions.historical.data.source.path', 'hdfs:///users/application/hcp_trans/sample_history_data_with_headers.TXT')
            config.set(section, 'hcp.transactions.data.source.schema.path', 'application_hcp_trans_schema.json')
            config.set(section, 'hcp.transactions.data.table', 'hcp_raw_txns')  # sch_dwh_rl.hcp_raw_txns

            config.set(section, 'hcp.txns.base.raw.table', 'sbx_dst.hcp_txns_base_raw')
            config.set(section, 'hcp.txns.base.raw.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/hcp_txns_base_raw')

            config.set(section, 'hcp.txns.base.table', 'sbx_dst.hcp_txns_base')
            config.set(section, 'hcp.txns.base.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/hcp_txns_base')

            config.set(section, 'hcp.txns.new.table', 'sbx_dst.sw_hcp_txns_new')
            config.set(section, 'hcp.txns.new.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/sw_hcp_txns_new')

            config.set(section, 'dwh.fact.hotel.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/fact_hotel')
            config.set(section, 'dwh.dim.booking.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/dim_booking')
            config.set(section, 'dwh.dim.supplier.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/dim_supplier')
            config.set(section, 'dwh.dim.traveler.profile.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/dim_traveler_profile')
            config.set(section, 'dwh.dim.location.path', 'hdfs:///user/hive/warehouse/sch_dwh_d3.db/dim_location')

            config.set(section, 'hotel.hub.dwh.mapping.new.1.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/sw_hotel_hub_dwh_mapping_new_1')
            config.set(section, 'hotel.hub.dwh.mapping.new.1.table', 'sbx_dst.sw_hotel_hub_dwh_mapping_new_1')

            config.set(section, 'hcp.txns.new.fin.f.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/sw_hcp_txns_new_fin_f')
            config.set(section, 'hcp.txns.new.fin.f.table', 'sbx_dst.sw_hcp_txns_new_fin_f')

            config.set(section, 'hcp.txns.new.fin.f.d2.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/sw_hcp_txns_new_fin_f_d2')
            config.set(section, 'hcp.txns.new.fin.f.d2.table', 'sbx_dst.sw_hcp_txns_new_fin_f_d2')

            config.set(section, 'hcp.txns.new.fin.f.d2.staging.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/sw_hcp_txns_new_fin_f_d2_staging')
            config.set(section, 'hcp.txns.new.fin.f.d2.staging.table', 'sbx_dst.sw_hcp_txns_new_fin_f_d2_staging')

            config.set(section, 'hcp.txns.to.dwh.f.path', 'hdfs:///user/hive/warehouse/sbx_dst.db/sw_hcp_txns_to_dwh_f')
            config.set(section, 'hcp.txns.to.dwh.f.table', 'sbx_dst.sw_hcp_txns_to_dwh_f')


def initialise_spark_session():
    global spark

    if config_set == 'Test':
        spark = SparkSession \
            .builder \
            .master('local[2]') \
            .appName(config.get('Global', 'application.name')) \
            .enableHiveSupport() \
            .config('spark.sql.parquet.binaryAsString', True) \
            .getOrCreate()
    else:
        spark = SparkSession \
            .builder \
            .appName(config.get('Global', 'application.name')) \
            .enableHiveSupport() \
            .config('spark.sql.shuffle.partitions', 2000) \
            .config('spark.dynamicAllocation.enabled', True) \
            .config('spark.sql.parquet.binaryAsString', True) \
            .getOrCreate()


def dump_dataframe(df, path=None, table_name=None):
    if (config_set == 'Test') | (table_name is None):
        df \
            .write \
            .mode('overwrite') \
            .parquet(path)
    else:
        df \
            .write \
            .saveAsTable(name=table_name,
                         mode='overwrite',
                         format='parquet',
                         path=path)
    print(str(spark.read.parquet(path).count()) + ' rows written to "' + path + '".')


def dump_partitioned_dataframe(df, partitioning_fields=None, path=None, table_name=None):
    if (config_set == 'Test') | (table_name is None):
        df \
            .repartition(*partitioning_fields) \
            .write \
            .partitionBy(*partitioning_fields) \
            .mode('overwrite') \
            .parquet(path)
    else:
        df \
            .repartition(*partitioning_fields) \
            .write \
            .partitionBy(*partitioning_fields) \
            .saveAsTable(name=table_name,
                         mode='overwrite',
                         format='parquet',
                         path=path)
    print(str(spark.read.parquet(path).count()) + ' rows written to "' + path + '".')


def ingest_hcp_trans_data():
    with open(config.get(config_set, 'hcp.transactions.data.source.schema.path'), 'r') as f:
        schema = json.load(f)

    df = spark.read.csv([config.get(config_set, 'hcp.transactions.data.source.path'),
                         config.get(config_set, 'hcp.transactions.historical.data.source.path')],
                        sep='\t',
                        header=True,
                        schema=T.StructType.fromJson(schema),
                        nullValue='\N'
                        ) \
        .filter(F.col('created_date').cast('int').isNotNull()) \
        .withColumnRenamed('CHEAP_AGGREGATOR_RATE', 'CHEAP_BOOKINGCOM_RATE') \
        .withColumnRenamed('CHEAP_AGGREGATOR_RATE_CURRCODE', 'CHEAP_BOOKINGCOM_RATE_CURRCODE') \
        .withColumnRenamed('CHEAP_AGGREGATOR_RATE_DESCRIPTION', 'CHEAP_BOOKINGCOM_RATE_DESCRIPTION') \
        .withColumnRenamed('DERIVED_COMMISSION%', 'DERIVED_COMMISSION') \
        .withColumn('date_created_year', F.concat(F.lit('20'), F.substring('created_date', 1, 2))) \
        .withColumn('date_created_month', F.substring('created_date', 3, 2)) \
        .withColumn('date_created_day', F.substring('created_date', 5, 2))

    dump_partitioned_dataframe(df,
                               ['date_created_year', 'date_created_month', 'date_created_day'],
                               config.get(config_set, 'hcp.txns.base.raw.path'),
                               config.get(config_set, 'hcp.txns.base.raw.table'))


def transform_hcp_trans_data():
    df_hcp_trans_data = spark \
        .read \
        .option('mergeSchema', 'true') \
        .parquet(config.get(config_set, 'hcp.txns.base.raw.path'))

    df_hcp_trans_data.createOrReplaceTempView(config.get(config_set, 'hcp.transactions.data.table'))

    df_hcp_txns_base = spark.sql("""
        select 
        BOOKING_STATUS
        ,HOTELHUB_BOOKING_REF
        ,CONFIRMATION_REF
        ,CANCELLATION_REF
        ,PNR
        ,PNR_Type
        ,HOTELHUB_MODE
        ,MARKET
        ,CWT_CLIENT_TOP_NAME
        ,CWT_SUB_UNIT_CLIENT_NAME
        ,CUSTOMER_AGENCY_NAME
        ,HOTEL_NAME
        ,CITY
        ,COUNTRY
        ,STAR_RATING
        ,cast(concat_ws
        ('-', concat(cast('20' as string), substr(cast(date_in as string),1,2)), 
        substr(cast(date_in as string),3,2), 
        substr(cast(date_in as string),5,2) ) as timestamp)
        as DATE_IN
        ,cast(concat_ws
        ('-', concat(cast('20' as string), substr(cast(date_out as string),1,2)), 
        substr(cast(date_out as string),3,2), 
        substr(cast(date_out as string),5,2) ) as timestamp)
        as DATE_OUT
        ,NIGHTS
        ,NUM_OF_ROOMS
        ,NUM_OF_GUEST
        ,OUT_POLICY_REASON
        ,BOOKING_SOURCE
        ,RATE_DESCRIPTION
        ,CANCELLATION_POLICY
        ,cast(RATEPERDAY_AMOUNT as double) as RATEPERDAY_AMOUNT
        ,RATEPERDAY_CURRCODE
        ,AGENCY_PRIORITY
        ,CUSTOMER_PRIORITY
        ,PAYMENT_MODE
        ,cast(RATEPERDAY_EUR as double) as RATEPERDAY_EUR
        ,cast(RATEPERDAY_GBP as double) as RATEPERDAY_GBP
        ,cast(RATEPERDAY_USD as double) as RATEPERDAY_USD
        ,TOTALAMOUNT_BOOKED_CURRCODE
        ,TOTALAMOUNT_BOOKED
        ,LOCAL_CURRENCY_CODE
        ,cast(RATEPERDAY_LCC as double) as RATEPERDAY_LCC
        ,cast(TOTALAMOUNT_EUR as double) as TOTALAMOUNT_EUR
        ,cast(TOTALAMOUNT_GBP as double) as TOTALAMOUNT_GBP
        ,cast(TOTALAMOUNT_USD as double) as TOTALAMOUNT_USD
        ,cast(TOTALAMOUNT_LCC as double) as TOTALAMOUNT_LCC
        ,BOOKED_RATE_TYPE_CODE
        ,CONTENT_SOURCE
        ,GDS_CHAIN_NAME
        ,cast(concat_ws
        ('-', concat(concat(cast('20' as string)), substr(cast(created_date as string),1,2)), 
        substr(cast(created_date as string),3,2), 
        substr(cast(created_date as string),5,2) ) as timestamp)
        as CREATED_DATE
        ,CREATEDBY_USER
        ,CONFIRMEDBY_USER
        ,case when length(cancel_datetime)=0 then '' 
        else 
        cast(concat_ws
        ('-', concat(concat(cast('20' as string)), substr(cancel_datetime,1,2)), 
        substr(cancel_datetime,3,2), 
        substr(cancel_datetime,5,11) ) as timestamp)
        end as cancel_datetime
        ,ABANDON_BY_USER
        ,OBT_PNR
        ,CWT_BOOKING_CHANNEL
        ,RATE_ACCESS_CODE_BOOKED
        ,COMMISSION_TYPE
        ,COMMISSION_CURRENCY
        ,COMMISSION_AMOUNT
        ,ESTIMATED_INCOME_DUE
        ,RATE_ACCESS_CODE_SHOPPED
        ,HOTELHUB_PROPERTY_ID
        ,HARP_PROPERTY_ID_NO
        ,CONTENT_SOURCE_PROPERTY_ID
        ,AGGREGATOR_BOOKING_COMMISSION
        ,AGGREGATOR_REVENUE_VALUE
        ,AGGREGATOR_REVENUE_SHARE
        ,AGGREGATOR_CURRENCY
        ,RATE_CHANGE
        ,RATE_ACCESS_CODE_RETURNED
        ,BACK_OFFICE_ACCOUNT_NUMBER
        ,case 
            when traveller_portrait_guid='' then 'UNKNOWN' 
            when traveller_portrait_guid like '%-%' then regexp_replace(traveller_portrait_guid, '-',':')
            else traveller_portrait_guid 
            end as TRAVELLER_PORTRAIT_GUID
        ,case when length(booking_start_dttm)=0 then '' 
        else 
        cast(concat_ws
        ('-', concat(concat(cast('20' as string)), substr(booking_start_dttm,1,2)), 
        substr(booking_start_dttm,3,2), 
        substr(booking_start_dttm,5,11) ) as timestamp)
        end as booking_st_tm
        ,case when length(booking_end_dttm)=0 then '' 
        else 
        cast(concat_ws
        ('-', concat(concat(cast('20' as string)), substr(booking_end_dttm,1,2)), 
        substr(booking_end_dttm,3,2), 
        substr(booking_end_dttm,5,11) ) as timestamp)
        end as booking_en_tm
        ,cast(STEP0_TIME as int) as STEP0_TIME
        ,cast(STEP1_TIME as int) as STEP1_TIME
        ,cast(STEP2_TIME as int) as STEP2_TIME
        ,cast(STEP3_TIME as int) as STEP3_TIME
        ,cast(STEP4_TIME as int) as STEP4_TIME
        ,GDS_SHOPPED_FOR_RATE
        ,cast(CHEAP_CLIENT_NEG_RATE as int) as CHEAP_CLIENT_NEG_RATE
        ,CHEAP_CLIENT_NEG_RATE_CURRCODE
        ,CHEAP_CLIENT_NEG_RATE_DESCRIPTION
        ,cast(CHEAP_CWT_OR_CWV_RATE as int) as CHEAP_CWT_OR_CWV_RATE
        ,CHEAP_CWT_OR_CWV_NEG_RATE_CURRCODE
        ,CHEAP_CWT_OR_CWV_RATE_DESCRIPTION
        ,cast(CHEAP_GDS_PUBLISHED_RATE as int) as CHEAP_GDS_PUBLISHED_RATE
        ,CHEAP_GDS_NEG_RATE_CURRCODE
        ,CHEAP_GDS_PUBLISHED_RATE_DESCRIPTION
        ,cast(CHEAP_BOOKINGCOM_RATE as int) as CHEAP_BOOKINGCOM_RATE 
        ,CHEAP_BOOKINGCOM_RATE_CURRCODE
        ,CHEAP_BOOKINGCOM_RATE_DESCRIPTION
        ,BRANCH_IATA
        ,ON_REQUEST_INDICATOR
        ,case when length(lastmodified_datetime)=0 then ''
        else
        cast(concat_ws('-', concat(concat(cast('20' as string)), substr(lastmodified_datetime,1,2)),
        substr(lastmodified_datetime,3,2),substr(lastmodified_datetime,5,11) ) as timestamp)
        end as lastmodified_datetime
        ,modified_count
        ,booking_time_duration
        ,rate_bucket
        ,back_office
        ,client_sub_unit_client_id
        ,cast (MISSED_SAVING as int) as missed_saving
        ,cast(REALISED_SAVING as int) as realised_saving
        ,POPULAR_HOTEL
        ,HOTEL_RANK
        ,cast(HOTEL_TOTAL as int) as hotel_total
        ,agency_source_name
        ,client_client_top_id
        ,err_desc
        ,aggregator_property_type
        ,hotel_bucket_simplified
        ,gds_commission_text
        ,avlb_htl_count 
        ,offer 
        ,aaa_rate 
        ,error_code 
        ,concat(hotelhub_property_id,rate_access_code_booked,booked_rate_type_code,case 
                when content_source like 'BOOKING%' then 'BC'
                when content_source like 'EAN%' then 'EH'
                when content_source like 'DESIYA%' then 'DH'
                when content_source like 'PREMIER%' then 'PI'
                when content_source like 'CHL%' then 'CM'
                when content_source like 'SABR%' then 'S'
                when content_source like 'AMAD%' then 'A'
                when content_source like 'GALI%' then 'G'
                when content_source like 'APO%' then '1V'
                when content_source like 'HOTELH%' and gds_shopped_for_rate like 'SABR%' then 'S'
                when content_source like 'HOTELH%' and gds_shopped_for_rate like 'APO%' then '1V'
                when content_source like 'HOTELH%' and gds_shopped_for_rate like 'AMAD%' then 'A'
                when content_source like 'HOTELH%' and gds_shopped_for_rate like 'GALI%' then 'G'
                else 'XX' end
        ) as rate_id
        ,case
         when content_source in('BOOKING.COM','EAN HOTEL COLLECT', 'BOOKING.COM CASHONLY','DESIYA HOTELS','LOCAL AGGREGATOR') then 'AGG RATE'
         when content_source ='PREMIER INN - PI' then 'PUB - DIRECT CONNECT'
         when (rate_bucket like '%CWT%' or rate_bucket like '%ROOMIT%') and rate_access_code_booked='CWV' then 'ROOMIT (CWV)'
         when (rate_bucket like '%CWT%' or rate_bucket like '%ROOMIT%') and ((instr(lower(rate_description),'client value')!=0) or (instr(lower(rate_description),'cwv')!=0)) then 'ROOMIT (CWV)'
         when (rate_access_code_booked='CWV' or rate_access_code_booked is null or rate_access_code_booked='') and 
              ((instr(lower(rate_description),'client value')!=0) or (instr(lower(rate_description),'cwv')!=0)) then 'ROOMIT (CWV)'
         when (instr(lower(rate_description),'client')!=0 and instr(lower(rate_description),'value')!=0)or 
              (instr(lower(rate_description),'carlson')!=0 and instr(lower(rate_description),'value')!=0) or
              instr(lower(rate_description),'roomit')!=0 or
              instr(lower(rate_description),'room it')!=0 then 'ROOMIT (CWV)'
         when rate_description not like'CWV%' and instr(lower(rate_description),'crs')!=0 then 'CLIENT'
         when (rate_bucket like '%CWT%' or rate_bucket like '%ROOMIT%') and rate_access_code_booked ='CWT' then 'ROOMIT (CWT)'
         when (rate_access_code_booked='CWT' or rate_access_code_booked is null or rate_access_code_booked='') and 
              ((instr(lower(rate_description),'client')!=0) or (instr(lower(rate_description),'carlson')!=0)) then 'ROOMIT (CWT)'
         when ((instr(lower(rate_description),'client')!=0) and (instr(lower(rate_description),'value')=0)) or 
              ((instr(lower(rate_description),'carlson')!=0) and (instr(lower(rate_description),'value')=0)) or
              instr(lower(rate_description), 'consortia')>0 then 'ROOMIT (CWT)'
         when rate_bucket like '%PUBLIC%' and instr(lower(rate_description), 'worldwide')>0 then 'PUB'
         when instr(lower(rate_description), 'room rac')>0 or
              instr(lower(rate_description), 'room pro')>0 or
              instr(lower(rate_description), 'bed flexible rate')>0 or
              instr(lower(rate_description), 'beds flexible rate')>0 then 'PUB'
         when  (rate_bucket like '%CLIENT%' or rate_bucket like '%KUNDEN%') then 'CLIENT'
         when  ((rate_bucket like '%REQUEST%' or rate_bucket like '%ANFRAGE%' or rate_bucket like '%DEMANDE%' or rate_bucket like '%PETICI%' or rate_bucket like 'PUBLIC%'  or rate_bucket is null or rate_bucket='') and 
              (gds_commission_text like 'NO%' or gds_commission_text is null or gds_commission_text ='')) then 'CLIENT'
         when  (rate_bucket like '%PUBLIC%' or rate_bucket='U') and 
              (gds_commission_text like '%NON%' or gds_commission_text like '%NOT%' or gds_commission_text like '%NO C%' or gds_commission_text like '% 0.00%') then 'CLIENT'
         when  ((gds_commission_text like '%NON%' or gds_commission_text like '%NOT%' or gds_commission_text like '%NO C%' or gds_commission_text like '% 0.00%' or gds_commission_text like '%UNK%') and 
              (rate_description like '%COR%' or rate_description like '%CLT' or rate_description like '%GOV%' or rate_description like '%NEG%')) and
              (instr(lower(rate_description),'corn')=0 and instr(lower(rate_description),'decor')=0 and instr(lower(rate_description),'corri')=0) then 'CLIENT'
         when ((gds_commission_text is null or gds_commission_text ='')and 
              (rate_bucket like '%REQUEST%' or rate_bucket like '%ANFRAGE%' or rate_bucket like '%DEMANDE%' or rate_bucket like '%PETICI%' or rate_bucket like 'PUBLIC%'  or rate_bucket is null or rate_bucket='') and 
              (rate_description like '%CLT' or rate_description like'%COR%' or rate_description like'%NEG%')) and
              (instr(lower(rate_description),'corn')=0 and instr(lower(rate_description),'decor')=0 and instr(lower(rate_description),'corri')=0) then 'CLIENT'
         when  ((gds_commission_text is null or gds_commission_text ='')and rate_description like '%COR') then 'CLIENT'
         when  (gds_commission_text like '%NON%' or gds_commission_text like '%NOT%' or gds_commission_text like '%NO C%' or gds_commission_text like '% 0.00%') then 'CLIENT'
         when  instr(lower(rate_description),'corporate')!=0 or instr(lower(rate_description),'government')!=0 then 'CLIENT'
         when  (rate_access_code_booked is null or rate_access_code_booked='' or rate_access_code_booked='SC' or rate_access_code_booked='COR') and 
            (instr(lower(rate_description),' cor')>0 or instr(lower(rate_description),' clt')>0) then 'CLIENT'
         when instr(lower(rate_description),lower(client_client_top_name))>0 then 'CLIENT'
         when instr(lower(rate_description), 'aaa')>0 or instr(lower(rate_description), 'caa')>0 or instr(lower(rate_description), 'aarp')>0 or instr(lower(rate_description), 'spg member')>0 then 'CLIENT'
         else 'PUB' end as new_rate_bucket,
         payment_type_used,
         case 
        when content_source like 'BOOKING%' then 'BC'
        when content_source like 'EAN%' then 'EH'
        when content_source like 'DESIYA%' then 'DH'
        when content_source like 'PREMIER%' then 'PI'
        when content_source like 'CHL%' then 'CM'
        when content_source like 'SABR%' then 'S'
        when content_source like 'AMAD%' then 'A'
        when content_source like 'GALI%' then 'G'
        when content_source like 'APO%' then '1V'
        when content_source like 'HOTELH%' and gds_shopped_for_rate like 'SABR%' then 'S'
        when content_source like 'HOTELH%' and gds_shopped_for_rate like 'APO%' then '1V'
        when content_source like 'HOTELH%' and gds_shopped_for_rate like 'AMAD%' then 'A'
        when content_source like 'HOTELH%' and gds_shopped_for_rate like 'GALI%' then 'G'
        else 'XX' end as channel_type,
        SESSIONID,
        date_created_year,
        date_created_month,
        date_created_day
    from 	{}""".format(config.get(config_set, 'hcp.transactions.data.table')))

    df_hcp_txns_base_deduped = df_hcp_txns_base \
        .withColumn('rownum',
                    F.row_number().over(Window
                                        .partitionBy('HOTELHUB_BOOKING_REF')
                                        .orderBy(F.col('LASTMODIFIED_DATETIME').desc()))) \
        .filter(F.col('rownum') == 1) \
        .drop('rownum')

    # Export for general analytical use as sbx_dst.hcp_txns_base
    dump_partitioned_dataframe(df_hcp_txns_base_deduped,
                               ['date_created_year', 'date_created_month', 'date_created_day'],
                               config.get(config_set, 'hcp.txns.base.path'),
                               config.get(config_set, 'hcp.txns.base.table'))

    # Generate match keys
    channels = ['CYTRIC', 'TRVDOO', 'KDSS', 'GETTHERE', 'CONCUR', 'BOOK2GO', 'SERKO', 'ZILLIOUS']
    regexp_pattern = '[^a-zA-Z0-9]+'

    df_hcp_txns_new = spark.read.parquet(config.get(config_set, 'hcp.txns.base.path')) \
        .filter(F.col('booking_status').isin(['CFD', 'CNX'])) \
        .filter('pnr is not null or obt_pnr is not null') \
        .withColumn('concat_base_OBT_PNR', F.concat('date_in', 'date_out', 'OBT_PNR')) \
        .withColumn('concat_base_PNR', F.concat('date_in', 'date_out', 'PNR')) \
        .withColumn('full_mk',
                    F.when(F.col('CWT_BOOKING_CHANNEL').isin(channels),
                           F.regexp_replace(F.concat('concat_base_OBT_PNR',
                                                     'HARP_PROPERTY_ID_NO',
                                                     'TRAVELLER_PORTRAIT_GUID'),
                                            regexp_pattern, ''))
                    .otherwise(F.regexp_replace(F.concat('concat_base_PNR',
                                                         'HARP_PROPERTY_ID_NO',
                                                         'TRAVELLER_PORTRAIT_GUID'),
                                                regexp_pattern, ''))) \
        .withColumn('prop_mk',
                    F.when(F.col('CWT_BOOKING_CHANNEL').isin(channels),
                           F.regexp_replace(F.concat('concat_base_OBT_PNR', 'HARP_PROPERTY_ID_NO'),
                                            regexp_pattern, ''))
                    .otherwise(F.regexp_replace(F.concat('concat_base_PNR', 'HARP_PROPERTY_ID_NO'),
                                                regexp_pattern, ''))) \
        .withColumn('pnr_mk',
                    F.when(F.col('CWT_BOOKING_CHANNEL').isin(channels),
                           F.regexp_replace('concat_base_OBT_PNR', regexp_pattern, ''))
                    .otherwise(F.regexp_replace('concat_base_PNR', regexp_pattern, ''))) \
        .withColumn('dedupe_key',
                    F.when(F.col('CWT_BOOKING_CHANNEL').isin(channels),
                           F.sha2(F.regexp_replace('concat_base_OBT_PNR', regexp_pattern, ''), 256))
                    .otherwise(F.sha2(F.regexp_replace('concat_base_PNR', regexp_pattern, ''), 256))) \
        .withColumn('rk', F.rank().over(Window.partitionBy('dedupe_key').orderBy(F.col('lastmodified_datetime').desc()))) \
        .filter('rk = 1') \
        .withColumn('row_num', F.lit(9999))

    dump_partitioned_dataframe(df_hcp_txns_new,
                               ['date_created_year', 'date_created_month', 'date_created_day'],
                               config.get(config_set, 'hcp.txns.new.path'),
                               config.get(config_set, 'hcp.txns.new.table'))


def parse_python_arguments():
    global config_set

    parser = optparse.OptionParser()

    parser.add_option('-e', '--env',
                      action='store',
                      dest='config_set',
                      help='Environment option',
                      default='Deploy')

    options, args = parser.parse_args()

    config_set = options.config_set

    print(config_set)

    assert config_set in config.get('Global', 'application.environment.choices'), \
        'config_set is not a valid option from those available: ' + \
        ','.join(config.get('Global', 'application.environment.choices'))


def prepare_dwh_data():
    df_dwh_fact_hotel = \
        spark.read.parquet(config.get(config_set, 'dwh.fact.hotel.path')) \
            .filter('start_date >= "2016-01-01"') \
            .select('fact_hotel_id',
                    'start_date',
                    'end_date',
                    'effective_date',
                    'dim_booking_id',
                    'dim_supplier_id',
                    'dim_traveler_profile_id',
                    F.col('issuing_country_id').alias('dim_location_id'))
    df_dwh_dim_booking = \
        spark.read.parquet(config.get(config_set, 'dwh.dim.booking.path')) \
            .filter('booking_locator is not null') \
            .select('dim_booking_id',
                    'booking_locator')
    df_dwh_dim_supplier = \
        spark.read.parquet(config.get(config_set, 'dwh.dim.supplier.path')) \
            .filter('discontinue_date="2000-01-01" or discontinue_date="2999-12-31"') \
            .select('harp_key',
                    'dim_supplier_id')
    df_dwh_dim_traveler_profile = \
        spark.read.parquet(config.get(config_set, 'dwh.dim.traveler.profile.path')) \
            .select('dim_traveler_profile_id',
                    'traveler_guid',
                    F.col('country_code').alias('trav_country'))
    df_dwh_dim_location = spark.read.parquet(config.get(config_set, 'dwh.dim.location.path')) \
        .select('dim_location_id',
                F.col('country_code').alias('loc_country'))

    regexp_pattern = '[^a-zA-Z0-9]+'

    df_dwh = \
        df_dwh_fact_hotel \
            .join(df_dwh_dim_booking, on='dim_booking_id') \
            .join(df_dwh_dim_supplier, on='dim_supplier_id') \
            .join(df_dwh_dim_traveler_profile, on='dim_traveler_profile_id') \
            .join(df_dwh_dim_location, on='dim_location_id', how='left') \
            .select('fact_hotel_id',
                    'start_date',
                    'end_date',
                    'effective_date',
                    'booking_locator',
                    'harp_key',
                    'traveler_guid',
                    'trav_country',
                    'loc_country',
                    F.when(F.col('trav_country') == F.col('loc_country'), 'N').otherwise('Y').alias('emulation_flag')
                    ) \
            .withColumn('concat_dwh_pnr', F.concat('start_date', 'end_date', 'booking_locator')) \
            .withColumn('full_mk',
                        F.regexp_replace(F.concat('concat_dwh_pnr',
                                                  'harp_key',
                                                  'traveler_guid'),
                                         regexp_pattern, '')) \
            .withColumn('prop_mk', F.regexp_replace(F.concat('concat_dwh_pnr', 'harp_key'), regexp_pattern, '')) \
            .withColumn('pnr_mk', F.regexp_replace(F.concat('concat_dwh_pnr'), regexp_pattern, '')) \
            .drop('concat_dwh_pnr')

    df_dwh_deduped = df_dwh \
        .withColumn('rk',
                    F.rank().over(Window
                                  .partitionBy(F.col('full_mk'))
                                  .orderBy(F.col('effective_date').desc(), F.col('fact_hotel_id').desc()))) \
        .filter(F.col('rk') == 1) \
        .drop('rk') \
        .distinct() \
        .withColumn('effective_date_year', F.year('effective_date')) \
        .withColumn('effective_date_month', F.month('effective_date')) \
        .withColumn('effective_date_day', F.dayofmonth('effective_date'))

    # sbx_dst.sw_hotel_hub_dwh_mapping_new_1
    dump_partitioned_dataframe(df_dwh_deduped,
                               ['effective_date_year', 'effective_date_month', 'effective_date_day'],
                               config.get(config_set, 'hotel.hub.dwh.mapping.new.1.path')
                               )


def merge_hcp_dwh_data():
    df_dwh_deduped = spark.read.parquet(config.get(config_set, 'hotel.hub.dwh.mapping.new.1.path'))

    df_hcp_txns_new = spark.read.parquet(config.get(config_set, 'hcp.txns.new.path'))

    df_hcp_txns_new_1 = df_hcp_txns_new \
        .filter(F.col('traveller_portrait_guid').isNotNull()) \
        .filter(F.col('harp_property_id_no').isNotNull()) \
        .join(df_dwh_deduped
              .filter('traveler_guid <> "UNKNOWN"')
              .filter('harp_key <> "-1"')
              .select('full_mk',
                      'fact_hotel_id',
                      'emulation_flag'),
              on='full_mk') \
        .withColumn('match_level', F.lit(4)) \
        .persist()

    df_hcp_txns_new_2 = df_hcp_txns_new \
        .filter(F.col('harp_property_id_no').isNotNull()) \
        .join(df_dwh_deduped
              .filter('harp_key <> "-1"')
              .select('prop_mk',
                      'fact_hotel_id',
                      'emulation_flag'),
              on='prop_mk') \
        .join(df_hcp_txns_new_1, on='hotelhub_booking_ref', how='left_anti') \
        .withColumn('match_level', F.lit(3)) \
        .persist()

    df_hcp_txns_new_3 = df_hcp_txns_new \
        .join(df_dwh_deduped.select('pnr_mk',
                                    'fact_hotel_id',
                                    'emulation_flag'),
              on='pnr_mk') \
        .join(df_hcp_txns_new_1, on='hotelhub_booking_ref', how='left_anti') \
        .join(df_hcp_txns_new_2, on='hotelhub_booking_ref', how='left_anti') \
        .withColumn('match_level', F.lit(2)) \
        .persist()

    df_hcp_txns_new_4 = df_hcp_txns_new \
        .join(df_hcp_txns_new_1, on='hotelhub_booking_ref', how='left_anti') \
        .join(df_hcp_txns_new_2, on='hotelhub_booking_ref', how='left_anti') \
        .join(df_hcp_txns_new_3, on='hotelhub_booking_ref', how='left_anti') \
        .withColumn('fact_hotel_id', F.lit(-1).cast('decimal')) \
        .withColumn('emulation_flag', F.lit('N')) \
        .withColumn('match_level', F.lit(-1)) \
        .persist()

    df_hcp_txns_new_fin = df_hcp_txns_new_1 \
        .union(df_hcp_txns_new_2.select(df_hcp_txns_new_1.columns)) \
        .union(df_hcp_txns_new_3.select(df_hcp_txns_new_1.columns)) \
        .persist()

    # Dedupe df_hcp_txns_new_fin based on fact_hotel_id
    df_hcp_txns_new_fin_a = df_hcp_txns_new_fin \
        .withColumn('rk_f', F.row_number().over(Window
                                                .partitionBy(F.col('fact_hotel_id'))
                                                .orderBy(F.col('lastmodified_datetime').desc(),
                                                         F.col('match_level').desc()))) \
        .filter('rk_f = 1') \
        .persist()

    # Dedupe df_hcp_txns_new_4 based on hotelhub_booking_ref and add missing hotelhub_booking_ref into fin_a
    # TODO: strange that were partitioning and ordering by the same field within the windowing function...
    df_hcp_txns_new_fin_b = df_hcp_txns_new_4 \
        .withColumn('rk_f', F.row_number().over(Window
                                                .partitionBy(F.col('hotelhub_booking_ref'))
                                                .orderBy(F.col('hotelhub_booking_ref')))) \
        .filter('rk_f = 1')

    df_hcp_txns_new_fin_a = df_hcp_txns_new_fin_a \
        .union(df_hcp_txns_new_fin_b
               .join(df_hcp_txns_new_fin_a, on='hotelhub_booking_ref', how='left_anti')
               .select(df_hcp_txns_new_fin_a.columns))

    df_hcp_txns_new_fin_1 = df_hcp_txns_new_fin_a \
        .withColumn('rnk_f', F.rank().over(Window
                                           .partitionBy(F.col('hotelhub_booking_ref'))
                                           .orderBy(F.col('fact_hotel_id').desc(),
                                                    F.col('match_level').desc()))) \
        .filter('rnk_f = 1')

    # Redundant field in place of legacy device_type determination
    df_hcp_txns_new_fin_f = df_hcp_txns_new_fin_1 \
        .withColumn('device_type', F.col('client_booking_channel'))

    dump_partitioned_dataframe(df_hcp_txns_new_fin_f,
                               ['date_created_year', 'date_created_month', 'date_created_day'],
                               config.get(config_set, 'hcp.txns.new.fin.f.path'),
                               config.get(config_set, 'hcp.txns.new.fin.f.table')
                               )

    # Clean up
    df_hcp_txns_new_1.unpersist()
    df_hcp_txns_new_2.unpersist()
    df_hcp_txns_new_3.unpersist()
    df_hcp_txns_new_4.unpersist()
    df_hcp_txns_new_fin.unpersist()
    df_hcp_txns_new_fin_a.unpersist()


def create_oracle_output():
    df_hcp_match_final = spark.read.parquet(config.get(config_set, 'hcp.txns.new.fin.f.path'))

    df_hcp_yesterdays_data = \
        spark.read.parquet(config.get(config_set, 'hcp.txns.new.fin.f.d2.path')) \
            .filter(F.col('full_mk').isNotNull()) \
            .filter(F.col('prop_mk').isNotNull()) \
            .filter(F.col('pnr_mk').isNotNull())

    join_fields = ['lastmodified_datetime', 'full_mk', 'prop_mk', 'pnr_mk', 'match_level', 'new_rate_bucket']

    df_hcp_yesterday = df_hcp_yesterdays_data \
        .withColumn('match_flag', F.lit('Y')) \
        .join(df_hcp_match_final.select(join_fields),
              on=join_fields) \
        .persist()

    df_hcp_today = df_hcp_match_final \
        .withColumn('dl_last_modified_date', F.current_date()) \
        .withColumn('rk', F.lit(1)) \
        .withColumn('rk_f', F.lit(1)) \
        .join(df_hcp_yesterday.select(join_fields + ['match_flag']),
              on=join_fields,
              how='left') \
        .filter(F.col('match_flag').isNull())

    # TODO: Remove redundant fields and remnants of deduping keys which only contain a single value
    df_hcp_oracle = \
        cast_fields_for_oracle_output(df_hcp_yesterday) \
            .union(cast_fields_for_oracle_output(df_hcp_today)) \
            .withColumn('dwh_rk',
                        F.row_number().over(Window
                                            .partitionBy(F.col('pnr_mk'))
                                            .orderBy(F.col('pnr_mk').desc()))) \
            .filter(F.col('dwh_rk') == 1) \
            .drop('dwh_rk')

    # TODO: Investigate partitioning of Oracle data suitable for end-user
    # Dump to staging path since Spark cannot simultaneously read and write to the same path
    dump_dataframe(df=df_hcp_oracle,
                   path=config.get(config_set, 'hcp.txns.new.fin.f.d2.staging.path'),
                   table_name=config.get(config_set, 'hcp.txns.new.fin.f.d2.staging.table'))
    df_hcp_yesterday.unpersist()
    df_hcp_oracle = spark.read.parquet(config.get(config_set, 'hcp.txns.new.fin.f.d2.staging.path'))
    dump_dataframe(df=df_hcp_oracle,
                   path=config.get(config_set, 'hcp.txns.new.fin.f.d2.path'),
                   table_name=config.get(config_set, 'hcp.txns.new.fin.f.d2.table'))

    # Oracle table for EDM
    dump_dataframe(df=df_hcp_oracle
                   .withColumnRenamed('rate_bucket', 'original_rate_bucket')
                   .withColumnRenamed('new_rate_bucket', 'rate_bucket'),
                   path=config.get(config_set, 'hcp.txns.to.dwh.f.path'),
                   table_name=config.get(config_set, 'hcp.txns.to.dwh.f.table')
                   )


def cast_fields_for_oracle_output(df):
    return df.select('booking_status',
                     'hotelhub_booking_ref',
                     'confirmation_ref',
                     'cancellation_ref',
                     'pnr',
                     'pnr_type',
                     'hotelhub_mode',
                     'market',
                     'client_client_top_name',
                     'client_sub_unit_client_name',
                     'customer_agency_name',
                     'hotel_name',
                     'city',
                     'country',
                     'star_rating',
                     'date_in',
                     'date_out',
                     F.col('nights').cast('string').alias('nights'),
                     F.col('num_of_rooms').cast('string').alias('num_of_rooms'),
                     F.col('num_of_guest').cast('string').alias('num_of_guest'),
                     'out_policy_reason',
                     'booking_source',
                     'rate_description',
                     'cancellation_policy',
                     'rateperday_amount',
                     'rateperday_currcode',
                     F.col('agency_priority').cast('string').alias('agency_priority'),
                     F.col('customer_priority').cast('string').alias('customer_priority'),
                     'payment_mode',
                     'rateperday_eur',
                     'rateperday_gbp',
                     'rateperday_usd',
                     'totalamount_booked_currcode',
                     F.col('totalamount_booked').cast('string').alias('totalamount_booked'),
                     'local_currency_code',
                     'rateperday_lcc',
                     'totalamount_eur',
                     'totalamount_gbp',
                     'totalamount_usd',
                     'totalamount_lcc',
                     'booked_rate_type_code',
                     'content_source',
                     'gds_chain_name',
                     'created_date',
                     'createdby_user',
                     'confirmedby_user',
                     F.col('cancel_datetime').cast('timestamp').alias('cancel_datetime'),
                     'abandon_by_user',
                     'obt_pnr',
                     'client_booking_channel',
                     'rate_access_code_booked',
                     'commission_type',
                     'commission_currency',
                     F.col('commission_amount').cast('string').alias('commission_amount'),
                     F.col('estimated_income_due').cast('string').alias('estimated_income_due'),
                     'rate_access_code_shopped',
                     'hotelhub_property_id',
                     F.col('harp_property_id_no').cast('string').alias('harp_property_id_no'),
                     'content_source_property_id',
                     F.col('aggregator_booking_commission').cast('string').alias('aggregator_booking_commission'),
                     F.col('aggregator_revenue_value').cast('string').alias('aggregator_revenue_value'),
                     F.col('aggregator_revenue_share').cast('string').alias('aggregator_revenue_share'),
                     'aggregator_currency',
                     'rate_change',
                     'rate_access_code_returned',
                     'back_office_account_number',
                     'traveller_portrait_guid',
                     F.col('booking_st_tm').cast('timestamp').alias('booking_st_tm'),
                     F.col('booking_en_tm').cast('timestamp').alias('booking_en_tm'),
                     'step0_time',
                     'step1_time',
                     'step2_time',
                     'step3_time',
                     'step4_time',
                     'gds_shopped_for_rate',
                     'cheap_client_neg_rate',
                     'cheap_client_neg_rate_currcode',
                     'cheap_client_neg_rate_description',
                     'cheap_client_or_cwv_rate',
                     'cheap_client_or_cwv_neg_rate_currcode',
                     'cheap_client_or_cwv_rate_description',
                     'cheap_gds_published_rate',
                     'cheap_gds_neg_rate_currcode',
                     'cheap_gds_published_rate_description',
                     'cheap_bookingcom_rate',
                     'cheap_bookingcom_rate_currcode',
                     'cheap_bookingcom_rate_description',
                     F.col('branch_iata').cast('string').alias('branch_iata'),
                     'on_request_indicator',
                     F.col('lastmodified_datetime').cast('timestamp').alias('lastmodified_datetime'),
                     F.col('modified_count').cast('string').alias('modified_count'),
                     'booking_time_duration',
                     'rate_bucket',
                     'back_office',
                     'client_sub_unit_client_id',
                     'missed_saving',
                     'realised_saving',
                     F.col('popular_hotel').cast('string').alias('popular_hotel'),
                     'hotel_rank',
                     'hotel_total',
                     'agency_source_name',
                     'client_client_top_id',
                     'err_desc',
                     'aggregator_property_type',
                     F.col('hotel_bucket_simplified').cast('string').alias('hotel_bucket_simplified'),
                     'gds_commission_text',
                     F.col('avlb_htl_count').cast('string').alias('avlb_htl_count'),
                     'offer',
                     'aaa_rate',
                     'error_code',
                     'rate_id',
                     'new_rate_bucket',
                     'payment_type_used',
                     'channel_type',
                     F.lit('null').cast('long').alias('rw_num'),
                     F.lit('null').cast('long').alias('nrk'),
                     'full_mk',
                     'prop_mk',
                     'pnr_mk',
                     F.col('row_num').cast('short').alias('row_num'),
                     F.col('dedupe_key').cast('long').alias('dedupe_key'),
                     F.col('rk').cast('long').alias('rk'),
                     'fact_hotel_id',
                     'emulation_flag',
                     F.col('match_level').cast('byte').alias('match_level'),
                     F.col('rk_f').cast('long').alias('rk_f'),
                     F.lit('null').cast('long').alias('rnk_f'),
                     'device_type',
                     F.col('dl_last_modified_date').cast('timestamp').alias('dl_last_modified_date'),
                     )


def main():
    application_config()

    parse_python_arguments()

    initialise_spark_session()

    ingest_hcp_trans_data()

    transform_hcp_trans_data()

    prepare_dwh_data()

    merge_hcp_dwh_data()

    create_oracle_output()

    spark.stop()


if __name__ == "__main__":
    main()