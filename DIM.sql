--上市公司基本信息维度表
DROP TABLE IF EXISTS dim_company_info;
CREATE EXTERNAL TABLE dim_company_info(
    `id` STRING COMMENT 'id',
    `code` STRING COMMENT '证券代码',
    `full_name` STRING COMMENT '公司名称',
    `short_name` STRING COMMENT '公司简称',
    `a_code` STRING COMMENT 'A股股票代码',
    `b_code` STRING COMMENT 'B股股票代码',
    `h_code` STRING COMMENT 'H股股票代码',
    `fullname_en` STRING COMMENT '英文名称',
    `shortname_en` STRING COMMENT '英文简称',
    `legal_representative` STRING COMMENT '法人代表',
    `register_location` STRING COMMENT '注册地址',
    `office_address` STRING COMMENT '办公地址',
    `zipcode` STRING COMMENT '邮政编码',
    `register_capital` STRING COMMENT '注册资金',
    `currency_id` STRING COMMENT '货币编码',
    `currency` STRING COMMENT '货币名称',
    `establish_date` STRING COMMENT '成立日期',
    `website` STRING COMMENT '机构网址',
    `email` STRING COMMENT '电子信箱',
    `contact_number` STRING COMMENT '联系电话',
    `fax_number` STRING COMMENT '联系传真',
    `main_business` STRING COMMENT '主营业务',
    `business_scope` STRING COMMENT '经营范围',
    `description` STRING COMMENT '机构简介',
    `tax_number` STRING COMMENT '税务登记号',
    `license_number` STRING COMMENT '法人营业执照号',
    `pub_newspaper` STRING COMMENT '指定信息披露报刊',
    `pub_website` STRING COMMENT '指定信息披露网站',
    `secretary` STRING COMMENT '董事会秘书',
    `secretary_number` STRING COMMENT '董秘联系电话',
    `secretary_fax` STRING COMMENT '董秘联系传真',
    `secretary_email` STRING COMMENT '董秘电子邮箱',
    `security_representative` STRING COMMENT '证券事务代表',
    `province_id` STRING COMMENT '所属省份编码',
    `province` STRING COMMENT '所属省份',
    `city_id` STRING COMMENT '所属城市编码',
    `city` STRING COMMENT '所属城市',
    `industry_id` STRING COMMENT '行业编码',
    `industry_one` STRING COMMENT '行业一级分类',
    `industry_two` STRING COMMENT '行业二级分类',
    `cpafirm` STRING COMMENT '会计师事务所',
    `lawfirm` STRING COMMENT '律师事务所',
    `ceo` STRING COMMENT '总经理',
    `comments` STRING COMMENT '备注',
    `industry_attr_values` ARRAY<STRUCT<attr_id:STRING,industry_code:STRING,industry_name:STRING,type:STRING>> COMMENT '行业属性'
) COMMENT '上市公司基本信息维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/stockdata/dim/dim_company_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

--行业信息维度表
DROP TABLE IF EXISTS dim_industry;
CREATE EXTERNAL TABLE dim_industry(
    `id` STRING COMMENT '编号',
    `industry_id` STRING  COMMENT '行业id',
    `name` STRING  COMMENT '名称',
    `start_date` STRING  COMMENT '开始日期',
    `type` STRING  COMMENT '类型'
) COMMENT '行业信息维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/stockdata/dim/dim_company_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

--地区维度表
DROP TABLE IF EXISTS dim_province;
CREATE EXTERNAL TABLE dim_province (
    `id` STRING COMMENT 'id',
    `province_name` STRING COMMENT '省市名称',
    `area_code` STRING COMMENT '地区编码',
    `iso_code` STRING COMMENT 'ISO-3166编码，供可视化使用',
    `iso_3166_2` STRING COMMENT 'IOS-3166-2编码，供可视化使用',
    `region_id` STRING COMMENT '地区id',
    `region_name` STRING COMMENT '地区名称'
) COMMENT '地区维度表'
STORED AS PARQUET
LOCATION '/warehouse/stockdata/dim/dim_province/';

--临时维度表
DROP TABLE IF EXISTS tmp_dim_province;
CREATE EXTERNAL TABLE tmp_dim_province (
    `id` STRING COMMENT 'id',
    `province_name` STRING COMMENT '省市名称',
    `area_code` STRING COMMENT '地区编码',
    `iso_code` STRING COMMENT 'ISO-3166编码，供可视化使用',
    `iso_3166_2` STRING COMMENT 'IOS-3166-2编码，供可视化使用',
    `region_id` STRING COMMENT '地区id',
    `region_name` STRING COMMENT '地区名称'
) COMMENT '地区维度表'
STORED AS PARQUET
LOCATION '/warehouse/stockdata/tmp/tmp_dim_province/';

--插入语句
insert overwrite table dim_province select * from tmp_dim_province;

--时间维度表
DROP TABLE IF EXISTS dim_date_info;
CREATE EXTERNAL TABLE dim_date_info(
    `date_id` STRING COMMENT '日',
    `week_id` STRING COMMENT '周ID',
    `week_day` STRING COMMENT '周几',
    `day` STRING COMMENT '每月的第几天',
    `month` STRING COMMENT '第几月',
    `quarter` STRING COMMENT '第几季度',
    `year` STRING COMMENT '年',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
STORED AS PARQUET
LOCATION '/warehouse/stockdata/dim/dim_date_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

--时间维度临时表
DROP TABLE IF EXISTS tmp_dim_date_info;
CREATE EXTERNAL TABLE tmp_dim_date_info (
    `date_id` STRING COMMENT '日',
    `week_id` STRING COMMENT '周ID',
    `week_day` STRING COMMENT '周几',
    `day` STRING COMMENT '每月的第几天',
    `month` STRING COMMENT '第几月',
    `quarter` STRING COMMENT '第几季度',
    `year` STRING COMMENT '年',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/stockdata/tmp/tmp_dim_date_info/';

--hdfs->tmp_dim_date_info

--导入数据
insert overwrite table dim_date_info select * from tmp_dim_date_info;

--导入维度表脚本

--set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with industry_attr as(
    select code ids_code,
           collect_set(named_struct('attr_id',id,'industry_code',industry_code,'industry_name',industry_name,'type',type)) industry_attr_values
           from ods_stk_industry
		   where dt='2021-08-08'
           group by code
)

--insert overwrite table stockdata.dim_company_info partition(dt='2021-08-08')
select  sc.id,
        sc.code,
        sc.full_name,
        sc.short_name,
        sc.a_code,
        sc.b_code,
        sc.h_code,
        sc.fullname_en,
        sc.shortname_en,
        sc.legal_representative,
        sc.register_location,
        sc.office_address,
        sc.zipcode,
        sc.register_capital,
        sc.currency_id,
        sc.currency,
        sc.establish_date,
        sc.website,
        sc.email,
        sc.contact_number,
        sc.fax_number,
        sc.main_business,
        sc.business_scope,
        sc.description,
        sc.tax_number,
        sc.license_number,
        sc.pub_newspaper,
        sc.pub_website,
        sc.secretary,
        sc.secretary_number,
        sc.secretary_fax,
        sc.secretary_email,
        sc.security_representative,
        sc.province_id,
        sc.province,
        sc.city_id,
        sc.city,
        sc.industry_id,
        sc.industry_one,
        sc.industry_two,
        sc.cpafirm,
        sc.lawfirm,
        sc.ceo,
        sc.comments,
        ia.industry_attr_values industry_attr_values
      from stockdata.ods_stk_company_info sc
      left join industry_attr ia on ia.ids_code=sc.code
      where dt='2021-08-08';

