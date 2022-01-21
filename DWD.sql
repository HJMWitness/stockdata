--上市公司基本信息  dwd_company_data
-------------------------建表语句start------------------------------
DROP TABLE IF EXISTS dwd_company_data;
CREATE EXTERNAL TABLE dwd_company_data (
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
    `cpafirm` STRING COMMENT '会计师事务所',
    `lawfirm` STRING COMMENT '律师事务所',
    `ceo` STRING COMMENT '总经理',
    `comments` STRING COMMENT '备注',
    `sw_l1` STRING COMMENT 'sw_l1',
    `sw_l2` STRING COMMENT 'sw_l2',
    `sw_l3` STRING COMMENT 'sw_l3'

) COMMENT '上市公司基本信息'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/stockdata/dwd/dwd_company_data/'
TBLPROPERTIES ("parquet.compression"="lzo");

--脚本
dwd_company_data="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table stockdata.dwd_company_data partition(dt='$do_date')
select  t.id,
        t.code,
        t.full_name,
        t.short_name,
        t.a_code,
        t.b_code,
        t.h_code,
        t.fullname_en,
        t.shortname_en,
        t.legal_representative,
        t.register_location,
        t.office_address,
        t.zipcode,
        t.register_capital,
        t.currency_id,
        t.currency,
        t.establish_date,
        t.website,
        t.email,
        t.contact_number,
        t.fax_number,
        t.main_business,
        t.business_scope,
        t.description,
        t.tax_number,
        t.license_number,
        t.pub_newspaper,
        t.pub_website,
        t.secretary,
        t.secretary_number,
        t.secretary_fax,
        t.secretary_email,
        t.security_representative,
        t.province_id,
        t.province,
        t.city_id,
        t.city,
        t.cpafirm,
        t.lawfirm,
        t.ceo,
        t.comments,
        case when i.type='sw_l1' then  i.name
                 else null end sw_l1,
        case when i.type='sw_l2' then  i.name
                 else null end sw_l2,
        case when i.type='sw_l3' then  i.name
                 else null end sw_l3
from
(
select
    c.id,
    c.code,
    c.full_name,
    c.short_name,
    c.a_code,
    c.b_code,
    c.h_code,
    c.fullname_en,
    c.shortname_en,
    c.legal_representative,
    c.register_location,
    c.office_address,
    c.zipcode,
    c.register_capital,
    c.currency_id,
    c.currency,
    c.establish_date,
    c.website,
    c.email,
    c.contact_number,
    c.fax_number,
    c.main_business,
    c.business_scope,
    c.description,
    c.tax_number,
    c.license_number,
    c.pub_newspaper,
    c.pub_website,
    c.secretary,
    c.secretary_number,
    c.secretary_fax,
    c.secretary_email,
    c.security_representative,
    c.province_id,
    c.province,
    c.city_id,
    c.city,
    c.cpafirm,
    c.lawfirm,
    c.ceo,
    c.comments,
    c.dt,
    st.industry_code industry_code
from stockdata.dim_company_info c
lateral view explode(c.industry_attr_values) t as st
where c.dt='$do_date'
) t
left join stockdata.ods_industry i on i.industry_id = t.industry_code
where i.dt='$do_date'
;"

-------------------------建表语句end--------------------------------



--资产负债表、利润表、现金流量表、衍生指标 dwd_finance_data
-------------------------建表语句start------------------------------
DROP TABLE IF EXISTS dwd_finance_data;
CREATE EXTERNAL TABLE dwd_finance_data (
    `id` STRING COMMENT 'id', --balance表的id
    `code` STRING COMMENT '股票代码',
    `company_id` STRING COMMENT 'dim_company_info的id',
    `pubDate` STRING COMMENT '发布日期',--balance表的pubDate
    `statDate` STRING COMMENT '开始日期',--balance表的statDate
	`bl_cash_equivalents` DECIMAL(20,4) COMMENT '货币资金',
    `bl_settlement_provi` DECIMAL(20,4) COMMENT '结算备付金',
    `bl_lend_capital` DECIMAL(20,4) COMMENT '拆出资金',
    `bl_trading_assets` DECIMAL(20,4) COMMENT '交易性金融资产',
    `bl_bill_receivable` DECIMAL(20,4) COMMENT '应收票据',
    `bl_account_receivable` DECIMAL(20,4) COMMENT '应收账款',
    `bl_advance_payment` DECIMAL(20,4) COMMENT '预付款项',
    `bl_insurance_receivables` DECIMAL(20,4) COMMENT '应收保费',
    `bl_reinsurance_receivables` DECIMAL(20,4) COMMENT '	应收分保账款',
    `bl_reinsurance_contract_reserves_receivable` DECIMAL(20,4) COMMENT '应收分保合同准备金',
    `bl_interest_receivable` DECIMAL(20,4) COMMENT '应收利息',
    `bl_dividend_receivable` DECIMAL(20,4) COMMENT '应收股利',
    `bl_other_receivable` DECIMAL(20,4) COMMENT '其他应收款',
    `bl_bought_sellback_assets` DECIMAL(20,4) COMMENT '买入返售金融资产',
    `bl_inventories` DECIMAL(20,4) COMMENT '存货',
    `bl_non_current_asset_in_one_year` DECIMAL(20,4) COMMENT '一年内到期的非流动资产',
    `bl_other_current_assets` DECIMAL(20,4) COMMENT '其他流动资产',
    `bl_total_current_assets` DECIMAL(20,4) COMMENT '流动资产合计',
    `bl_loan_and_advance` DECIMAL(20,4) COMMENT '发放委托贷款及垫款',
    `bl_hold_for_sale_assets` DECIMAL(20,4) COMMENT '可供出售金融资产',
    `bl_hold_to_maturity_investments` DECIMAL(20,4) COMMENT '持有至到期投资',
    `bl_longterm_receivable_account` DECIMAL(20,4) COMMENT '长期应收款',
    `bl_longterm_equity_invest` DECIMAL(20,4) COMMENT '长期股权投资',
    `bl_investment_property` DECIMAL(20,4) COMMENT '投资性房地产',
    `bl_fixed_assets` DECIMAL(20,4) COMMENT '固定资产',
    `bl_constru_in_process` DECIMAL(20,4) COMMENT '在建工程',
    `bl_construction_materials` DECIMAL(20,4) COMMENT '工程物资',
    `bl_fixed_assets_liquidation` DECIMAL(20,4) COMMENT '固定资产清理',
    `bl_biological_assets` DECIMAL(20,4) COMMENT '生产性生物资产',
    `bl_development_expenditure` DECIMAL(20,4) COMMENT '开发支出',
    `bl_good_will` DECIMAL(20,4) COMMENT '商誉',
    `bl_long_deferred_expense` DECIMAL(20,4) COMMENT '长期待摊费用',
    `bl_deferred_tax_assets` DECIMAL(20,4) COMMENT '递延所得税资产',
    `bl_other_non_current_assets` DECIMAL(20,4) COMMENT '其他非流动资产',
    `bl_total_non_current_assets` DECIMAL(20,4) COMMENT '非流动资产合计',
    `bl_total_assets` DECIMAL(20,4) COMMENT '资产总计',
    `bl_shortterm_loan` DECIMAL(20,4) COMMENT '短期借款',
    `bl_borrowing_from_centralbank` DECIMAL(20,4) COMMENT '向中央银行借款',
    `bl_deposit_in_interbank` DECIMAL(20,4) COMMENT '吸收存款及同业存放',
    `bl_borrowing_capital` DECIMAL(20,4) COMMENT '拆入资金',
    `bl_trading_liability` DECIMAL(20,4) COMMENT '交易性金融负债',
    `bl_notes_payable` DECIMAL(20,4) COMMENT '应付票据',
    `bl_accounts_payable` DECIMAL(20,4) COMMENT '应付账款',
    `bl_advance_peceipts` DECIMAL(20,4) COMMENT '预收款项',
    `bl_sold_buyback_secu_proceeds` DECIMAL(20,4) COMMENT '卖出回购金融资产款',
    `bl_commission_payable` DECIMAL(20,4) COMMENT '应付手续费及佣金',
    `bl_salaries_payable` DECIMAL(20,4) COMMENT '应付职工薪酬',
    `bl_taxs_payable` DECIMAL(20,4) COMMENT '应交税费',
    `bl_interest_payable` DECIMAL(20,4) COMMENT '应付利息',
    `bl_dividend_payable` DECIMAL(20,4) COMMENT '应付股利',
    `bl_other_payable` DECIMAL(20,4) COMMENT '其他应付款',
    `bl_reinsurance_payables` DECIMAL(20,4) COMMENT '应付分保账款',
    `bl_insurance_contract_reserves` DECIMAL(20,4) COMMENT '保险合同准备金',
    `bl_proxy_secu_proceeds` DECIMAL(20,4) COMMENT '代理买卖证券款',
    `bl_receivings_from_vicariously_sold_securities` DECIMAL(20,4) COMMENT '代理承销证券款',
    `bl_non_current_liability_in_one_year` DECIMAL(20,4) COMMENT '一年内到期的非流动负债',
    `bl_other_current_liability` DECIMAL(20,4) COMMENT '其他流动负债',
    `bl_total_current_liability` DECIMAL(20,4) COMMENT '流动负债合计',
    `bl_longterm_loan` DECIMAL(20,4) COMMENT '长期借款',
    `bl_bonds_payable` DECIMAL(20,4) COMMENT '应付债券',
    `bl_longterm_account_payable` DECIMAL(20,4) COMMENT '长期应付款',
    `bl_specific_account_payable` DECIMAL(20,4) COMMENT '专项应付款',
    `bl_estimate_liability` DECIMAL(20,4) COMMENT '预计负债',
    `bl_deferred_tax_liability` DECIMAL(20,4) COMMENT '递延所得税负债',
    `bl_other_non_current_liability` DECIMAL(20,4) COMMENT '其他非流动负债',
    `bl_total_non_current_liability` DECIMAL(20,4) COMMENT '非流动负债合计',
    `bl_total_liability` DECIMAL(20,4) COMMENT '负债合计',
    `bl_paidin_capital` DECIMAL(20,4) COMMENT '实收资本(或股本)',
    `bl_capital_reserve_fund` DECIMAL(20,4) COMMENT '资本公积金',
    `bl_treasury_stock` DECIMAL(20,4) COMMENT '库存股',
    `bl_specific_reserves` DECIMAL(20,4) COMMENT '	专项储备',
    `bl_surplus_reserve_fund` DECIMAL(20,4) COMMENT '盈余公积金',
    `bl_ordinary_risk_reserve_fund` DECIMAL(20,4) COMMENT '一般风险准备',
    `bl_retained_profit` DECIMAL(20,4) COMMENT '未分配利润',
    `bl_foreign_currency_report_conv_diff` DECIMAL(20,4) COMMENT '	外币报表折算差额',
    `bl_equities_parent_company_owners` DECIMAL(20,4) COMMENT '归属于母公司股东权益合计',
    `bl_minority_interests` DECIMAL(20,4) COMMENT '	少数股东权益',
    `bl_total_owner_equities` DECIMAL(20,4) COMMENT '	股东权益合计',
    `bl_total_sheet_owner_equities` DECIMAL(20,4) COMMENT '	负债和股东权益合计',
    `cf_goods_sale_and_service_render_cash` DECIMAL(20,4) COMMENT '销售商品、提供劳务收到的现金',
    `cf_net_deposit_increase` DECIMAL(20,4) COMMENT '客户存款和同业存放款项净增加额',
    `cf_net_borrowing_from_central_bank` DECIMAL(20,4) COMMENT '向中央银行借款净增加额',
    `cf_net_borrowing_from_finance_co` DECIMAL(20, 4) COMMENT '向其他金融机构拆入资金净增加额',
    `cf_net_original_insurance_cash` DECIMAL(20, 4) COMMENT '收到原保险合同保费取得的现金',
    `cf_net_cash_received_from_reinsurance_business` DECIMAL(20, 4) COMMENT '收到再保险业务现金净额',
    `cf_net_insurer_deposit_investment` DECIMAL(20, 4) COMMENT '保户储金及投资款净增加额',
    `cf_net_deal_trading_assets` DECIMAL(20, 4) COMMENT '处置交易性金融资产净增加额',
    `cf_interest_and_commission_cashin` DECIMAL(20, 4) COMMENT '收取利息、手续费及佣金的现金',
    `cf_net_increase_in_placements` DECIMAL(20, 4) COMMENT '拆入资金净增加额',
    `cf_net_buyback` DECIMAL(20, 4) COMMENT '回购业务资金净增加额',
    `cf_tax_levy_refund` DECIMAL(20, 4) COMMENT '收到的税费返还',
    `cf_other_cashin_related_operate` DECIMAL(20, 4) COMMENT '收到其他与经营活动有关的现金',
    `cf_subtotal_operate_cash_inflow` DECIMAL(20, 4) COMMENT '经营活动现金流入小计',
    `cf_goods_and_services_cash_paid` DECIMAL(20, 4) COMMENT '购买商品、接受劳务支付的现金',
    `cf_net_loan_and_advance_increase` DECIMAL(20, 4) COMMENT '客户贷款及垫款净增加额',
    `cf_net_deposit_in_cb_and_ib` DECIMAL(20, 4) COMMENT '存放中央银行和同业款项净增加额',
    `cf_original_compensation_paid` DECIMAL(20, 4) COMMENT '支付原保险合同赔付款项的现金',
    `cf_handling_charges_and_commission` DECIMAL(20, 4) COMMENT '支付利息、手续费及佣金的现金',
    `cf_policy_dividend_cash_paid` DECIMAL(20, 4) COMMENT '支付保单红利的现金',
    `cf_staff_behalf_paid` DECIMAL(20, 4) COMMENT '支付给职工以及为职工支付的现金',
    `cf_tax_payments` DECIMAL(20, 4) COMMENT '支付的各项税费',
    `cf_other_operate_cash_paid` DECIMAL(20, 4) COMMENT '支付其他与经营活动有关的现金',
    `cf_subtotal_operate_cash_outflow` DECIMAL(20, 4) COMMENT '经营活动现金流出小计',
    `cf_net_operate_cash_flow` DECIMAL(20, 4) COMMENT '经营活动产生的现金流量净额',
    `cf_invest_withdrawal_cash` DECIMAL(20, 4) COMMENT '收回投资收到的现金',
    `cf_invest_proceeds` DECIMAL(20, 4) COMMENT '取得投资收益收到的现金',
    `cf_fix_intan_other_asset_dispo_cash` DECIMAL(20, 4) COMMENT '处置固定资产、无形资产和其他长期资产收回的现金净额',
    `cf_net_cash_deal_subcompany` DECIMAL(20, 4) COMMENT '处置子公司及其他营业单位收到的现金净额',
    `cf_other_cash_from_invest_act` DECIMAL(20, 4) COMMENT '收到其他与投资活动有关的现金',
    `cf_subtotal_invest_cash_inflow` DECIMAL(20, 4) COMMENT '投资活动现金流入小计',
    `cf_fix_intan_other_asset_acqui_cash` DECIMAL(20, 4) COMMENT '购建固定资产、无形资产和其他长期资产支付的现金',
    `cf_invest_cash_paid` DECIMAL(20, 4) COMMENT '投资支付的现金',
    `cf_impawned_loan_net_increase` DECIMAL(20, 4) COMMENT '质押贷款净增加额',
    `cf_net_cash_from_sub_company` DECIMAL(20, 4) COMMENT '取得子公司及其他营业单位支付的现金净额',
    `cf_other_cash_to_invest_act` DECIMAL(20, 4) COMMENT '支付其他与投资活动有关的现金',
    `cf_subtotal_invest_cash_outflow` DECIMAL(20, 4) COMMENT '投资活动现金流出小计',
    `cf_net_invest_cash_flow` DECIMAL(20, 4) COMMENT '投资活动产生的现金流量净额',
    `cf_cash_from_invest` DECIMAL(20, 4) COMMENT '吸收投资收到的现金',
    `cf_cash_from_mino_s_invest_sub` DECIMAL(20, 4) COMMENT '子公司吸收少数股东投资收到的现金',
    `cf_cash_from_borrowing` DECIMAL(20, 4) COMMENT '取得借款收到的现金',
    `cf_cash_from_bonds_issue` DECIMAL(20, 4) COMMENT '发行债券收到的现金',
    `cf_other_finance_act_cash` DECIMAL(20, 4) COMMENT '收到其他与筹资活动有关的现金',
    `cf_subtotal_finance_cash_inflow` DECIMAL(20, 4) COMMENT '筹资活动现金流入小计',
    `cf_borrowing_repayment` DECIMAL(20, 4) COMMENT '偿还债务支付的现金',
    `cf_dividend_interest_payment` DECIMAL(20, 4) COMMENT '分配股利、利润或偿付利息支付的现金',
    `cf_proceeds_from_sub_to_mino_s` DECIMAL(20, 4) COMMENT '子公司支付给少数股东的股利、利润',
    `cf_other_finance_act_payment` DECIMAL(20, 4) COMMENT '支付其他与筹资活动有关的现金',
    `cf_subtotal_finance_cash_outflow` DECIMAL(20, 4) COMMENT '筹资活动现金流出小计',
    `cf_net_finance_cash_flow` DECIMAL(20, 4) COMMENT '筹资活动产生的现金流量净额',
    `cf_exchange_rate_change_effect` DECIMAL(20, 4) COMMENT '汇率变动对现金及现金等价物的影响',
    `cf_cash_equivalent_increase` DECIMAL(20, 4) COMMENT '现金及现金等价物净增加额',
    `cf_cash_equivalents_at_beginning` DECIMAL(20, 4) COMMENT '期初现金及现金等价物余额',
    `cf_cash_and_equivalents_at_end` DECIMAL(20, 4) COMMENT '期末现金及现金等价物余额(元)',
    `ic_total_operating_revenue` DECIMAL(20,4) COMMENT '营业总收入',
    `ic_operating_revenue` DECIMAL(20,4) COMMENT '	营业收入',
    `ic_interest_income` DECIMAL(20,4) COMMENT '利息收入',
    `ic_premiums_earned` DECIMAL(20,4) COMMENT '已赚保费',
    `ic_commission_income` DECIMAL(20,4) COMMENT '	手续费及佣金收入',
    `ic_total_operating_cost` DECIMAL(20,4) COMMENT '营业总成本',
    `ic_operating_cost` DECIMAL(20,4) COMMENT '营业成本',
    `ic_interest_expense` DECIMAL(20,4) COMMENT '利息支出',
    `ic_commission_expense` DECIMAL(20,4) COMMENT '手续费及佣金支出',
    `ic_refunded_premiums` DECIMAL(20,4) COMMENT '退保金',
    `ic_net_pay_insurance_claims` DECIMAL(20,4) COMMENT '赔付支出净额(元)',
    `ic_withdraw_insurance_contract_reserve` DECIMAL(20,4) COMMENT '提取保险合同准备金净额(元)',
    `ic_policy_dividend_payout` DECIMAL(20,4) COMMENT '保单红利支出(元)',
    `ic_reinsurance_cost` DECIMAL(20,4) COMMENT '分保费用(元)',
    `ic_operating_tax_surcharges` DECIMAL(20,4) COMMENT '营业税金及附加(元)',
    `ic_sale_expense` DECIMAL(20,4) COMMENT '销售费用(元)',
    `ic_administration_expense` DECIMAL(20,4) COMMENT '管理费用(元)',
    `ic_financial_expense` DECIMAL(20,4) COMMENT '	财务费用(元)',
    `ic_asset_impairment_loss` DECIMAL(20,4) COMMENT '资产减值损失(元)',
    `ic_fair_value_variable_income` DECIMAL(20,4) COMMENT '公允价值变动收益(元)',
    `ic_investment_income` DECIMAL(20,4) COMMENT '投资收益(元)',
    `ic_invest_income_associates` DECIMAL(20,4) COMMENT '对联营企业和合营企业的投资收益(元)',
    `ic_exchange_income` DECIMAL(20,4) COMMENT '汇兑收益(元)',
    `ic_operating_profit` DECIMAL(20,4) COMMENT '营业利润(元)',
    `ic_non_operating_revenue` DECIMAL(20,4) COMMENT '营业外收入(元)',
    `ic_non_operating_expense` DECIMAL(20,4) COMMENT '营业外支出(元)',
    `ic_disposal_loss_non_current_liability` DECIMAL(20,4) COMMENT '非流动资产处置净损失(元)',
    `ic_total_profit` DECIMAL(20,4) COMMENT '利润总额(元)',
    `ic_income_tax_expense` DECIMAL(20,4) COMMENT '所得税费用(元)',
    `ic_net_profit` DECIMAL(20,4) COMMENT '净利润(元)',
    `ic_np_parent_company_owners` DECIMAL(20,4) COMMENT '归属于母公司股东的净利润(元)',
    `ic_minority_profit` DECIMAL(20,4) COMMENT '少数股东损益(元)',
    `ic_basic_eps` DECIMAL(20,4) COMMENT '基本每股收益(元)',
    `ic_diluted_eps` DECIMAL(20,4) COMMENT '稀释每股收益(元)',
    `ic_other_composite_income` DECIMAL(20,4) COMMENT '其他综合收益(元)',
    `ic_total_composite_income` DECIMAL(20,4) COMMENT '综合收益总额(元)',
    `ic_ci_parent_company_owners` DECIMAL(20,4) COMMENT '归属于母公司所有者的综合收益总额(元)',
    `ic_ci_minority_owners` DECIMAL(20,4) COMMENT '归属于少数股东的综合收益总额(元)'

) COMMENT '财务数据表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/stockdata/dwd/dwd_finance_data/'
TBLPROPERTIES ("parquet.compression"="lzo");
-------------------------建表语句end--------------------------------

-------------------------insert语句start--------------------------------
#!/bin/bash
APP=stockdata

if [ -n "$2" ] ;then
   do_date=$2
else
   echo "请传入日期参数"
   exit
fi

dwd_finance_data="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table stockdata.dwd_finance_data partition(dt='$do_date')
select
    bl.id id,
    bl.code code,
    dci.id company_id,
    bl.pubDate pubDate,
    bl.statDate statDate,
    sum(nvl(bl.cash_equivalents,0))  bl_cash_equivalents,
    sum(nvl(bl.settlement_provi,0))  bl_settlement_provi,
    sum(nvl(bl.lend_capital,0))  bl_lend_capital,
    sum(nvl(bl.trading_assets,0))  bl_trading_assets,
    sum(nvl(bl.bill_receivable,0))  bl_bill_receivable,
    sum(nvl(bl.account_receivable,0))  bl_account_receivable,
    sum(nvl(bl.advance_payment,0))  bl_advance_payment,
    sum(nvl(bl.insurance_receivables,0))  bl_insurance_receivables,
    sum(nvl(bl.reinsurance_receivables,0))  bl_reinsurance_receivables,
    sum(nvl(bl.reinsurance_contract_reserves_receivable,0))  bl_reinsurance_contract_reserves_receivable,
    sum(nvl(bl.interest_receivable,0))  bl_interest_receivable,
    sum(nvl(bl.dividend_receivable,0))  bl_dividend_receivable,
    sum(nvl(bl.other_receivable,0))  bl_other_receivable,
    sum(nvl(bl.bought_sellback_assets,0))  bl_bought_sellback_assets,
    sum(nvl(bl.inventories,0))  bl_inventories,
    sum(nvl(bl.non_current_asset_in_one_year,0))  bl_non_current_asset_in_one_year,
    sum(nvl(bl.other_current_assets,0))  bl_other_current_assets,
    sum(nvl(bl.total_current_assets,0))  bl_total_current_assets,
    sum(nvl(bl.loan_and_advance,0))  bl_loan_and_advance,
    sum(nvl(bl.hold_for_sale_assets,0))  bl_hold_for_sale_assets,
    sum(nvl(bl.hold_to_maturity_investments,0))  bl_hold_to_maturity_investments,
    sum(nvl(bl.longterm_receivable_account,0))  bl_longterm_receivable_account,
    sum(nvl(bl.longterm_equity_invest,0))  bl_longterm_equity_invest,
    sum(nvl(bl.investment_property,0))  bl_investment_property,
    sum(nvl(bl.fixed_assets,0))  bl_fixed_assets,
    sum(nvl(bl.constru_in_process,0))  bl_constru_in_process,
    sum(nvl(bl.construction_materials,0))  bl_construction_materials,
    sum(nvl(bl.fixed_assets_liquidation,0))  bl_fixed_assets_liquidation,
    sum(nvl(bl.biological_assets,0))  bl_biological_assets,
    sum(nvl(bl.development_expenditure,0))  bl_development_expenditure,
    sum(nvl(bl.good_will,0))  bl_good_will,
    sum(nvl(bl.long_deferred_expense,0))  bl_long_deferred_expense,
    sum(nvl(bl.deferred_tax_assets,0))  bl_deferred_tax_assets,
    sum(nvl(bl.other_non_current_assets,0))  bl_other_non_current_assets,
    sum(nvl(bl.total_non_current_assets,0))  bl_total_non_current_assets,
    sum(nvl(bl.total_assets,0))  bl_total_assets,
    sum(nvl(bl.shortterm_loan,0))  bl_shortterm_loan,
    sum(nvl(bl.borrowing_from_centralbank,0))  bl_borrowing_from_centralbank,
    sum(nvl(bl.deposit_in_interbank,0))  bl_deposit_in_interbank,
    sum(nvl(bl.borrowing_capital,0))  bl_borrowing_capital,
    sum(nvl(bl.trading_liability,0))  bl_trading_liability,
    sum(nvl(bl.notes_payable,0))  bl_notes_payable,
    sum(nvl(bl.accounts_payable,0))  bl_accounts_payable,
    sum(nvl(bl.advance_peceipts,0))  bl_advance_peceipts,
    sum(nvl(bl.sold_buyback_secu_proceeds,0))  bl_sold_buyback_secu_proceeds,
    sum(nvl(bl.commission_payable,0))  bl_commission_payable,
    sum(nvl(bl.salaries_payable,0))  bl_salaries_payable,
    sum(nvl(bl.taxs_payable,0))  bl_taxs_payable,
    sum(nvl(bl.interest_payable,0))  bl_interest_payable,
    sum(nvl(bl.dividend_payable,0))  bl_dividend_payable,
    sum(nvl(bl.other_payable,0))  bl_other_payable,
    sum(nvl(bl.reinsurance_payables,0))  bl_reinsurance_payables,
    sum(nvl(bl.insurance_contract_reserves,0))  bl_insurance_contract_reserves,
    sum(nvl(bl.proxy_secu_proceeds,0))  bl_proxy_secu_proceeds,
    sum(nvl(bl.receivings_from_vicariously_sold_securities,0))  bl_receivings_from_vicariously_sold_securities,
    sum(nvl(bl.non_current_liability_in_one_year,0))  bl_non_current_liability_in_one_year,
    sum(nvl(bl.other_current_liability,0))  bl_other_current_liability,
    sum(nvl(bl.total_current_liability,0))  bl_total_current_liability,
    sum(nvl(bl.longterm_loan,0))  bl_longterm_loan,
    sum(nvl(bl.bonds_payable,0))  bl_bonds_payable,
    sum(nvl(bl.longterm_account_payable,0))  bl_longterm_account_payable,
    sum(nvl(bl.specific_account_payable,0))  bl_specific_account_payable,
    sum(nvl(bl.estimate_liability,0))  bl_estimate_liability,
    sum(nvl(bl.deferred_tax_liability,0))  bl_deferred_tax_liability,
    sum(nvl(bl.other_non_current_liability,0))  bl_other_non_current_liability,
    sum(nvl(bl.total_non_current_liability,0))  bl_total_non_current_liability,
    sum(nvl(bl.total_liability,0))  bl_total_liability,
    sum(nvl(bl.paidin_capital,0))  bl_paidin_capital,
    sum(nvl(bl.capital_reserve_fund,0))  bl_capital_reserve_fund,
    sum(nvl(bl.treasury_stock,0))  bl_treasury_stock,
    sum(nvl(bl.specific_reserves,0))  bl_specific_reserves,
    sum(nvl(bl.surplus_reserve_fund,0))  bl_surplus_reserve_fund,
    sum(nvl(bl.ordinary_risk_reserve_fund,0))  bl_ordinary_risk_reserve_fund,
    sum(nvl(bl.retained_profit,0))  bl_retained_profit,
    sum(nvl(bl.foreign_currency_report_conv_diff,0))  bl_foreign_currency_report_conv_diff,
    sum(nvl(bl.equities_parent_company_owners,0))  bl_equities_parent_company_owners,
    sum(nvl(bl.minority_interests,0))  bl_minority_interests,
    sum(nvl(bl.total_owner_equities,0))  bl_total_owner_equities,
    sum(nvl(bl.total_sheet_owner_equities,0))  bl_total_sheet_owner_equities,
    sum(nvl(cf.goods_sale_and_service_render_cash,0)) cf_goods_sale_and_service_render_cash,
    sum(nvl(cf.net_deposit_increase,0)) cf_net_deposit_increase,
    sum(nvl(cf.net_borrowing_from_central_bank,0)) cf_net_borrowing_from_central_bank,
    sum(nvl(cf.net_borrowing_from_finance_co,0)) cf_net_borrowing_from_finance_co,
    sum(nvl(cf.net_original_insurance_cash,0)) cf_net_original_insurance_cash,
    sum(nvl(cf.net_cash_received_from_reinsurance_business,0)) cf_net_cash_received_from_reinsurance_business,
    sum(nvl(cf.net_insurer_deposit_investment,0)) cf_net_insurer_deposit_investment,
    sum(nvl(cf.net_deal_trading_assets,0)) cf_net_deal_trading_assets,
    sum(nvl(cf.interest_and_commission_cashin,0)) cf_interest_and_commission_cashin,
    sum(nvl(cf.net_increase_in_placements,0)) cf_net_increase_in_placements,
    sum(nvl(cf.net_buyback,0)) cf_net_buyback,
    sum(nvl(cf.tax_levy_refund,0)) cf_tax_levy_refund,
    sum(nvl(cf.other_cashin_related_operate,0)) cf_other_cashin_related_operate,
    sum(nvl(cf.subtotal_operate_cash_inflow,0)) cf_subtotal_operate_cash_inflow,
    sum(nvl(cf.goods_and_services_cash_paid,0)) cf_goods_and_services_cash_paid,
    sum(nvl(cf.net_loan_and_advance_increase,0)) cf_net_loan_and_advance_increase,
    sum(nvl(cf.net_deposit_in_cb_and_ib,0)) cf_net_deposit_in_cb_and_ib,
    sum(nvl(cf.original_compensation_paid,0)) cf_original_compensation_paid,
    sum(nvl(cf.handling_charges_and_commission,0)) cf_handling_charges_and_commission,
    sum(nvl(cf.policy_dividend_cash_paid,0)) cf_policy_dividend_cash_paid,
    sum(nvl(cf.staff_behalf_paid,0)) cf_staff_behalf_paid,
    sum(nvl(cf.tax_payments,0)) cf_tax_payments,
    sum(nvl(cf.other_operate_cash_paid,0)) cf_other_operate_cash_paid,
    sum(nvl(cf.subtotal_operate_cash_outflow,0)) cf_subtotal_operate_cash_outflow,
    sum(nvl(cf.net_operate_cash_flow,0)) cf_net_operate_cash_flow,
    sum(nvl(cf.invest_withdrawal_cash,0)) cf_invest_withdrawal_cash,
    sum(nvl(cf.invest_proceeds,0)) cf_invest_proceeds,
    sum(nvl(cf.fix_intan_other_asset_dispo_cash,0)) cf_fix_intan_other_asset_dispo_cash,
    sum(nvl(cf.net_cash_deal_subcompany,0)) cf_net_cash_deal_subcompany,
    sum(nvl(cf.other_cash_from_invest_act,0)) cf_other_cash_from_invest_act,
    sum(nvl(cf.subtotal_invest_cash_inflow,0)) cf_subtotal_invest_cash_inflow,
    sum(nvl(cf.fix_intan_other_asset_acqui_cash,0)) cf_fix_intan_other_asset_acqui_cash,
    sum(nvl(cf.invest_cash_paid,0)) cf_invest_cash_paid,
    sum(nvl(cf.impawned_loan_net_increase,0)) cf_impawned_loan_net_increase,
    sum(nvl(cf.net_cash_from_sub_company,0)) cf_net_cash_from_sub_company,
    sum(nvl(cf.other_cash_to_invest_act,0)) cf_other_cash_to_invest_act,
    sum(nvl(cf.subtotal_invest_cash_outflow,0)) cf_subtotal_invest_cash_outflow,
    sum(nvl(cf.net_invest_cash_flow,0)) cf_net_invest_cash_flow,
    sum(nvl(cf.cash_from_invest,0)) cf_cash_from_invest,
    sum(nvl(cf.cash_from_mino_s_invest_sub,0)) cf_cash_from_mino_s_invest_sub,
    sum(nvl(cf.cash_from_borrowing,0)) cf_cash_from_borrowing,
    sum(nvl(cf.cash_from_bonds_issue,0)) cf_cash_from_bonds_issue,
    sum(nvl(cf.other_finance_act_cash,0)) cf_other_finance_act_cash,
    sum(nvl(cf.subtotal_finance_cash_inflow,0)) cf_subtotal_finance_cash_inflow,
    sum(nvl(cf.borrowing_repayment,0)) cf_borrowing_repayment,
    sum(nvl(cf.dividend_interest_payment,0)) cf_dividend_interest_payment,
    sum(nvl(cf.proceeds_from_sub_to_mino_s,0)) cf_proceeds_from_sub_to_mino_s,
    sum(nvl(cf.other_finance_act_payment,0)) cf_other_finance_act_payment,
    sum(nvl(cf.subtotal_finance_cash_outflow,0)) cf_subtotal_finance_cash_outflow,
    sum(nvl(cf.net_finance_cash_flow,0)) cf_net_finance_cash_flow,
    sum(nvl(cf.exchange_rate_change_effect,0)) cf_exchange_rate_change_effect,
    sum(nvl(cf.cash_equivalent_increase,0)) cf_cash_equivalent_increase,
    sum(nvl(cf.cash_equivalents_at_beginning,0)) cf_cash_equivalents_at_beginning,
    sum(nvl(cf.cash_and_equivalents_at_end,0)) cf_cash_and_equivalents_at_end,
    sum(nvl(ic.total_operating_revenue,0)) ic_total_operating_revenue,
    sum(nvl(ic.operating_revenue,0)) ic_operating_revenue,
    sum(nvl(ic.interest_income,0)) ic_interest_income,
    sum(nvl(ic.premiums_earned,0)) ic_premiums_earned,
    sum(nvl(ic.commission_income,0)) ic_commission_income,
    sum(nvl(ic.total_operating_cost,0)) ic_total_operating_cost,
    sum(nvl(ic.operating_cost,0)) ic_operating_cost,
    sum(nvl(ic.interest_expense,0)) ic_interest_expense,
    sum(nvl(ic.commission_expense,0)) ic_commission_expense,
    sum(nvl(ic.refunded_premiums,0)) ic_refunded_premiums,
    sum(nvl(ic.net_pay_insurance_claims,0)) ic_net_pay_insurance_claims,
    sum(nvl(ic.withdraw_insurance_contract_reserve,0)) ic_withdraw_insurance_contract_reserve,
    sum(nvl(ic.policy_dividend_payout,0)) ic_policy_dividend_payout,
    sum(nvl(ic.reinsurance_cost,0)) ic_reinsurance_cost,
    sum(nvl(ic.operating_tax_surcharges,0)) ic_operating_tax_surcharges,
    sum(nvl(ic.sale_expense,0)) ic_sale_expense,
    sum(nvl(ic.administration_expense,0)) ic_administration_expense,
    sum(nvl(ic.financial_expense,0)) ic_financial_expense,
    sum(nvl(ic.asset_impairment_loss,0)) ic_asset_impairment_loss,
    sum(nvl(ic.fair_value_variable_income,0)) ic_fair_value_variable_income,
    sum(nvl(ic.investment_income,0)) ic_investment_income,
    sum(nvl(ic.invest_income_associates,0)) ic_invest_income_associates,
    sum(nvl(ic.exchange_income,0)) ic_exchange_income,
    sum(nvl(ic.operating_profit,0)) ic_operating_profit,
    sum(nvl(ic.non_operating_revenue,0)) ic_non_operating_revenue,
    sum(nvl(ic.non_operating_expense,0)) ic_non_operating_expense,
    sum(nvl(ic.disposal_loss_non_current_liability,0)) ic_disposal_loss_non_current_liability,
    sum(nvl(ic.total_profit,0)) ic_total_profit,
    sum(nvl(ic.income_tax_expense,0)) ic_income_tax_expense,
    sum(nvl(ic.net_profit,0)) ic_net_profit,
    sum(nvl(ic.np_parent_company_owners,0)) ic_np_parent_company_owners,
    sum(nvl(ic.minority_profit,0)) ic_minority_profit,
    sum(nvl(ic.basic_eps,0)) ic_basic_eps,
    sum(nvl(ic.diluted_eps,0)) ic_diluted_eps,
    sum(nvl(ic.other_composite_income,0)) ic_other_composite_income,
    sum(nvl(ic.total_composite_income,0)) ic_total_composite_income,
    sum(nvl(ic.ci_parent_company_owners,0)) ic_ci_parent_company_owners,
    sum(nvl(ic.ci_minority_owners,0)) ic_ci_minority_owners
    from stockdata.ods_stk_balance bl
    left join stockdata.dim_company_info dci on dci.code=bl.code
    left join stockdata.ods_stk_income ic on ic.code=bl.code and ic.pubdate=bl.pubdate and ic.statdate=bl.statdate
    left join stockdata.ods_stk_cash_flow cf on cf.code=bl.code and cf.pubdate=bl.pubdate and cf.statdate=bl.statdate
    where bl.dt='$do_date'
    group by bl.id,bl.code,dci.id,bl.pubDate,bl.statDate
;"

case $1 in
    "dwd_finance_data" )
        hive -e "$dwd_finance_data"
    ;;
    "all" )
        hive -e "$dwd_finance_data$dws_coupon_info_daycount"
    ;;
esac

-------------------------insert语句end----------------------------------

--交易数据表 dwd_trade_data
-------------------------建表语句start------------------------------
DROP TABLE IF EXISTS dwd_trade_data;
CREATE EXTERNAL TABLE dwd_trade_data (
    `id` STRING COMMENT 'id',
    `code` STRING COMMENT '分摊活动优惠',
    `code_day` STRING COMMENT '分摊活动优惠',
    `open` DECIMAL(20,4) COMMENT '时间段开始时价格',
    `close` DECIMAL(20,4) COMMENT '时间段结束时价格',
    `low` DECIMAL(20,4) COMMENT '时间段中的最低价',
    `high` DECIMAL(20,4) COMMENT '时间段中的最高价',
    `volume` DECIMAL(20,4) COMMENT '时间段中的成交的股票数量',
    `money` DECIMAL(20,4) COMMENT '时间段中的成交的金额	',
    `factor` DECIMAL(20,4) COMMENT 'pre:前复权(默认)None:不复权,返回实际价格post:后复权',
    `high_limit` DECIMAL(20,4) COMMENT '时间段中的涨停价',
    `low_limit` DECIMAL(20,4) COMMENT '时间段中的跌停价',
    `avg` DECIMAL(20,4) COMMENT '时间段中的平均价',
    `pre_close` DECIMAL(20,4) COMMENT '前一个单位时间结束时的价格,按天则是前一天的收盘价',
    `paused` DECIMAL(20,4) COMMENT 'bool值,股票是否停牌',
    `capitalization` DECIMAL(20,4) COMMENT '总股本(万股)',
    `circulating_cap` DECIMAL(20,4) COMMENT '流通股本(万股)',
    `market_cap` DECIMAL(20,4) COMMENT '总市值(亿元)',
    `circulating_market_cap` DECIMAL(20,4) COMMENT '流通市值(亿元)',
    `turnover_ratio` DECIMAL(20,4) COMMENT '换手率(%)',
    `pe_ratio` DECIMAL(20,4) COMMENT '市盈率(PE, TTM)',
    `pe_ratio_lyr` DECIMAL(20,4) COMMENT '市盈率(PE)',
    `pb_ratio` DECIMAL(20,4) COMMENT '市净率(PB)',
    `ps_ratio` DECIMAL(20,4) COMMENT '市销率(PS, TTM)'
 ) COMMENT '交易数据表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/stockdata/dwd/dwd_trade_data/'
TBLPROPERTIES ("parquet.compression"="lzo");
-------------------------建表语句end--------------------------------


-------------------------insert语句start------------------------------
select
    dp.id id,
    dp.code code,
    dp.code_day code_day,
    sum(nvl(dp.open,0)) open,
    sum(nvl(dp.close,0)) close,
    sum(nvl(dp.low,0)) low,
    sum(nvl(dp.high,0)) high,
    sum(nvl(dp.volume,0)) volume,
    sum(nvl(dp.money,0)) money,
    sum(nvl(dp.factor,0)) factor,
    sum(nvl(dp.high_limit,0)) high_limit,
    sum(nvl(dp.low_limit,0)) low_limit,
    sum(nvl(dp.avg,0)) avg,
    sum(nvl(dp.pre_close,0)) pre_close,
    sum(nvl(dp.paused,0)) paused,
    sum(nvl(dv.capitalization,0)) capitalization,
    sum(nvl(dv.circulating_cap,0)) circulating_cap,
    sum(nvl(dv.market_cap,0)) market_cap,
    sum(nvl(dv.circulating_market_cap,0)) circulating_market_cap,
    sum(nvl(dv.turnover_ratio,0)) turnover_ratio,
    sum(nvl(dv.pe_ratio,0)) pe_ratio,
    sum(nvl(dv.pe_ratio_lyr,0)) pe_ratio_lyr,
    sum(nvl(dv.pb_ratio,0)) pb_ratio,
    sum(nvl(dv.ps_ratio,0)) ps_ratio
    from stockdata.ods_stk_day_price dp
    left join stockdata.dim_company_info dci on dp.code = dci.code
    left join stockdata.ods_stk_day_valuation dv on dp.code = dv.code and dp.code_day=dv.code_day
    where dp.dt='$do_date'
    and dci.dt='$do_date'
    and dv.dt='$do_date'
    group by dp.id,dp.code,dp.code_day

-------------------------insert语句end--------------------------------