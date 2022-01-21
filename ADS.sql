-----------------------各省市上市公司数-------------------------
DROP TABLE IF EXISTS ads_province_company_data;
CREATE EXTERNAL TABLE ads_province_company_data (
    `province_id` STRING COMMENT 'province_id',
    `province_name` STRING COMMENT '省市名称',
    `area_code` STRING COMMENT '地区编码',
    `iso_code` STRING COMMENT 'ISO-3166编码，供可视化使用',
    `iso_3166_2` STRING COMMENT 'IOS-3166-2编码，供可视化使用',
    `company_count` STRING COMMENT '上市公司数'
)COMMENT '各地区上市公司数据'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/stockdata/ads/ads_province_company_data/';




select  dp.id province_id,
        dp.province_name province_name,
        dp.area_code area_code,
        dp.iso_code iso_code,
        dp.iso_3166_2 iso_3166_2,
        count(c.id) company_count
        from stockdata.dim_company_info c
        left join stockdata.dim_province dp on c.province = dp.province_name
        where c.dt='2021-08-08'
        and c.province='广东'
        group by c.dt,dp.id,dp.province_name,dp.area_code,dp.iso_code,dp.iso_3166_2


--------------------------------------------------

DROP TABLE IF EXISTS ads_trade_data;
CREATE EXTERNAL TABLE ads_trade_data (
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
)COMMENT '财务数据表'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/stockdata/ads/ads_trade_data/';

--交易数据表 ads_trade_data
-------------------------建表语句start------------------------------
--当天交易数据 当天市值 当月涨幅
with min_day as (
    select t.code,
           t.code_mon,
           min(t.code_day) minday
    from (
        select td.code                         code,
                 substr(date(td.code_day), 1, 7) code_mon,
                 date_format(td.code_day,'yyyy-MM-dd') code_day
          from dwd_trade_data td
          where code = '000002'
            and dt = '2021-08-08'
        ) t
    where 1 = 1
    group by t.code, t.code_mon
),
max_day as(
     select t.code,
           t.code_mon,
           max(t.code_day) maxday
    from (select td.code                         code,
                 substr(date(td.code_day), 1, 7) code_mon,
                 code_day                        code_day
          from dwd_trade_data td
          where code = '000002'
            and dt = '2021-08-08') t
    where 1 = 1
    group by t.code, t.code_mon

)


select td.code,
    date_sub(date(td.code_day),dayofmonth(date(td.code_day))-1),
    last_day(date(td.code_day)),
    code_day,
    td.open,
    td.close
    from dwd_trade_data td
    where code='000002'
    and dt='2021-08-08'


select
		code,
		code_day,
		sum(nvl(open,0)),
		sum(nvl(close,0)),
		sum(nvl(low,0)),
		sum(nvl(high,0)),
		sum(nvl(volume,0)),
		sum(nvl(money,0)),
		sum(nvl(factor,0)),
		sum(nvl(high_limit,0)),
		sum(nvl(low_limit,0)),
		sum(nvl(avg,0)),
		sum(nvl(pre_close,0)),
		sum(nvl(paused,0)),
		sum(nvl(capitalization,0)),
		sum(nvl(circulating_cap,0)),
		sum(nvl(market_cap,0)),
		sum(nvl(circulating_market_cap,0)),
		sum(nvl(turnover_ratio,0)),
		sum(nvl(pe_ratio,0)),
		sum(nvl(pe_ratio_lyr,0)),
		sum(nvl(ps_ratio,0))
		from  stockdata.dwd_trade_data
		where dt='2021-08-08'
		group by code,code_day


-------------------------建表语句end--------------------------------



--财务数据表 ads_finance_data
-------------------------建表语句start------------------------------
DROP TABLE IF EXISTS ads_finance_data;
CREATE EXTERNAL TABLE ads_finance_data (
    --`id` STRING COMMENT 'id', --balance表的id
    `code` STRING COMMENT '股票代码',
    `company_id` STRING COMMENT 'dim_company_info的id',
    `sw_l1` STRING COMMENT '行业swl3',
    `sw_l2` STRING COMMENT '行业swl3',
    `sw_l3` STRING COMMENT '行业swl3',
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
    `ic_ci_minority_owners` DECIMAL(20,4) COMMENT '归属于少数股东的综合收益总额(元)',
    `balance_ratio` DECIMAL(20,4) COMMENT '负债率',
    `bussiness_assets_acc_all` DECIMAL(20,4) COMMENT '经营性资产占比',
    `goodwills_acc_net` DECIMAL(20,4) COMMENT '商誉占比',
    `account_receivable_acc_net` DECIMAL(20,4) COMMENT '应收账款占比',
    `accounts_payable_acc_net` DECIMAL(20,4) COMMENT '应付账款占比',
    `intangible_assets_acc_net` DECIMAL(20,4) COMMENT '无形资产占比',
    `gross_profit_rate` DECIMAL(20,4) COMMENT '毛利率',
    `profit_cash_cover` DECIMAL(20,4) COMMENT '利润现金保障倍数'
)COMMENT '财务数据表'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/stockdata/ads/ads_finance_data/';



select  dfd.id id,
        dfd.code code,
        null company_id,
        null sw_l1,
        null sw_l2,
        null sw_l3,
        dfd.pubDate pubDate,
        dfd.statDate statDate,
        sum(nvl(dfd.bl_cash_equivalents,0))  bl_cash_equivalents,
        sum(nvl(dfd.bl_settlement_provi,0))  bl_settlement_provi,
        sum(nvl(dfd.bl_lend_capital,0))  bl_lend_capital,
        sum(nvl(dfd.bl_trading_assets,0))  bl_trading_assets,
        sum(nvl(dfd.bl_bill_receivable,0))  bl_bill_receivable,
        sum(nvl(dfd.bl_account_receivable,0))  bl_account_receivable,
        sum(nvl(dfd.bl_advance_payment,0))  bl_advance_payment,
        sum(nvl(dfd.bl_insurance_receivables,0))  bl_insurance_receivables,
        sum(nvl(dfd.bl_reinsurance_receivables,0))  bl_reinsurance_receivables,
        sum(nvl(dfd.bl_reinsurance_contract_reserves_receivable,0))  bl_reinsurance_contract_reserves_receivable,
        sum(nvl(dfd.bl_interest_receivable,0))  bl_interest_receivable,
        sum(nvl(dfd.bl_dividend_receivable,0))  bl_dividend_receivable,
        sum(nvl(dfd.bl_other_receivable,0))  bl_other_receivable,
        sum(nvl(dfd.bl_bought_sellback_assets,0))  bl_bought_sellback_assets,
        sum(nvl(dfd.bl_inventories,0))  bl_inventories,
        sum(nvl(dfd.bl_non_current_asset_in_one_year,0))  bl_non_current_asset_in_one_year,
        sum(nvl(dfd.bl_other_current_assets,0))  bl_other_current_assets,
        sum(nvl(dfd.bl_total_current_assets,0))  bl_total_current_assets,
        sum(nvl(dfd.bl_loan_and_advance,0))  bl_loan_and_advance,
        sum(nvl(dfd.bl_hold_for_sale_assets,0))  bl_hold_for_sale_assets,
        sum(nvl(dfd.bl_hold_to_maturity_investments,0))  bl_hold_to_maturity_investments,
        sum(nvl(dfd.bl_longterm_receivable_account,0))  bl_longterm_receivable_account,
        sum(nvl(dfd.bl_longterm_equity_invest,0))  bl_longterm_equity_invest,
        sum(nvl(dfd.bl_investment_property,0))  bl_investment_property,
        sum(nvl(dfd.bl_fixed_assets,0))  bl_fixed_assets,
        sum(nvl(dfd.bl_constru_in_process,0))  bl_constru_in_process,
        sum(nvl(dfd.bl_construction_materials,0))  bl_construction_materials,
        sum(nvl(dfd.bl_fixed_assets_liquidation,0))  bl_fixed_assets_liquidation,
        sum(nvl(dfd.bl_biological_assets,0))  bl_biological_assets,
        sum(nvl(dfd.bl_development_expenditure,0))  bl_development_expenditure,
        sum(nvl(dfd.bl_good_will,0))  bl_good_will,
        sum(nvl(dfd.bl_long_deferred_expense,0))  bl_long_deferred_expense,
        sum(nvl(dfd.bl_deferred_tax_assets,0))  bl_deferred_tax_assets,
        sum(nvl(dfd.bl_other_non_current_assets,0))  bl_other_non_current_assets,
        sum(nvl(dfd.bl_total_non_current_assets,0))  bl_total_non_current_assets,
        sum(nvl(dfd.bl_total_assets,0))  bl_total_assets,
        sum(nvl(dfd.bl_shortterm_loan,0))  bl_shortterm_loan,
        sum(nvl(dfd.bl_borrowing_from_centralbank,0))  bl_borrowing_from_centralbank,
        sum(nvl(dfd.bl_deposit_in_interbank,0))  bl_deposit_in_interbank,
        sum(nvl(dfd.bl_borrowing_capital,0))  bl_borrowing_capital,
        sum(nvl(dfd.bl_trading_liability,0))  bl_trading_liability,
        sum(nvl(dfd.bl_notes_payable,0))  bl_notes_payable,
        sum(nvl(dfd.bl_accounts_payable,0))  bl_accounts_payable,
        sum(nvl(dfd.bl_advance_peceipts,0))  bl_advance_peceipts,
        sum(nvl(dfd.bl_sold_buyback_secu_proceeds,0))  bl_sold_buyback_secu_proceeds,
        sum(nvl(dfd.bl_commission_payable,0))  bl_commission_payable,
        sum(nvl(dfd.bl_salaries_payable,0))  bl_salaries_payable,
        sum(nvl(dfd.bl_taxs_payable,0))  bl_taxs_payable,
        sum(nvl(dfd.bl_interest_payable,0))  bl_interest_payable,
        sum(nvl(dfd.bl_dividend_payable,0))  bl_dividend_payable,
        sum(nvl(dfd.bl_other_payable,0))  bl_other_payable,
        sum(nvl(dfd.bl_reinsurance_payables,0))  bl_reinsurance_payables,
        sum(nvl(dfd.bl_insurance_contract_reserves,0))  bl_insurance_contract_reserves,
        sum(nvl(dfd.bl_proxy_secu_proceeds,0))  bl_proxy_secu_proceeds,
        sum(nvl(dfd.bl_receivings_from_vicariously_sold_securities,0))  bl_receivings_from_vicariously_sold_securities,
        sum(nvl(dfd.bl_non_current_liability_in_one_year,0))  bl_non_current_liability_in_one_year,
        sum(nvl(dfd.bl_other_current_liability,0))  bl_other_current_liability,
        sum(nvl(dfd.bl_total_current_liability,0))  bl_total_current_liability,
        sum(nvl(dfd.bl_longterm_loan,0))  bl_longterm_loan,
        sum(nvl(dfd.bl_bonds_payable,0))  bl_bonds_payable,
        sum(nvl(dfd.bl_longterm_account_payable,0))  bl_longterm_account_payable,
        sum(nvl(dfd.bl_specific_account_payable,0))  bl_specific_account_payable,
        sum(nvl(dfd.bl_estimate_liability,0))  bl_estimate_liability,
        sum(nvl(dfd.bl_deferred_tax_liability,0))  bl_deferred_tax_liability,
        sum(nvl(dfd.bl_other_non_current_liability,0))  bl_other_non_current_liability,
        sum(nvl(dfd.bl_total_non_current_liability,0))  bl_total_non_current_liability,
        sum(nvl(dfd.bl_total_liability,0))  bl_total_liability,
        sum(nvl(dfd.bl_paidin_capital,0))  bl_paidin_capital,
        sum(nvl(dfd.bl_capital_reserve_fund,0))  bl_capital_reserve_fund,
        sum(nvl(dfd.bl_treasury_stock,0))  bl_treasury_stock,
        sum(nvl(dfd.bl_specific_reserves,0))  bl_specific_reserves,
        sum(nvl(dfd.bl_surplus_reserve_fund,0))  bl_surplus_reserve_fund,
        sum(nvl(dfd.bl_ordinary_risk_reserve_fund,0))  bl_ordinary_risk_reserve_fund,
        sum(nvl(dfd.bl_retained_profit,0))  bl_retained_profit,
        sum(nvl(dfd.bl_foreign_currency_report_conv_diff,0))  bl_foreign_currency_report_conv_diff,
        sum(nvl(dfd.bl_equities_parent_company_owners,0))  bl_equities_parent_company_owners,
        sum(nvl(dfd.bl_minority_interests,0))  bl_minority_interests,
        sum(nvl(dfd.bl_total_owner_equities,0))  bl_total_owner_equities,
        sum(nvl(dfd.bl_total_sheet_owner_equities,0))  bl_total_sheet_owner_equities,
        sum(nvl(dfd.cf_goods_sale_and_service_render_cash,0)) cf_goods_sale_and_service_render_cash,
        sum(nvl(dfd.cf_net_deposit_increase,0)) cf_net_deposit_increase,
        sum(nvl(dfd.cf_net_borrowing_from_central_bank,0)) cf_net_borrowing_from_central_bank,
        sum(nvl(dfd.cf_net_borrowing_from_finance_co,0)) cf_net_borrowing_from_finance_co,
        sum(nvl(dfd.cf_net_original_insurance_cash,0)) cf_net_original_insurance_cash,
        sum(nvl(dfd.cf_net_cash_received_from_reinsurance_business,0)) cf_net_cash_received_from_reinsurance_business,
        sum(nvl(dfd.cf_net_insurer_deposit_investment,0)) cf_net_insurer_deposit_investment,
        sum(nvl(dfd.cf_net_deal_trading_assets,0)) cf_net_deal_trading_assets,
        sum(nvl(dfd.cf_interest_and_commission_cashin,0)) cf_interest_and_commission_cashin,
        sum(nvl(dfd.cf_net_increase_in_placements,0)) cf_net_increase_in_placements,
        sum(nvl(dfd.cf_net_buyback,0)) cf_net_buyback,
        sum(nvl(dfd.cf_tax_levy_refund,0)) cf_tax_levy_refund,
        sum(nvl(dfd.cf_other_cashin_related_operate,0)) cf_other_cashin_related_operate,
        sum(nvl(dfd.cf_subtotal_operate_cash_inflow,0)) cf_subtotal_operate_cash_inflow,
        sum(nvl(dfd.cf_goods_and_services_cash_paid,0)) cf_goods_and_services_cash_paid,
        sum(nvl(dfd.cf_net_loan_and_advance_increase,0)) cf_net_loan_and_advance_increase,
        sum(nvl(dfd.cf_net_deposit_in_cb_and_ib,0)) cf_net_deposit_in_cb_and_ib,
        sum(nvl(dfd.cf_original_compensation_paid,0)) cf_original_compensation_paid,
        sum(nvl(dfd.cf_handling_charges_and_commission,0)) cf_handling_charges_and_commission,
        sum(nvl(dfd.cf_policy_dividend_cash_paid,0)) cf_policy_dividend_cash_paid,
        sum(nvl(dfd.cf_staff_behalf_paid,0)) cf_staff_behalf_paid,
        sum(nvl(dfd.cf_tax_payments,0)) cf_tax_payments,
        sum(nvl(dfd.cf_other_operate_cash_paid,0)) cf_other_operate_cash_paid,
        sum(nvl(dfd.cf_subtotal_operate_cash_outflow,0)) cf_subtotal_operate_cash_outflow,
        sum(nvl(dfd.cf_net_operate_cash_flow,0)) cf_net_operate_cash_flow,
        sum(nvl(dfd.cf_invest_withdrawal_cash,0)) cf_invest_withdrawal_cash,
        sum(nvl(dfd.cf_invest_proceeds,0)) cf_invest_proceeds,
        sum(nvl(dfd.cf_fix_intan_other_asset_dispo_cash,0)) cf_fix_intan_other_asset_dispo_cash,
        sum(nvl(dfd.cf_net_cash_deal_subcompany,0)) cf_net_cash_deal_subcompany,
        sum(nvl(dfd.cf_other_cash_from_invest_act,0)) cf_other_cash_from_invest_act,
        sum(nvl(dfd.cf_subtotal_invest_cash_inflow,0)) cf_subtotal_invest_cash_inflow,
        sum(nvl(dfd.cf_fix_intan_other_asset_acqui_cash,0)) cf_fix_intan_other_asset_acqui_cash,
        sum(nvl(dfd.cf_invest_cash_paid,0)) cf_invest_cash_paid,
        sum(nvl(dfd.cf_impawned_loan_net_increase,0)) cf_impawned_loan_net_increase,
        sum(nvl(dfd.cf_net_cash_from_sub_company,0)) cf_net_cash_from_sub_company,
        sum(nvl(dfd.cf_other_cash_to_invest_act,0)) cf_other_cash_to_invest_act,
        sum(nvl(dfd.cf_subtotal_invest_cash_outflow,0)) cf_subtotal_invest_cash_outflow,
        sum(nvl(dfd.cf_net_invest_cash_flow,0)) cf_net_invest_cash_flow,
        sum(nvl(dfd.cf_cash_from_invest,0)) cf_cash_from_invest,
        sum(nvl(dfd.cf_cash_from_mino_s_invest_sub,0)) cf_cash_from_mino_s_invest_sub,
        sum(nvl(dfd.cf_cash_from_borrowing,0)) cf_cash_from_borrowing,
        sum(nvl(dfd.cf_cash_from_bonds_issue,0)) cf_cash_from_bonds_issue,
        sum(nvl(dfd.cf_other_finance_act_cash,0)) cf_other_finance_act_cash,
        sum(nvl(dfd.cf_subtotal_finance_cash_inflow,0)) cf_subtotal_finance_cash_inflow,
        sum(nvl(dfd.cf_borrowing_repayment,0)) cf_borrowing_repayment,
        sum(nvl(dfd.cf_dividend_interest_payment,0)) cf_dividend_interest_payment,
        sum(nvl(dfd.cf_proceeds_from_sub_to_mino_s,0)) cf_proceeds_from_sub_to_mino_s,
        sum(nvl(dfd.cf_other_finance_act_payment,0)) cf_other_finance_act_payment,
        sum(nvl(dfd.cf_subtotal_finance_cash_outflow,0)) cf_subtotal_finance_cash_outflow,
        sum(nvl(dfd.cf_net_finance_cash_flow,0)) cf_net_finance_cash_flow,
        sum(nvl(dfd.cf_exchange_rate_change_effect,0)) cf_exchange_rate_change_effect,
        sum(nvl(dfd.cf_cash_equivalent_increase,0)) cf_cash_equivalent_increase,
        sum(nvl(dfd.cf_cash_equivalents_at_beginning,0)) cf_cash_equivalents_at_beginning,
        sum(nvl(dfd.cf_cash_and_equivalents_at_end,0)) cf_cash_and_equivalents_at_end,
        sum(nvl(dfd.ic_total_operating_revenue,0)) ic_total_operating_revenue,
        sum(nvl(dfd.ic_operating_revenue,0)) ic_operating_revenue,
        sum(nvl(dfd.ic_interest_income,0)) ic_interest_income,
        sum(nvl(dfd.ic_premiums_earned,0)) ic_premiums_earned,
        sum(nvl(dfd.ic_commission_income,0)) ic_commission_income,
        sum(nvl(dfd.ic_total_operating_cost,0)) ic_total_operating_cost,
        sum(nvl(dfd.ic_operating_cost,0)) ic_operating_cost,
        sum(nvl(dfd.ic_interest_expense,0)) ic_interest_expense,
        sum(nvl(dfd.ic_commission_expense,0)) ic_commission_expense,
        sum(nvl(dfd.ic_refunded_premiums,0)) ic_refunded_premiums,
        sum(nvl(dfd.ic_net_pay_insurance_claims,0)) ic_net_pay_insurance_claims,
        sum(nvl(dfd.ic_withdraw_insurance_contract_reserve,0)) ic_withdraw_insurance_contract_reserve,
        sum(nvl(dfd.ic_policy_dividend_payout,0)) ic_policy_dividend_payout,
        sum(nvl(dfd.ic_reinsurance_cost,0)) ic_reinsurance_cost,
        sum(nvl(dfd.ic_operating_tax_surcharges,0)) ic_operating_tax_surcharges,
        sum(nvl(dfd.ic_sale_expense,0)) ic_sale_expense,
        sum(nvl(dfd.ic_administration_expense,0)) ic_administration_expense,
        sum(nvl(dfd.ic_financial_expense,0)) ic_financial_expense,
        sum(nvl(dfd.ic_asset_impairment_loss,0)) ic_asset_impairment_loss,
        sum(nvl(dfd.ic_fair_value_variable_income,0)) ic_fair_value_variable_income,
        sum(nvl(dfd.ic_investment_income,0)) ic_investment_income,
        sum(nvl(dfd.ic_invest_income_associates,0)) ic_invest_income_associates,
        sum(nvl(dfd.ic_exchange_income,0)) ic_exchange_income,
        sum(nvl(dfd.ic_operating_profit,0)) ic_operating_profit,
        sum(nvl(dfd.ic_non_operating_revenue,0)) ic_non_operating_revenue,
        sum(nvl(dfd.ic_non_operating_expense,0)) ic_non_operating_expense,
        sum(nvl(dfd.ic_disposal_loss_non_current_liability,0)) ic_disposal_loss_non_current_liability,
        sum(nvl(dfd.ic_total_profit,0)) ic_total_profit,
        sum(nvl(dfd.ic_income_tax_expense,0)) ic_income_tax_expense,
        sum(nvl(dfd.ic_net_profit,0)) ic_net_profit,
        sum(nvl(dfd.ic_np_parent_company_owners,0)) ic_np_parent_company_owners,
        sum(nvl(dfd.ic_minority_profit,0)) ic_minority_profit,
        sum(nvl(dfd.ic_basic_eps,0)) ic_basic_eps,
        sum(nvl(dfd.ic_diluted_eps,0)) ic_diluted_eps,
        sum(nvl(dfd.ic_other_composite_income,0)) ic_other_composite_income,
        sum(nvl(dfd.ic_total_composite_income,0)) ic_total_composite_income,
        sum(nvl(dfd.ic_ci_parent_company_owners,0)) ic_ci_parent_company_owners,
        sum(nvl(dfd.ic_ci_minority_owners,0)) ic_ci_minority_owners,
       case when sum(dfd.bl_total_assets)=0 then null
            else round(sum(dfd.bl_total_liability)/sum(dfd.bl_total_assets),4) end balance_ratio, --负债率

       case when sum(dfd.bl_total_assets)=0 then null
             else  round((sum(dfd.bl_cash_equivalents)+sum(dfd.bl_bill_receivable)+sum(dfd.bl_account_receivable)+sum(dfd.bl_inventories)+
							sum(dfd.bl_fixed_assets)+sum(dfd.bl_constru_in_process))/
					sum(dfd.bl_total_assets),4) end  bussiness_assets_acc_all, --经营性资产占比 +sum(dfd.bl_intangible_assets)未加无形资产

       case when (sum(dfd.bl_total_assets)-sum(dfd.bl_total_liability))=0 then null
            else round(sum(dfd.bl_good_will)/(sum(dfd.bl_total_assets)-sum(dfd.bl_total_liability)),4) end goodwills_acc_net, --商誉占比

       case when (sum(dfd.bl_total_assets)-sum(dfd.bl_total_liability))=0 then null
             else round(sum(dfd.bl_account_receivable)/(sum(dfd.bl_total_assets)-sum(dfd.bl_total_liability)),4) end
                       account_receivable_acc_net, --应收账款占比

        case when (sum(dfd.bl_total_assets)-sum(dfd.bl_total_liability))=0 then null
             else round(sum(dfd.bl_accounts_payable)/(sum(dfd.bl_total_assets)-sum(dfd.bl_total_liability)),4) end
            accounts_payable_acc_net, --应付账款占比

        null intangible_assets_acc_net, --无形资产占比

       case when sum(nvl(dfd.ic_operating_revenue,0))=0 then null
	        else round((sum(nvl(dfd.ic_operating_revenue,0))-sum(nvl(dfd.ic_operating_cost,0)))/sum(nvl(dfd.ic_operating_revenue,0)),4) end  gross_profit_rate, --毛利率

        case when sum(nvl(dfd.ic_net_profit,0))=0 then null
	        else round(sum(nvl(dfd.cf_net_operate_cash_flow,0))/sum(nvl(dfd.ic_net_profit,0)),4) end  profit_cash_cover --利润现金保障倍数
        from dwd_finance_data dfd
        --left join dim_company_info dfi on dfd.company_id=dfi.id
        where 1=1
        group by dfd.id,dfd.code,dfd.pubDate,dfd.statDate


-------------------------建表语句end--------------------------------