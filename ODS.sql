--会计期间表
DROP TABLE IF EXISTS ods_bd_period;
CREATE EXTERNAL TABLE ods_bd_period(
    `id` STRING COMMENT '编号',
    `pd_year` BIGINT  COMMENT '期间年份',
    `pd_quarter` BIGINT  COMMENT '期间季度',
    `pd_number` BIGINT  COMMENT '期间编码值',
    `begin_date` STRING  COMMENT '开始日期',
    `end_date` STRING  COMMENT '结束日期',
    `number` STRING  COMMENT '编码',
    `description` STRING  COMMENT '描述信息'
) COMMENT '会计期间表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_bd_period/';

--行业表 industry
DROP TABLE IF EXISTS ods_industry;
CREATE EXTERNAL TABLE ods_industry(
    `id` STRING COMMENT '编号',
    `industry_id` STRING  COMMENT '行业id',
    `name` STRING  COMMENT '名称',
    `start_date` STRING  COMMENT '开始日期',
    `type` STRING  COMMENT '类型'
) COMMENT '行业表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_industry/';

--资产负债表 stk_balance
DROP TABLE IF EXISTS ods_stk_balance;
CREATE EXTERNAL TABLE ods_stk_balance(
    `id` STRING COMMENT 'id',
    `code` STRING COMMENT '股票代码',
    `pubDate` STRING COMMENT '发布日期',
    `statDate` STRING COMMENT '开始日期',
    `cash_equivalents` DECIMAL(20,4) COMMENT '货币资金',
    `settlement_provi` DECIMAL(20,4) COMMENT '结算备付金',
    `lend_capital` DECIMAL(20,4) COMMENT '拆出资金',
    `trading_assets` DECIMAL(20,4) COMMENT '交易性金融资产',
    `bill_receivable` DECIMAL(20,4) COMMENT '应收票据',
    `account_receivable` DECIMAL(20,4) COMMENT '应收账款',
    `advance_payment` DECIMAL(20,4) COMMENT '预付款项',
    `insurance_receivables` DECIMAL(20,4) COMMENT '应收保费',
    `reinsurance_receivables` DECIMAL(20,4) COMMENT '	应收分保账款',
    `reinsurance_contract_reserves_receivable` DECIMAL(20,4) COMMENT '应收分保合同准备金',
    `interest_receivable` DECIMAL(20,4) COMMENT '应收利息',
    `dividend_receivable` DECIMAL(20,4) COMMENT '应收股利',
    `other_receivable` DECIMAL(20,4) COMMENT '其他应收款',
    `bought_sellback_assets` DECIMAL(20,4) COMMENT '买入返售金融资产',
    `inventories` DECIMAL(20,4) COMMENT '存货',
    `non_current_asset_in_one_year` DECIMAL(20,4) COMMENT '一年内到期的非流动资产',
    `other_current_assets` DECIMAL(20,4) COMMENT '其他流动资产',
    `total_current_assets` DECIMAL(20,4) COMMENT '流动资产合计',
    `loan_and_advance` DECIMAL(20,4) COMMENT '发放委托贷款及垫款',
    `hold_for_sale_assets` DECIMAL(20,4) COMMENT '可供出售金融资产',
    `hold_to_maturity_investments` DECIMAL(20,4) COMMENT '持有至到期投资',
    `longterm_receivable_account` DECIMAL(20,4) COMMENT '长期应收款',
    `longterm_equity_invest` DECIMAL(20,4) COMMENT '长期股权投资',
    `investment_property` DECIMAL(20,4) COMMENT '投资性房地产',
    `fixed_assets` DECIMAL(20,4) COMMENT '固定资产',
    `constru_in_process` DECIMAL(20,4) COMMENT '在建工程',
    `construction_materials` DECIMAL(20,4) COMMENT '工程物资',
    `fixed_assets_liquidation` DECIMAL(20,4) COMMENT '固定资产清理',
    `biological_assets` DECIMAL(20,4) COMMENT '生产性生物资产',
    `development_expenditure` DECIMAL(20,4) COMMENT '开发支出',
    `good_will` DECIMAL(20,4) COMMENT '商誉',
    `long_deferred_expense` DECIMAL(20,4) COMMENT '长期待摊费用',
    `deferred_tax_assets` DECIMAL(20,4) COMMENT '递延所得税资产',
    `other_non_current_assets` DECIMAL(20,4) COMMENT '其他非流动资产',
    `total_non_current_assets` DECIMAL(20,4) COMMENT '非流动资产合计',
    `total_assets` DECIMAL(20,4) COMMENT '资产总计',
    `shortterm_loan` DECIMAL(20,4) COMMENT '短期借款',
    `borrowing_from_centralbank` DECIMAL(20,4) COMMENT '向中央银行借款',
    `deposit_in_interbank` DECIMAL(20,4) COMMENT '吸收存款及同业存放',
    `borrowing_capital` DECIMAL(20,4) COMMENT '拆入资金',
    `trading_liability` DECIMAL(20,4) COMMENT '交易性金融负债',
    `notes_payable` DECIMAL(20,4) COMMENT '应付票据',
    `accounts_payable` DECIMAL(20,4) COMMENT '应付账款',
    `advance_peceipts` DECIMAL(20,4) COMMENT '预收款项',
    `sold_buyback_secu_proceeds` DECIMAL(20,4) COMMENT '卖出回购金融资产款',
    `commission_payable` DECIMAL(20,4) COMMENT '应付手续费及佣金',
    `salaries_payable` DECIMAL(20,4) COMMENT '应付职工薪酬',
    `taxs_payable` DECIMAL(20,4) COMMENT '应交税费',
    `interest_payable` DECIMAL(20,4) COMMENT '应付利息',
    `dividend_payable` DECIMAL(20,4) COMMENT '应付股利',
    `other_payable` DECIMAL(20,4) COMMENT '其他应付款',
    `reinsurance_payables` DECIMAL(20,4) COMMENT '应付分保账款',
    `insurance_contract_reserves` DECIMAL(20,4) COMMENT '保险合同准备金',
    `proxy_secu_proceeds` DECIMAL(20,4) COMMENT '代理买卖证券款',
    `receivings_from_vicariously_sold_securities` DECIMAL(20,4) COMMENT '代理承销证券款',
    `non_current_liability_in_one_year` DECIMAL(20,4) COMMENT '一年内到期的非流动负债',
    `other_current_liability` DECIMAL(20,4) COMMENT '其他流动负债',
    `total_current_liability` DECIMAL(20,4) COMMENT '流动负债合计',
    `longterm_loan` DECIMAL(20,4) COMMENT '长期借款',
    `bonds_payable` DECIMAL(20,4) COMMENT '应付债券',
    `longterm_account_payable` DECIMAL(20,4) COMMENT '长期应付款',
    `specific_account_payable` DECIMAL(20,4) COMMENT '专项应付款',
    `estimate_liability` DECIMAL(20,4) COMMENT '预计负债',
    `deferred_tax_liability` DECIMAL(20,4) COMMENT '递延所得税负债',
    `other_non_current_liability` DECIMAL(20,4) COMMENT '其他非流动负债',
    `total_non_current_liability` DECIMAL(20,4) COMMENT '非流动负债合计',
    `total_liability` DECIMAL(20,4) COMMENT '负债合计',
    `paidin_capital` DECIMAL(20,4) COMMENT '实收资本(或股本)',
    `capital_reserve_fund` DECIMAL(20,4) COMMENT '资本公积金',
    `treasury_stock` DECIMAL(20,4) COMMENT '库存股',
    `specific_reserves` DECIMAL(20,4) COMMENT '	专项储备',
    `surplus_reserve_fund` DECIMAL(20,4) COMMENT '盈余公积金',
    `ordinary_risk_reserve_fund` DECIMAL(20,4) COMMENT '一般风险准备',
    `retained_profit` DECIMAL(20,4) COMMENT '未分配利润',
    `foreign_currency_report_conv_diff` DECIMAL(20,4) COMMENT '	外币报表折算差额',
    `equities_parent_company_owners` DECIMAL(20,4) COMMENT '归属于母公司股东权益合计',
    `minority_interests` DECIMAL(20,4) COMMENT '	少数股东权益',
    `total_owner_equities` DECIMAL(20,4) COMMENT '	股东权益合计',
    `total_sheet_owner_equities` DECIMAL(20,4) COMMENT '	负债和股东权益合计'
) COMMENT '资产负债表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_balance/';

--现金流量表  stk_cash_flow
DROP TABLE IF EXISTS ods_stk_cash_flow;
CREATE EXTERNAL TABLE ods_stk_cash_flow(
    `id` STRING COMMENT 'id',
    `code` STRING COMMENT '股票代码',
    `pubDate` STRING COMMENT '公司发布财报日期',
    `statDate` STRING COMMENT '财报统计的季度的最后一天',
    `goods_sale_and_service_render_cash` DECIMAL(20,4) COMMENT '销售商品、提供劳务收到的现金',
    `net_deposit_increase` DECIMAL(20,4) COMMENT '客户存款和同业存放款项净增加额',
    `net_borrowing_from_central_bank` DECIMAL(20,4) COMMENT '向中央银行借款净增加额',
    `net_borrowing_from_finance_co` DECIMAL(20, 4) COMMENT '向其他金融机构拆入资金净增加额',
    `net_original_insurance_cash` DECIMAL(20, 4) COMMENT '收到原保险合同保费取得的现金',
    `net_cash_received_from_reinsurance_business` DECIMAL(20, 4) COMMENT '收到再保险业务现金净额',
    `net_insurer_deposit_investment` DECIMAL(20, 4) COMMENT '保户储金及投资款净增加额',
    `net_deal_trading_assets` DECIMAL(20, 4) COMMENT '处置交易性金融资产净增加额',
    `interest_and_commission_cashin` DECIMAL(20, 4) COMMENT '收取利息、手续费及佣金的现金',
    `net_increase_in_placements` DECIMAL(20, 4) COMMENT '拆入资金净增加额',
    `net_buyback` DECIMAL(20, 4) COMMENT '回购业务资金净增加额',
    `tax_levy_refund` DECIMAL(20, 4) COMMENT '收到的税费返还',
    `other_cashin_related_operate` DECIMAL(20, 4) COMMENT '收到其他与经营活动有关的现金',
    `subtotal_operate_cash_inflow` DECIMAL(20, 4) COMMENT '经营活动现金流入小计',
    `goods_and_services_cash_paid` DECIMAL(20, 4) COMMENT '购买商品、接受劳务支付的现金',
    `net_loan_and_advance_increase` DECIMAL(20, 4) COMMENT '客户贷款及垫款净增加额',
    `net_deposit_in_cb_and_ib` DECIMAL(20, 4) COMMENT '存放中央银行和同业款项净增加额',
    `original_compensation_paid` DECIMAL(20, 4) COMMENT '支付原保险合同赔付款项的现金',
    `handling_charges_and_commission` DECIMAL(20, 4) COMMENT '支付利息、手续费及佣金的现金',
    `policy_dividend_cash_paid` DECIMAL(20, 4) COMMENT '支付保单红利的现金',
    `staff_behalf_paid` DECIMAL(20, 4) COMMENT '支付给职工以及为职工支付的现金',
    `tax_payments` DECIMAL(20, 4) COMMENT '支付的各项税费',
    `other_operate_cash_paid` DECIMAL(20, 4) COMMENT '支付其他与经营活动有关的现金',
    `subtotal_operate_cash_outflow` DECIMAL(20, 4) COMMENT '经营活动现金流出小计',
    `net_operate_cash_flow` DECIMAL(20, 4) COMMENT '经营活动产生的现金流量净额',
    `invest_withdrawal_cash` DECIMAL(20, 4) COMMENT '收回投资收到的现金',
    `invest_proceeds` DECIMAL(20, 4) COMMENT '取得投资收益收到的现金',
    `fix_intan_other_asset_dispo_cash` DECIMAL(20, 4) COMMENT '处置固定资产、无形资产和其他长期资产收回的现金净额',
    `net_cash_deal_subcompany` DECIMAL(20, 4) COMMENT '处置子公司及其他营业单位收到的现金净额',
    `other_cash_from_invest_act` DECIMAL(20, 4) COMMENT '收到其他与投资活动有关的现金',
    `subtotal_invest_cash_inflow` DECIMAL(20, 4) COMMENT '投资活动现金流入小计',
    `fix_intan_other_asset_acqui_cash` DECIMAL(20, 4) COMMENT '购建固定资产、无形资产和其他长期资产支付的现金',
    `invest_cash_paid` DECIMAL(20, 4) COMMENT '投资支付的现金',
    `impawned_loan_net_increase` DECIMAL(20, 4) COMMENT '质押贷款净增加额',
    `net_cash_from_sub_company` DECIMAL(20, 4) COMMENT '取得子公司及其他营业单位支付的现金净额',
    `other_cash_to_invest_act` DECIMAL(20, 4) COMMENT '支付其他与投资活动有关的现金',
    `subtotal_invest_cash_outflow` DECIMAL(20, 4) COMMENT '投资活动现金流出小计',
    `net_invest_cash_flow` DECIMAL(20, 4) COMMENT '投资活动产生的现金流量净额',
    `cash_from_invest` DECIMAL(20, 4) COMMENT '吸收投资收到的现金',
    `cash_from_mino_s_invest_sub` DECIMAL(20, 4) COMMENT '子公司吸收少数股东投资收到的现金',
    `cash_from_borrowing` DECIMAL(20, 4) COMMENT '取得借款收到的现金',
    `cash_from_bonds_issue` DECIMAL(20, 4) COMMENT '发行债券收到的现金',
    `other_finance_act_cash` DECIMAL(20, 4) COMMENT '收到其他与筹资活动有关的现金',
    `subtotal_finance_cash_inflow` DECIMAL(20, 4) COMMENT '筹资活动现金流入小计',
    `borrowing_repayment` DECIMAL(20, 4) COMMENT '偿还债务支付的现金',
    `dividend_interest_payment` DECIMAL(20, 4) COMMENT '分配股利、利润或偿付利息支付的现金',
    `proceeds_from_sub_to_mino_s` DECIMAL(20, 4) COMMENT '子公司支付给少数股东的股利、利润',
    `other_finance_act_payment` DECIMAL(20, 4) COMMENT '支付其他与筹资活动有关的现金',
    `subtotal_finance_cash_outflow` DECIMAL(20, 4) COMMENT '筹资活动现金流出小计',
    `net_finance_cash_flow` DECIMAL(20, 4) COMMENT '筹资活动产生的现金流量净额',
    `exchange_rate_change_effect` DECIMAL(20, 4) COMMENT '汇率变动对现金及现金等价物的影响',
    `cash_equivalent_increase` DECIMAL(20, 4) COMMENT '现金及现金等价物净增加额',
    `cash_equivalents_at_beginning` DECIMAL(20, 4) COMMENT '期初现金及现金等价物余额',
    `cash_and_equivalents_at_end` DECIMAL(20, 4) COMMENT '期末现金及现金等价物余额(元)	现金流量表科'
) COMMENT '现金流量表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_cash_flow/';

--公司详细信息表  stk_company_info
DROP TABLE IF EXISTS ods_stk_company_info;
CREATE EXTERNAL TABLE ods_stk_company_info(
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
    `province_id` STRING COMMENT '所属省份编码	',
    `province` STRING COMMENT '所属省份',
    `city_id` STRING COMMENT '所属城市编码',
    `city` STRING COMMENT '所属城市',
    `industry_id` STRING COMMENT '行业编码',
    `industry_one` STRING COMMENT '行业一级分类',
    `industry_two` STRING COMMENT '行业二级分类',
    `cpafirm` STRING COMMENT '会计师事务所',
    `lawfirm` STRING COMMENT '律师事务所',
    `ceo` STRING COMMENT '总经理',
    `comments` STRING COMMENT '备注'
) COMMENT '公司详细信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_company_info/';

--每日价格变化表  stk_day_price
DROP TABLE IF EXISTS ods_stk_day_price;
CREATE EXTERNAL TABLE ods_stk_day_price(
    `id` STRING COMMENT 'id',
    `code` STRING COMMENT '分摊活动优惠',
    `code_day` STRING COMMENT '分摊活动优惠',
    `open` DECIMAL(20,4) COMMENT '时间段开始时价格',
    `close` DECIMAL(20,4) COMMENT '时间段结束时价格',
    `low` DECIMAL(20,4) COMMENT '时间段中的最低价',
    `high` DECIMAL(20,4) COMMENT '时间段中的最高价',
    `volume` DECIMAL(20,4) COMMENT '时间段中的成交的股票数量',
    `money` DECIMAL(20,4) COMMENT '时间段中的成交的金额	',
    `factor` DECIMAL(20,4) COMMENT 'pre前复权',
    `high_limit` DECIMAL(20,4) COMMENT '时间段中的涨停价',
    `low_limit` DECIMAL(20,4) COMMENT '时间段中的跌停价',
    `avg` DECIMAL(20,4) COMMENT '时间段中的平均价',
    `pre_close` DECIMAL(20,4) COMMENT '前一个单位时间结束时的价格,按天则是前一天的收盘价',
    `paused` DECIMAL(20,4) COMMENT 'bool值,股票是否停牌'
) COMMENT '每日价格变化表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_day_price/';

--每日价格变化表  stk_day_valuation
DROP TABLE IF EXISTS ods_stk_day_valuation;
CREATE EXTERNAL TABLE ods_stk_day_valuation(
    `id` STRING COMMENT 'id',
    `code` STRING COMMENT '分摊活动优惠',
    `code_day` STRING COMMENT '分摊活动优惠',
    `capitalization` DECIMAL(20,4) COMMENT '总股本(万股)',
    `circulating_cap` DECIMAL(20,4) COMMENT '流通股本(万股)',
    `market_cap` DECIMAL(20,4) COMMENT '总市值(亿元)',
    `circulating_market_cap` DECIMAL(20,4) COMMENT '流通市值(亿元)',
    `turnover_ratio` DECIMAL(20,4) COMMENT '换手率(%)',
    `pe_ratio` DECIMAL(20,4) COMMENT '市盈率(PE, TTM)',
    `pe_ratio_lyr` DECIMAL(20,4) COMMENT '市盈率(PE)',
    `pb_ratio` DECIMAL(20,4) COMMENT '市净率(PB)',
    `ps_ratio` DECIMAL(20,4) COMMENT '市销率(PS, TTM)'
) COMMENT '每日价格变化表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_day_valuation/';

--员工情况
DROP TABLE IF EXISTS ods_stk_employee_info;
CREATE EXTERNAL TABLE ods_stk_employee_info(
    `id` STRING COMMENT '公司ID',
    `code` STRING COMMENT '证券代码',
    `name` STRING COMMENT '证券名称',
    `end_date` STRING COMMENT '报告期截止日',
    `pub_date` STRING COMMENT '公告日期',
    `employee` INTEGER COMMENT '在职员工总数',
    `retirement` INTEGER COMMENT '离退休人员',
    `graduate_rate` DECIMAL(20,4) COMMENT '研究生以上人员比例',
    `college_rate` DECIMAL(20,4) COMMENT '大学专科以上人员比例',
    `middle_rate` DECIMAL(20,4) COMMENT '中专及以下人员比例'
    --pcf_ratio
) COMMENT '员工情况'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_employee_info/';


--股东户数表
DROP TABLE IF EXISTS ods_stk_holder_num;
CREATE EXTERNAL TABLE ods_stk_holder_num(
    `id` STRING COMMENT 'ID',
    `code` STRING COMMENT '股票代码',
    `pub_date` STRING COMMENT '公告日期',
    `end_date` STRING COMMENT '截止日期',
    `share_holders` INTEGER COMMENT '股东总户数'
) COMMENT '股东户数表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_holder_num/';

--利润表
DROP TABLE IF EXISTS ods_stk_income;
CREATE EXTERNAL TABLE ods_stk_income(
    `id` STRING COMMENT 'id',
    `code` STRING COMMENT '股票代码',
    `pubDate` STRING COMMENT '公司发布财报日期',
    `statDate` STRING COMMENT '财报统计的季度的最后一天',
    `total_operating_revenue` DECIMAL(20,4) COMMENT '营业总收入',
    `operating_revenue` DECIMAL(20,4) COMMENT '	营业收入',
    `interest_income` DECIMAL(20,4) COMMENT '利息收入',
    `premiums_earned` DECIMAL(20,4) COMMENT '已赚保费',
    `commission_income` DECIMAL(20,4) COMMENT '	手续费及佣金收入',
    `total_operating_cost` DECIMAL(20,4) COMMENT '营业总成本',
    `operating_cost` DECIMAL(20,4) COMMENT '营业成本',
    `interest_expense` DECIMAL(20,4) COMMENT '利息支出',
    `commission_expense` DECIMAL(20,4) COMMENT '手续费及佣金支出',
    `refunded_premiums` DECIMAL(20,4) COMMENT '退保金',
    `net_pay_insurance_claims` DECIMAL(20,4) COMMENT '赔付支出净额(元)',
    `withdraw_insurance_contract_reserve` DECIMAL(20,4) COMMENT '提取保险合同准备金净额(元)',
    `policy_dividend_payout` DECIMAL(20,4) COMMENT '保单红利支出(元)',
    `reinsurance_cost` DECIMAL(20,4) COMMENT '分保费用(元)',
    `operating_tax_surcharges` DECIMAL(20,4) COMMENT '营业税金及附加(元)',
    `sale_expense` DECIMAL(20,4) COMMENT '销售费用(元)',
    `administration_expense` DECIMAL(20,4) COMMENT '管理费用(元)',
    `financial_expense` DECIMAL(20,4) COMMENT '	财务费用(元)',
    `asset_impairment_loss` DECIMAL(20,4) COMMENT '资产减值损失(元)',
    `fair_value_variable_income` DECIMAL(20,4) COMMENT '公允价值变动收益(元)',
    `investment_income` DECIMAL(20,4) COMMENT '投资收益(元)',
    `invest_income_associates` DECIMAL(20,4) COMMENT '对联营企业和合营企业的投资收益(元)',
    `exchange_income` DECIMAL(20,4) COMMENT '汇兑收益(元)',
    `operating_profit` DECIMAL(20,4) COMMENT '营业利润(元)',
    `non_operating_revenue` DECIMAL(20,4) COMMENT '营业外收入(元)',
    `non_operating_expense` DECIMAL(20,4) COMMENT '营业外支出(元)',
    `disposal_loss_non_current_liability` DECIMAL(20,4) COMMENT '非流动资产处置净损失(元)',
    `total_profit` DECIMAL(20,4) COMMENT '利润总额(元)',
    `income_tax_expense` DECIMAL(20,4) COMMENT '所得税费用(元)',
    `net_profit` DECIMAL(20,4) COMMENT '净利润(元)',
    `np_parent_company_owners` DECIMAL(20,4) COMMENT '归属于母公司股东的净利润(元)',
    `minority_profit` DECIMAL(20,4) COMMENT '少数股东损益(元)',
    `basic_eps` DECIMAL(20,4) COMMENT '基本每股收益(元)',
    `diluted_eps` DECIMAL(20,4) COMMENT '稀释每股收益(元)',
    `other_composite_income` DECIMAL(20,4) COMMENT '其他综合收益(元)',
    `total_composite_income` DECIMAL(20,4) COMMENT '综合收益总额(元)',
    `ci_parent_company_owners` DECIMAL(20,4) COMMENT '归属于母公司所有者的综合收益总额(元)',
    `ci_minority_owners` DECIMAL(20,4) COMMENT '归属于少数股东的综合收益总额(元)'
) COMMENT '利润表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_income/';

--行业信息关系表
DROP TABLE IF EXISTS ods_stk_industry;
CREATE EXTERNAL TABLE ods_stk_industry(
    `id` STRING COMMENT 'ID',
    `industry_code` STRING COMMENT '行业编码',
    `industry_name` STRING COMMENT '行业名称',
    `type` STRING COMMENT '类型',
    `code` STRING COMMENT '股票代码'
) COMMENT '行业信息关系表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/stockdata/ods/ods_stk_industry/';

