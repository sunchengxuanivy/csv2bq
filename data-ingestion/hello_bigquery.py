# -*- coding: UTF-8 -*-
from google.cloud import bigquery

# from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("load-data")
# sc = SparkContext(conf=conf)

"""
    Hello World from Bigquery, @Matthew, you may ignore this script.
"""



client = bigquery.Client(project='fifty-shades-of-brown')
dataset_id = 'my_dataset'
dataset_ref = client.create_dataset(dataset_id, exists_ok=True)
job_config = bigquery.LoadJobConfig()
job_config.schema = [
    bigquery.SchemaField('id', 'STRING'),
    bigquery.SchemaField('member_id', 'STRING'),
    bigquery.SchemaField('loan_amnt', 'STRING'),
    bigquery.SchemaField('funded_amnt', 'STRING'),
    bigquery.SchemaField('funded_amnt_inv', 'STRING'),
    bigquery.SchemaField('term', 'STRING'),
    bigquery.SchemaField('int_rate', 'STRING'),
    bigquery.SchemaField('installment', 'STRING'),
    bigquery.SchemaField('grade', 'STRING'),
    bigquery.SchemaField('sub_grade', 'STRING'),
    bigquery.SchemaField('emp_title', 'STRING'),
    bigquery.SchemaField('emp_length', 'STRING'),
    bigquery.SchemaField('home_ownership', 'STRING'),
    bigquery.SchemaField('annual_inc', 'STRING'),
    bigquery.SchemaField('verification_status', 'STRING'),
    bigquery.SchemaField('issue_d', 'STRING'),
    bigquery.SchemaField('loan_status', 'STRING'),
    bigquery.SchemaField('pymnt_plan', 'STRING'),
    bigquery.SchemaField('url', 'STRING'),
    bigquery.SchemaField('desc', 'STRING'),
    bigquery.SchemaField('purpose', 'STRING'),
    bigquery.SchemaField('title', 'STRING'),
    bigquery.SchemaField('zip_code', 'STRING'),
    bigquery.SchemaField('addr_state', 'STRING'),
    bigquery.SchemaField('dti', 'STRING'),
    bigquery.SchemaField('delinq_2yrs', 'STRING'),
    bigquery.SchemaField('earliest_cr_line', 'STRING'),
    bigquery.SchemaField('inq_last_6mths', 'STRING'),
    bigquery.SchemaField('mths_since_last_delinq', 'STRING'),
    bigquery.SchemaField('mths_since_last_record', 'STRING'),
    bigquery.SchemaField('open_acc', 'STRING'),
    bigquery.SchemaField('pub_rec', 'STRING'),
    bigquery.SchemaField('revol_bal', 'STRING'),
    bigquery.SchemaField('revol_util', 'STRING'),
    bigquery.SchemaField('total_acc', 'STRING'),
    bigquery.SchemaField('initial_list_status', 'STRING'),
    bigquery.SchemaField('out_prncp', 'STRING'),
    bigquery.SchemaField('out_prncp_inv', 'STRING'),
    bigquery.SchemaField('total_pymnt', 'STRING'),
    bigquery.SchemaField('total_pymnt_inv', 'STRING'),
    bigquery.SchemaField('total_rec_prncp', 'STRING'),
    bigquery.SchemaField('total_rec_int', 'STRING'),
    bigquery.SchemaField('total_rec_late_fee', 'STRING'),
    bigquery.SchemaField('recoveries', 'STRING'),
    bigquery.SchemaField('collection_recovery_fee', 'STRING'),
    bigquery.SchemaField('last_pymnt_d', 'STRING'),
    bigquery.SchemaField('last_pymnt_amnt', 'STRING'),
    bigquery.SchemaField('next_pymnt_d', 'STRING'),
    bigquery.SchemaField('last_credit_pull_d', 'STRING'),
    bigquery.SchemaField('collections_12_mths_ex_med', 'STRING'),
    bigquery.SchemaField('mths_since_last_major_derog', 'STRING'),
    bigquery.SchemaField('policy_code', 'STRING'),
    bigquery.SchemaField('application_type', 'STRING'),
    bigquery.SchemaField('annual_inc_joint', 'STRING'),
    bigquery.SchemaField('dti_joint', 'STRING'),
    bigquery.SchemaField('verification_status_joint', 'STRING'),
    bigquery.SchemaField('acc_now_delinq', 'STRING'),
    bigquery.SchemaField('tot_coll_amt', 'STRING'),
    bigquery.SchemaField('tot_cur_bal', 'STRING'),
    bigquery.SchemaField('open_acc_6m', 'STRING'),
    bigquery.SchemaField('open_act_il', 'STRING'),
    bigquery.SchemaField('open_il_12m', 'STRING'),
    bigquery.SchemaField('open_il_24m', 'STRING'),
    bigquery.SchemaField('mths_since_rcnt_il', 'STRING'),
    bigquery.SchemaField('total_bal_il', 'STRING'),
    bigquery.SchemaField('il_util', 'STRING'),
    bigquery.SchemaField('open_rv_12m', 'STRING'),
    bigquery.SchemaField('open_rv_24m', 'STRING'),
    bigquery.SchemaField('max_bal_bc', 'STRING'),
    bigquery.SchemaField('all_util', 'STRING'),
    bigquery.SchemaField('total_rev_hi_lim', 'STRING'),
    bigquery.SchemaField('inq_fi', 'STRING'),
    bigquery.SchemaField('total_cu_tl', 'STRING'),
    bigquery.SchemaField('inq_last_12m', 'STRING'),
    bigquery.SchemaField('acc_open_past_24mths', 'STRING'),
    bigquery.SchemaField('avg_cur_bal', 'STRING'),
    bigquery.SchemaField('bc_open_to_buy', 'STRING'),
    bigquery.SchemaField('bc_util', 'STRING'),
    bigquery.SchemaField('chargeoff_within_12_mths', 'STRING'),
    bigquery.SchemaField('delinq_amnt', 'STRING'),
    bigquery.SchemaField('mo_sin_old_il_acct', 'STRING'),
    bigquery.SchemaField('mo_sin_old_rev_tl_op', 'STRING'),
    bigquery.SchemaField('mo_sin_rcnt_rev_tl_op', 'STRING'),
    bigquery.SchemaField('mo_sin_rcnt_tl', 'STRING'),
    bigquery.SchemaField('mort_acc', 'STRING'),
    bigquery.SchemaField('mths_since_recent_bc', 'STRING'),
    bigquery.SchemaField('mths_since_recent_bc_dlq', 'STRING'),
    bigquery.SchemaField('mths_since_recent_inq', 'STRING'),
    bigquery.SchemaField('mths_since_recent_revol_delinq', 'STRING'),
    bigquery.SchemaField('num_accts_ever_120_pd', 'STRING'),
    bigquery.SchemaField('num_actv_bc_tl', 'STRING'),
    bigquery.SchemaField('num_actv_rev_tl', 'STRING'),
    bigquery.SchemaField('num_bc_sats', 'STRING'),
    bigquery.SchemaField('num_bc_tl', 'STRING'),
    bigquery.SchemaField('num_il_tl', 'STRING'),
    bigquery.SchemaField('num_op_rev_tl', 'STRING'),
    bigquery.SchemaField('num_rev_accts', 'STRING'),
    bigquery.SchemaField('num_rev_tl_bal_gt_0', 'STRING'),
    bigquery.SchemaField('num_sats', 'STRING'),
    bigquery.SchemaField('num_tl_120dpd_2m', 'STRING'),
    bigquery.SchemaField('num_tl_30dpd', 'STRING'),
    bigquery.SchemaField('num_tl_90g_dpd_24m', 'STRING'),
    bigquery.SchemaField('num_tl_op_past_12m', 'STRING'),
    bigquery.SchemaField('pct_tl_nvr_dlq', 'STRING'),
    bigquery.SchemaField('percent_bc_gt_75', 'STRING'),
    bigquery.SchemaField('pub_rec_bankruptcies', 'STRING'),
    bigquery.SchemaField('tax_liens', 'STRING'),
    bigquery.SchemaField('tot_hi_cred_lim', 'STRING'),
    bigquery.SchemaField('total_bal_ex_mort', 'STRING'),
    bigquery.SchemaField('total_bc_limit', 'STRING'),
    bigquery.SchemaField('total_il_high_credit_limit', 'STRING'),
    bigquery.SchemaField('revol_bal_joint', 'STRING'),
    bigquery.SchemaField('sec_app_earliest_cr_line', 'STRING'),
    bigquery.SchemaField('sec_app_inq_last_6mths', 'STRING'),
    bigquery.SchemaField('sec_app_mort_acc', 'STRING'),
    bigquery.SchemaField('sec_app_open_acc', 'STRING'),
    bigquery.SchemaField('sec_app_revol_util', 'STRING'),
    bigquery.SchemaField('sec_app_open_act_il', 'STRING'),
    bigquery.SchemaField('sec_app_num_rev_accts', 'STRING'),
    bigquery.SchemaField('sec_app_chargeoff_within_12_mths', 'STRING'),
    bigquery.SchemaField('sec_app_collections_12_mths_ex_med', 'STRING'),
    bigquery.SchemaField('sec_app_mths_since_last_major_derog', 'STRING'),
    bigquery.SchemaField('hardship_flag', 'STRING'),
    bigquery.SchemaField('hardship_type', 'STRING'),
    bigquery.SchemaField('hardship_reason', 'STRING'),
    bigquery.SchemaField('hardship_status', 'STRING'),
    bigquery.SchemaField('deferral_term', 'STRING'),
    bigquery.SchemaField('hardship_amount', 'STRING'),
    bigquery.SchemaField('hardship_start_date', 'STRING'),
    bigquery.SchemaField('hardship_end_date', 'STRING'),
    bigquery.SchemaField('payment_plan_start_date', 'STRING'),
    bigquery.SchemaField('hardship_length', 'STRING'),
    bigquery.SchemaField('hardship_dpd', 'STRING'),
    bigquery.SchemaField('hardship_loan_status', 'STRING'),
    bigquery.SchemaField('orig_projected_additional_accrued_interest', 'STRING'),
    bigquery.SchemaField('hardship_payoff_balance_amount', 'STRING'),
    bigquery.SchemaField('hardship_last_payment_amount', 'STRING'),
    bigquery.SchemaField('disbursement_method', 'STRING'),
    bigquery.SchemaField('debt_settlement_flag', 'STRING'),
    bigquery.SchemaField('debt_settlement_flag_date', 'STRING'),
    bigquery.SchemaField('settlement_status', 'STRING'),
    bigquery.SchemaField('settlement_date', 'STRING'),
    bigquery.SchemaField('settlement_amount', 'STRING'),
    bigquery.SchemaField('settlement_percentage', 'STRING'),
    bigquery.SchemaField('settlement_term', 'STRING')
]
job_config.skip_leading_rows = 1
# The source format defaults to CSV, so the line below is optional.
job_config.source_format = bigquery.SourceFormat.CSV
uri = 'gs://src_raw-data_bucket/loan.csv'
load_job = client.load_table_from_uri(
    uri,
    dataset_ref.table('loan'),
    job_config=job_config)  # API request
print('Starting job {}'.format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
print('Job finished.')

destination_table = client.get_table(dataset_ref.table('loan'))
print('Loaded {} rows.'.format(destination_table.num_rows))


# sc.stop()
