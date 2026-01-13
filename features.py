from typing import Dict, Any
from datetime import datetime
from utils import Static_Utils, Global
from pathlib import Path
# 2025-12-30 Fadi
import pickle as pkl

class FeatureExtraction:
    
    def __init__(self, config):
        """Initialize feature extraction with configuration"""
        self.config = config

    @staticmethod
    def calculate_cmgr(start, end, periods):
        return (end / start).pow(1 / periods) - 1

    @staticmethod
    def read_input_data(config):
        preprocess_outputs = ["dda_monthly_agg", "dda_last12M_cumulative_agg", "demographics", "ics_monthly_agg", "ics_last12M_cumulative_agg"]
        return {preprocess_output:Static_Utils.read_output_parquet(f'preprocess/{preprocess_output}.parquet') for preprocess_output in preprocess_outputs}

    ## Lookalike Functions
    def __filter_data(dfs):
        # Set date parameters
        df_demog = dfs["demographics"]
        end_month = dfs["dda_monthly_agg"]["month"].max()
        start_month = end_month - pd.DateOffset(months=12)
        
        df_dda_monthly = dfs["dda_monthly_agg"][(dfs["dda_monthly_agg"]["month"] >= start_month) & (dfs["dda_monthly_agg"]["month"] <= end_month)]
        df_dda_last12m = dfs["dda_last12M_cumulative_agg"][dfs["dda_last12M_cumulative_agg"]["month"] == end_month]
        df_ics_monthly = dfs["ics_monthly_agg"][(dfs["ics_monthly_agg"]["month"] >= start_month) & (dfs["ics_monthly_agg"]["month"] <= end_month)]
        df_ics_last12m = dfs["ics_last12M_cumulative_agg"][dfs["ics_last12M_cumulative_agg"]["month"] == end_month]
        return df_dda_monthly, df_dda_last12m, df_demog, df_ics_monthly, df_ics_last12m

    def calculate_days_to_last_txn(row):
        """Calculate days between max transaction date and last day of month"""
        if pd.isna(row["max_txn_dt"]):
            return np.nan
        # Get the last day of the month as a Timestamp
        last_day = calendar.monthrange(row["month"].year, row["month"].month)[1]
        last_day_of_month = pd.Timestamp(row["month"].year, row["month"].month, last_day)
        return (last_day_of_month - row["max_txn_dt"]).days
    
    def calculate_growth_metrics_lookalike(feature_mappings, dda_df, ics_df):
        """Calculate growth metrics using shift operation for all metrics"""
        # 2026-01-06 Fadi
        Global.log("calculate_growth_metrics_lookalike() 1")
        metrics_map = feature_mappings["growth_metrics_map"]
        ics_metrics_map = feature_mappings["ics_growth_metrics_map"]
        dda_growth_cols, ics_growth_cols = [], []
        Global.log("calculate_growth_metrics_lookalike() 2")
        
        for period in [3, 6, 12]:
            Global.log("calculate_growth_metrics_lookalike() 3")
            for metric_name, col_name in metrics_map.items():
                growth_col = f"{metric_name}_growth_last_{period}m"
                grouped = dda_df.groupby("CUSTID")[col_name]
                dda_df[growth_col] = (grouped.shift(0) / grouped.shift(period)).pow(1 / period) - 1
                dda_growth_cols.append(growth_col)
            #Calculate ICS growth metrics
            for metric_name, col_name in ics_metrics_map.items():
                growth_col = f"{metric_name}_growth_last_{period}m"
                grouped = ics_df.groupby("CUSTID")[col_name]
                ics_df[growth_col] = (grouped.shift(0) / grouped.shift(period)).pow(1 / period) - 1
                ics_growth_cols.append(growth_col)
            Global.log("calculate_growth_metrics_lookalike() 4")
        Global.log("calculate_growth_metrics_lookalike() 5")
        dda_df_final = dda_df[["CUSTID"] + dda_growth_cols].groupby("CUSTID").last().reset_index().drop_duplicates()
        Global.log("calculate_growth_metrics_lookalike() 6")
        ics_df_final = ics_df[["CUSTID"] + ics_growth_cols].groupby("CUSTID").last().reset_index().drop_duplicates()
        Global.log("calculate_growth_metrics_lookalike() 7")
        result_df = dda_df_final.merge(ics_df_final, on="CUSTID", how="outer").fillna(0).drop_duplicates(subset=["CUSTID"])
        Global.log("calculate_growth_metrics_lookalike() 8")
        return result_df

    def create_basic_features(feature_mappings, dda_source_df, ics_source_df):
        """Create basic transaction and balance features"""
        df_basic = pd.DataFrame({"CUSTID": dda_source_df["CUSTID"]})
        for target_col, source_col in feature_mappings["basic_features"].items():
            df_basic[target_col] = dda_source_df[source_col]
        
        for col in feature_mappings["txncode_columns"]:
            df_basic[col] = dda_source_df[col]

        for col in [col for col in ics_source_df.columns if col not in ["CUSTID", "month"]]:
            df_basic[col] = ics_source_df[col]
        
        # Calculate averages
        df_basic["avg_txn_amount"] = dda_source_df["total_transactions_amount_last_12_months"] / dda_source_df["total_transactions_count_last_12_months"]
        df_basic["avg_balance"] = dda_source_df["total_balance_last_12_months"] / dda_source_df["total_transactions_count_last_12_months"]
        df_basic["monthly_avg_balance"] = dda_source_df["current_balance"] / 12
        df_basic["monthly_avg_txn_count"] = dda_source_df["total_transactions_count_last_12_months"] / 12
        return df_basic

    def create_credit_debit_features(feature_mappings, source_df):
        """Create credit and debit transaction features"""
        df_credit_debit = pd.DataFrame({"CUSTID": source_df["CUSTID"]})
        for feature_type in ["credit_features", "debit_features"]:
            for target_col, source_col in feature_mappings[feature_type].items():
                df_credit_debit[target_col] = source_df[source_col]

            # Calculate averages
            type_prefix = feature_type.split("_")[0]  # "credit" or "debit"
            df_credit_debit[f"avg_{type_prefix}_amount"] = df_credit_debit[f"total_{type_prefix}_amount"] / df_credit_debit[f"total_{type_prefix}_txns"]
            df_credit_debit[f"monthly_avg_{type_prefix}_amount"] = df_credit_debit[f"total_{type_prefix}_amount"] / 12

        return df_credit_debit

    # 2025-12-30 Fadi
    @staticmethod
    def extract_lookalike_features(config, lookalike_data):
        """Extract and save features for lookalike modeling"""
        Global.log("extract_lookalike_features() 1")
        feature_mappings = config["parameters"]["lookalike"]["feature_mappings"]
        Global.log("extract_lookalike_features() 2")
        df_dda_monthly, df_dda_last12m, df_demog, df_ics_monthly, df_ics_last12m = lookalike_data
        Global.log("extract_lookalike_features() 3")
        
        # Initialize features DataFrame with CUSTID and calculating basic features
        df_features = pd.DataFrame({"CUSTID": df_dda_last12m["CUSTID"]})
        Global.log("extract_lookalike_features() 4")
        df_features["days_to_last_txn"] = df_dda_last12m.apply(FeatureExtraction.calculate_days_to_last_txn, axis=1)
        Global.log("extract_lookalike_features() 5")
        df_basic_features = FeatureExtraction.create_basic_features(feature_mappings, df_dda_last12m, df_ics_last12m)
        Global.log("extract_lookalike_features() 6")

        df_credit_debit_features = FeatureExtraction.create_credit_debit_features(feature_mappings, df_dda_last12m)
        Global.log("extract_lookalike_features() 7")

        df_features = df_features.merge(df_basic_features, on="CUSTID", how="inner")
        Global.log("extract_lookalike_features() 8")
        df_features = df_features.merge(df_credit_debit_features, on="CUSTID", how="inner").drop_duplicates()
        Global.log("extract_lookalike_features() 9")
        
        # Calculate and add growth metrics
        growth_metrics = FeatureExtraction.calculate_growth_metrics_lookalike(feature_mappings, df_dda_monthly, df_ics_monthly)
        Global.log("extract_lookalike_features() 10")
        df_features = df_features.merge(growth_metrics, on="CUSTID", how="inner")
        Global.log("extract_lookalike_features() 11")
        
        # Add demographics, txncode and account features
        all_additional_features = (feature_mappings["demographic_features"] + feature_mappings["account_columns"])
        Global.log("extract_lookalike_features() 12")
        df_features = pd.merge(df_features, df_demog[["CUSTID"] + all_additional_features], on="CUSTID", how="left")
        Global.log("extract_lookalike_features() 13")
        df_features[feature_mappings["account_columns"]] = df_features[feature_mappings["account_columns"]].apply(pd.to_numeric, downcast="integer")
        Global.log("extract_lookalike_features() 14")
        Static_Utils.to_parquet(df_features.fillna(0).drop_duplicates(subset=["CUSTID"]), "features/lookalike.parquet")
        Global.log("extract_lookalike_features() 15")

    ## Additional Income
    def calculate_growth_metrics_add_inc(self, df):
        growth_mapping = {
            "total_credit_transaction_amount_last_1_months": "monthly_credit_transaction_amount_growth_last_12m",
            "total_debit_transaction_amount_last_1_months": "monthly_debit_transaction_amount_growth_last_12m",
            "total_transactions_amount_last_1_months": "monthly_transaction_amount_growth_last_12m"
            }

        growth_metrics = {
            "monthly_balance_growth_last_12m": "average_balance_last_1_months",
            "monthly_transaction_count_growth_last_12m": "total_transactions_count_last_1_months"
            } 
        for new_column, original_column in growth_metrics.items():
            df[new_column] = df.groupby("CUSTID")[original_column].apply(
            lambda x: FeatureExtraction.calculate_cmgr(x.shift(12), x, 12)
            ).reset_index(level=0, drop=True)

        for column in df.columns:
            for suffix, new_suffix in growth_mapping.items():
                if column.endswith(suffix):
                    prefix = column.replace(suffix, "")
                    new_column_name = f"{prefix}{new_suffix}"
                    df[new_column_name] = df.groupby("CUSTID")[column].apply(
                        lambda x: FeatureExtraction.calculate_cmgr(x.shift(12), x, 12)
                    ).reset_index(level=0, drop=True)
                    break  

        cols_to_keep = ["CUSTID", "month","current_balance"] + [col for col in df.columns if col.endswith("growth_last_12m")]    
        df = df[cols_to_keep]
        return df
    
    def extract_common_features(self, dfs):
        # Read input data
        dda_cum_df = dfs["dda_last12M_cumulative_agg"]
        # 2025-12-16 Fadi
        #dda_cum_df = dda_cum_df.drop(columns=[col for col in dda_cum_df.columns if col.startswith('txncode')] + ["current_balance", "max_txn_dt","high_value_txn_freq", "tax_refund_cnt",  "tax_refund_amt","additional_income_amt","bonus_amt", "salary", "total_balance_last_12_months", "operator_Others_total_transactions_count_last_12_months",    "operator_Others_total_credit_transaction_amount_last_12_months", "operator_Others_total_debit_transaction_amount_last_12_months"], axis=1)
        dda_cum_df = dda_cum_df.drop(columns=[col for col in dda_cum_df.columns if col.startswith('txncode')] + ["current_balance", "max_txn_dt","high_value_txn_freq", "tax_refund_cnt",  "tax_refund_amt","additional_income_amt","bonus_amt", "salary", "total_balance_last_12_months", "operator_Others_total_transactions_count_last_12_months",    "operator_Others_total_credit_transaction_amount_last_12_months", "operator_Others_total_debit_transaction_amount_last_12_months"], errors='ignore')
        ics_cum_df = dfs["ics_last12M_cumulative_agg"]
        ics_cum_df = ics_cum_df.loc[:, ~ics_cum_df.columns.str.endswith("_last_1_months")]
        columns_to_process = ics_cum_df.filter(regex="total_transactions_amount_last_12_months")
        for col in columns_to_process:
            new_col_name = col.replace("total_transactions_amount_last_12_months", "monthly_average_spend_last_12_months")
            ics_cum_df[new_col_name] = ics_cum_df[col] / 12
        ics_cum_df.drop(columns=columns_to_process, inplace=True)
        demo_df = dfs["demographics"]
        cols_to_keep = ["CUSTID", "Total_Accnts","Active_Accnts","Dormant_Accnts","Closed_Accnts","DDA_Accnts","ICS_Accnts","TENURE"]
        demo_df = demo_df[cols_to_keep]
        features_df = dda_cum_df.merge(ics_cum_df, on=["CUSTID","month"], how="left")
        features_df = features_df.merge(demo_df, on = "CUSTID", how = "left")
        end_date = pd.Timestamp(dda_cum_df["month"].max())
        label_end_dt = end_date - pd.DateOffset(months=3)
        start_date = label_end_dt - pd.DateOffset(months = 11)
        features_df = features_df[features_df["month"]>=start_date]
        del dda_cum_df, ics_cum_df, demo_df
        return features_df
        
    def extract_growth_features(self, dfs):
        # 2026-01-06 Fadi
        Global.log("extract_growth_features() 1")
        dda_df, ics_df = dfs["dda_monthly_agg"], dfs["ics_monthly_agg"]
        Global.log("extract_growth_features() 2")
        dda_df["average_balance_last_1_months"]=dda_df["total_balance_last_1_months"]/dda_df["total_transactions_count_last_1_months"]
        Global.log("extract_growth_features() 3")
        dda_df["average_balance_last_1_months"]=dda_df["total_balance_last_1_months"]/dda_df["total_transactions_count_last_1_months"]
        Global.log("extract_growth_features() 4")
        dda_df = dda_df.drop(columns=[col for col in dda_df.columns if col.startswith('txncode')] + ["max_txn_dt","total_transactions_amount_last_1_months","total_balance_last_1_months",
        "high_value_txn_freq","tax_refund_cnt","tax_refund_amt", "operator_Others_total_transactions_count_last_1_months",
        "operator_Others_total_credit_transaction_amount_last_1_months",
        "operator_Others_total_debit_transaction_amount_last_1_months","additional_income_amt","bonus_amt","salary"])
        Global.log("extract_growth_features() 5")
        merged_df = dda_df.merge(ics_df, on =["CUSTID", "month"], how ="left")
        Global.log("extract_growth_features() 6")
        merged_df = self.calculate_growth_metrics_add_inc(merged_df)
        Global.log("extract_growth_features() 7")
        del dda_df, ics_df
        Global.log("extract_growth_features() 8")
        return merged_df

    def extract_hist_add_inc_features(self, cat, dfs):
        dda_df = dfs["dda_monthly_agg"][["CUSTID","month", cat]]
        end_date = pd.Timestamp(dda_df["month"].max())
        label_end_dt = end_date - pd.DateOffset(months=3)
        start_date = label_end_dt - pd.DateOffset(months = 11)
        df= dda_df[dda_df["month"]>=start_date]
        if cat == "tax_refund_amt":
            prefix = "Refund_Received"
        elif cat == "bonus_amt":
            prefix = "Bonus_Received"
        elif cat == "additional_income_amt":
            prefix = "Add_Inc_Received"
        else:
            raise ValueError(f"Unknown category: {cat}")

        for n in range(1, 25):
            df["{}_{}_months_ago".format(prefix,n)] = dda_df.groupby("CUSTID")[cat].shift(n).apply(lambda x: 1 if x > 0 else 0)
        df.drop(columns=[cat], axis=1, inplace = True)
        del dda_df
        return df
    
    # 2025-12-30 Fadi
    def build_features_lookalike(config):
        Global.log("build_features_lookalike() 1")
        feature_extractor = FeatureExtraction(config)
        Global.log("build_features_lookalike() 2")
        with Static_Utils.open("features/lookalike_temp.pkl", "rb") as temp_file:
            Global.log("filter_lookalike() 3")
            lookalike_data = pkl.load(temp_file)
            Global.log("filter_lookalike() 4")
        Global.log("build_features_lookalike() 5")
        Global.log("Completed reading all tables")
        feature_extractor.extract_lookalike_features(config, lookalike_data)
        Global.log("build_features_lookalike() 6")

    # 2026-01-13 Fadi
    def build_features_additionalincome_prep(config):
        Global.log("build_features_additionalincome() 1")
        feature_extractor = FeatureExtraction(config)
        Global.log("build_features_additionalincome() 2")
        dfs = feature_extractor.read_input_data(config)
        Global.log("build_features_additionalincome() 3")
        common_features_df = feature_extractor.extract_common_features(dfs)
        with Static_Utils.open("features/add_inc_temp_common_features.pkl", "wb") as temp_file:
            pkl.dump(common_features_df, temp_file)
        growth_features_df = feature_extractor.extract_growth_features(dfs)
        with Static_Utils.open("features/add_inc_temp_growth_features.pkl", "wb") as temp_file:
            pkl.dump(growth_features_df, temp_file)
        hist_base_df = dfs["dda_monthly_agg"]["CUSTID","month", "tax_refund_amt", "bonus_amt", "additional_income_amt"]
        with Static_Utils.open("features/add_inc_temp_hist_base.pkl", "wb") as temp_file:
            pkl.dump(hist_base_df, temp_file)
        Global.log("build_features_additionalincome() 4")

    # 2026-01-13 Fadi
    def build_features_additionalincome_merge(config):
        Global.log("build_features_additionalincome_merge() 1")
        with Static_Utils.open("features/add_inc_temp_common_features.pkl", "rb") as temp_file:
            common_features_df = pkl.load(temp_file)
        Global.log("build_features_additionalincome_merge() 2")
        with Static_Utils.open("features/add_inc_temp_growth_features.pkl", "rb") as temp_file:
            growth_features_df = pkl.load(temp_file)   
        Global.log("build_features_additionalincome_merge() 3")
        merged_df = common_features_df.merge(growth_features_df, on=["CUSTID","month"], how="left")
        Global.log("build_features_additionalincome_merge() 4")
        with Static_Utils.open("features/add_inc_temp_merged_features.pkl", "wb") as temp_file:
            pkl.dump(merged_df, temp_file)
        Global.log("build_features_additionalincome_merge() 5")

    # 2026-01-13 Fadi
    def build_features_additionalincome_final(config):
        Global.log("build_features_additionalincome_final() 1")
        with Static_Utils.open("features/add_inc_temp_merged_features.pkl", "rb") as temp_file:
            df = pkl.load(temp_file)
        Global.log("build_features_additionalincome_final() 2")
        with Static_Utils.open("features/add_inc_temp_hist_base.pkl", "rb") as temp_file:
            dfs = {"dda_monthly_agg": pkl.load(temp_file)}
        Global.log("build_features_additionalincome_final() 3")
        categories = {
            "tax_refund_amt": "features/tax_refund.parquet",
            "bonus_amt": "features/bonus.parquet",
            "additional_income_amt": "features/general_additional_income.parquet"
        }
        feature_extractor = FeatureExtraction(config)
        for cat, output_path in categories.items():
            df_cat = df.merge(feature_extractor.extract_hist_add_inc_features(cat, dfs), on=["CUSTID","month"], how="left")
            Global.log("build_features_additionalincome_final() 4")
            Static_Utils.to_parquet(df_cat, output_path)
            Global.log("build_features_additionalincome_final() 5")
            del df_cat
        Global.log("build_features_additionalincome_final() 6")

    @staticmethod
    def filter_lookalike(config):
        Global.log("filter_lookalike() 1")
        feature_extractor = FeatureExtraction(config)
        Global.log("filter_lookalike() 2")
        dfs = feature_extractor.read_input_data(config)
        Global.log("filter_lookalike() 3")
        lookalike_data = FeatureExtraction.__filter_data(dfs)
        Global.log("filter_lookalike() 4")
        with Static_Utils.open("features/lookalike_temp.pkl", "wb") as temp_file:
            Global.log("filter_lookalike() 5")
            pkl.dump(lookalike_data, temp_file)
            Global.log("filter_lookalike() 6")
