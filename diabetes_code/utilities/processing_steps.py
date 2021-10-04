import sys
from typing import Tuple
from pyspark.sql import functions as F
from pyspark.sql import dataframe as DF
from diabetes_code.utilities.data_connections import get_df_from_SQL, write_df_to_SQL
from diabetes_code.utilities.field_definitions import *
from diabetes_code.params import params

import logging
logger = logging.getLogger(__name__)

def read_and_prepare_data(params: dict):
    """
    This function will read all of the relevant dataframes from SQL, rename columns where necessary,
    and cast the columns to the correct data type where necessary. We put all of this logic in a
    function to help the abstraction of the main script.

    Args:
        config: The config dictionary containing all of the information that changes from one
                publication to the next. This config dict includes details of the SQL tables that
                should be used.

    Returns:
        A tuple containing all of the datasets needed for this publication

    Example:
        nda_demo, nda_bmi, nda_bp, nda_chol, nda_hba1c, ccg_map = read_and_prepare_data(params=params)

    """
    logger.info('Read and prepare input data')
    # Use the params to identify which datasets to import
    try:
        nda_demo = get_df_from_SQL(database=params['nda_demo_table']['database'],
                                table=params['nda_demo_table']['table'])
        nda_bmi = get_df_from_SQL(database=params['nda_bmi_table']['database'],
                                table=params['nda_bmi_table']['table'])
        nda_bp = get_df_from_SQL(database=params['nda_bp_table']['database'],
                                table=params['nda_bp_table']['table'])
        nda_chol = get_df_from_SQL(database=params['nda_chol_table']['database'],
                                table=params['nda_chol_table']['table'])
        nda_hba1c = get_df_from_SQL(database=params['nda_hba1c_table']['database'],
                                table=params['nda_hba1c_table']['table'])
        ccg_map = get_df_from_SQL(database=params['ccg_map']['database'],
                                table=params['ccg_map']['table'])
        hes_diabetes = get_df_from_SQL(database=params['hes_diabetes_table']['database'],
                                table=params['hes_diabetes_table']['table'])
        nda_drug = get_df_from_SQL(database=params['nda_drug_table']['database'],
                                table=params['nda_drug_table']['table'])
        imd_scores = get_df_from_SQL(database=params['imd_scores']['database'],
                                table=params['imd_scores']['table'])

    except KeyError as e:
        logger.error(f"There is an issue with the {e} value in params.py")
        sys.exit(1)


    # Import all of the data that we will require
    # Select only the required columns. Cast and rename to be correct at the same time
    nda_demo = (nda_demo.select(
        'NHS_NUMBER',
        'ORGANISATION_CODE',
        'AGE',
        'DIABETES_TYPE',
        'SEX',
        'ETHNICITY',
        'LSOA',
        'DIAGNOSIS_YEAR',
        'IHD_VALUE',
        'SMOKING_VALUE',
        F.to_date('BIRTH_DATE').alias('BIRTH_DATE'),
        F.to_date('CREATININE_DATE').alias('CREATININE_DATE'),
        F.to_date('ALBUMIN_DATE').alias('ALBUMIN_DATE'),
        F.to_date('EYE_EXAM_DATE').alias('EYE_EXAM_DATE'),
        F.to_date('FOOT_EXAM_DATE').alias('FOOT_EXAM_DATE'),
        F.to_date('SMOKING_DATE').alias('SMOKING_DATE'),
        F.to_date('ED_OFFER_DATE').alias('ED_OFFER_DATE'),
        F.to_date('ED_ATTEND_DATE').alias('ED_ATTEND_DATE'),
        F.to_date('DIAGNOSIS_DATE').alias('DIAGNOSIS_DATE')
        )
        .withColumn('AGE_CLEANED', age_cleaned)
        .drop(F.col('AGE'))
        .withColumnRenamed('AGE_CLEANED', 'AGE')
    )

    nda_chol = nda_chol.select(
        F.col('CHOLESTEROL_VALUE').alias('CHOLESTEROL_VALUE'),
        F.to_date('CHOLESTEROL_DATE').alias('CHOLESTEROL_DATE'),
        F.col('NHS_Number').alias('NHS_NUMBER'),
        F.col('organisation_code').alias('ORGANISATION_CODE'))

    nda_bmi = nda_bmi.select(
        F.col('BMI_VALUE').alias('BMI_VALUE'),
        F.to_date('BMI_DATE').alias('BMI_DATE'),
        F.col('NHS_Number').alias('NHS_NUMBER'),
        F.col('organisation_code').alias('ORGANISATION_CODE'))

    nda_bp = nda_bp.select(
        F.col('NHS_Number').alias('NHS_NUMBER'),
        F.col('organisation_code').alias('ORGANISATION_CODE'),
        F.to_date('BP_DATE').alias('BP_DATE'),
        F.col('SYSTOLIC_VALUE').alias('SYSTOLIC_VALUE'),
        F.col('DIASTOLIC_VALUE').alias('DIASTOLIC_VALUE'),
    )

    nda_hba1c = nda_hba1c.select(
        F.col('NHS_Number').alias('NHS_NUMBER'),
        F.col('HBA1C_VALUE').alias('HBA1C_VALUE'),
        F.col('organisation_code').alias('ORGANISATION_CODE'),
        F.to_date('HBA1C_DATE').alias('HBA1C_DATE'))

    ccg_map = ccg_map.select(
        F.col('Organisation Code').alias('ORGANISATION_CODE'),
        F.col('Organisation name').alias('ORGANISATION_NAME'),
        F.col('CCG Code').alias('CCG_CODE'),
        F.col('Include').alias('INCLUDE')
    )

    hes_diabetes = (
        hes_diabetes
            .select(F.col('NEWNHSNO').alias('NHS_NUMBER'))
            .withColumn('CVD_ADMISSION', F.lit(1))
    )

    nda_drug = nda_drug.select(
        F.col('NHS_NUMBER').alias('NHS_NUMBER'),
        F.col('DATA_FIELD').alias('DATA_FIELD'),
        F.col('ORGANISATION_CODE').alias('ORGANISATION_CODE'),
        F.to_date('DATE').alias('DRUG_DATE')
    )

    imd_scores = (
        imd_scores
            .select(F.col('LSOA_Code').alias('LSOA'),
                    F.col('IMD_Score').alias('IMD_SCORE'),
                    F.col('Combined_Quintile').alias('COMBINED_QUINTILE'))
            .withColumn('IMD_QUINTILE', deprivation_imd_group)
    )

    return nda_demo, nda_bmi, nda_bp, nda_chol, nda_hba1c, ccg_map, hes_diabetes, nda_drug, imd_scores


def assign_record_scores(nda_demo: DF, nda_bmi: DF, nda_bp: DF, nda_chol: DF, nda_hba1c: DF, ccg_map: DF, hes_diabetes: DF, nda_drug: DF) -> DF:
    """
    This function assigns a score to each record. Calculating these scores involves lots of steps 
    so it is better to encapsulate this logic. The function returns a dataframe similar to nda_demo,
    i.e., one row per person per organisation. The dataframe returned includes a 'RECORD_SCORE' 
    field.

    Args:
        
    Returns:
        A dataframe

    Example:
        record_scores = assign_record_scores(
                            nda_demo = nda_demo,
                            nda_bmi = nda_bmi,
                            nda_bp = nda_bp,
                            nda_chol = nda_chol,
                            nda_hba1c = nda_hba1c,
                            ccg_map = ccg_map)
    """
    # Period included in this publication:
    START_DATE = F.to_date(F.lit(params['publication_start_date']))
    END_DATE = F.to_date(F.lit(params['publication_end_date']))

    # Identify the most recent date for each of the supplementary tables
    # .between() should be inclusive - i.e., the two dates mentioned will be in the data
    chol_max_date = (nda_chol
                    .filter(F.col('CHOLESTEROL_DATE').between(START_DATE, END_DATE)) 
                    .groupBy('NHS_NUMBER', 'ORGANISATION_CODE')
                    .agg(F.max('CHOLESTEROL_DATE').alias('CHOLESTEROL_DATE'))
    )
    
    bmi_max_date = (nda_bmi
                    .filter(F.col('BMI_DATE').between(START_DATE, END_DATE)) 
                    .groupBy('NHS_NUMBER', 'ORGANISATION_CODE')
                    .agg(F.max('BMI_DATE').alias('BMI_DATE'))
    )

    bp_max_date = (nda_bp
                    .filter(F.col('BP_DATE').between(START_DATE, END_DATE)) 
                    .groupBy('NHS_NUMBER', 'ORGANISATION_CODE')
                    .agg(F.max('BP_DATE').alias('BP_DATE'))
    )

    hba1c_max_date = (nda_hba1c
                    .filter(F.col('HBA1C_DATE').between(START_DATE, END_DATE)) 
                    .groupBy('NHS_NUMBER', 'ORGANISATION_CODE')
                    .agg(F.max('HBA1C_DATE').alias('HBA1C_DATE'))
    )

    # Inner join to BP_max_date to to get 
    # only nda_bp rows recorded on the max date
    bp_min_sys_dia = (nda_bp
                    .join(bp_max_date,
                        (nda_bp.NHS_NUMBER == bp_max_date.NHS_NUMBER) & 
                        (nda_bp.BP_DATE == bp_max_date.BP_DATE), how='inner')
                    .groupBy(nda_bp.NHS_NUMBER, nda_bp.ORGANISATION_CODE)
                    .agg(F.min('SYSTOLIC_VALUE').alias('SYSTOLIC_VALUE'),
                         F.min('DIASTOLIC_VALUE').alias('DIASTOLIC_VALUE'))
    )

    best_chol_value = (nda_chol
                    .join(chol_max_date,
                        (nda_chol.NHS_NUMBER == chol_max_date.NHS_NUMBER) & 
                        (nda_chol.CHOLESTEROL_DATE == chol_max_date.CHOLESTEROL_DATE), how='inner')
                    .groupBy(nda_chol.NHS_NUMBER, nda_chol.ORGANISATION_CODE)
                    .agg(F.min('CHOLESTEROL_VALUE').alias("CHOLESTEROL_VALUE"))
    )

    best_bmi_value = (nda_bmi
                    .join(bmi_max_date,
                        (nda_bmi.NHS_NUMBER == bmi_max_date.NHS_NUMBER) & 
                        (nda_bmi.BMI_DATE == bmi_max_date.BMI_DATE), how='inner')
                    .groupBy(nda_bmi.NHS_NUMBER, nda_bmi.ORGANISATION_CODE)
                    .agg(F.min('BMI_VALUE').alias("BMI_VALUE"))
    )

    best_hba1c_value = (nda_hba1c
                    .join(hba1c_max_date,
                        (nda_hba1c.NHS_NUMBER == hba1c_max_date.NHS_NUMBER) & 
                        (nda_hba1c.HBA1C_DATE == hba1c_max_date.HBA1C_DATE), how='inner')
                    .groupBy(nda_hba1c.NHS_NUMBER, nda_hba1c.ORGANISATION_CODE)
                    .agg(F.min('HBA1C_VALUE').alias("HBA1C_VALUE"))
    )

    has_statin = (nda_drug
      .filter(F.col('DATA_FIELD').like("%statin%"))
      .select('NHS_NUMBER')
      .distinct()
      .withColumn('STATIN_FLAG', F.lit(1))
    )

    # Fix for abiguous field name error.
    # Spark gets confused when you join
    # two sub-dataframes based on the same underlying dataframe,
    # nda_bp in this case. 
    # BP_max_date and BP_min_sys_dia are both based on nda_bp.

    for i in bp_min_sys_dia.columns: 
        bp_min_sys_dia = bp_min_sys_dia.withColumnRenamed(i,i)

    for i in best_chol_value.columns: 
        best_chol_value = best_chol_value.withColumnRenamed(i,i)

    for i in best_bmi_value.columns: 
        best_bmi_value = best_bmi_value.withColumnRenamed(i,i)

    for i in best_hba1c_value.columns: 
        best_hba1c_value = best_hba1c_value.withColumnRenamed(i,i)


    record_scores = (
        nda_demo
        .join(bmi_max_date, 
                (nda_demo.NHS_NUMBER        == bmi_max_date.NHS_NUMBER) & 
                (nda_demo.ORGANISATION_CODE == bmi_max_date.ORGANISATION_CODE), how='left')
        .join(bp_max_date, 
                (nda_demo.NHS_NUMBER        == bp_max_date.NHS_NUMBER) & 
                (nda_demo.ORGANISATION_CODE == bp_max_date.ORGANISATION_CODE), how='left')
        .join(chol_max_date, 
                (nda_demo.NHS_NUMBER        == chol_max_date.NHS_NUMBER) & 
                (nda_demo.ORGANISATION_CODE == chol_max_date.ORGANISATION_CODE), how='left')
        .join(hba1c_max_date, 
                (nda_demo.NHS_NUMBER        == hba1c_max_date.NHS_NUMBER) & 
                (nda_demo.ORGANISATION_CODE == hba1c_max_date.ORGANISATION_CODE), how='left')
        .join(bp_min_sys_dia,
                (nda_demo.NHS_NUMBER        == bp_min_sys_dia.NHS_NUMBER) &
                (nda_demo.ORGANISATION_CODE == bp_min_sys_dia.ORGANISATION_CODE), how='left')
        .join(best_chol_value,
                (nda_demo.NHS_NUMBER        == best_chol_value.NHS_NUMBER) &
                (nda_demo.ORGANISATION_CODE == best_chol_value.ORGANISATION_CODE), how='left')
        .join(best_bmi_value,
                (nda_demo.NHS_NUMBER        == best_bmi_value.NHS_NUMBER) &
                (nda_demo.ORGANISATION_CODE == best_bmi_value.ORGANISATION_CODE), how='left')
        .join(best_hba1c_value,
                (nda_demo.NHS_NUMBER        == best_hba1c_value.NHS_NUMBER) &
                (nda_demo.ORGANISATION_CODE == best_hba1c_value.ORGANISATION_CODE), how='left')
        .join(has_statin, (nda_demo.NHS_NUMBER == has_statin.NHS_NUMBER), how='left')
        .join(ccg_map, (nda_demo.ORGANISATION_CODE == ccg_map.ORGANISATION_CODE), how='left')
        .select(nda_demo["*"],
            bmi_max_date["BMI_DATE"],
            best_bmi_value["BMI_VALUE"],
            bp_max_date["BP_DATE"],
            has_statin["STATIN_FLAG"],
            chol_max_date["CHOLESTEROL_DATE"],
            best_chol_value["CHOLESTEROL_VALUE"],
            hba1c_max_date["HBA1C_DATE"],
            best_hba1c_value["HBA1C_VALUE"],
            ccg_map["CCG_CODE"],
            bp_min_sys_dia["SYSTOLIC_VALUE"],
            bp_min_sys_dia["DIASTOLIC_VALUE"]
        )
    )

    # This list is used below to help calculate the sum of the date flags
    # I.e. .withColumn('DATE_COUNT', sum(gp_scores[col] for col in flags_to_sum))
    flags_to_sum = ['CREATININE_DATE_FLAG', 'ALBUMIN_DATE_FLAG', 'EYE_DATE_FLAG', 'FOOT_DATE_FLAG',
                    'SMOKING_DATE_FLAG', 'ED_OFFER_DATE_FLAG', 'ED_ATTEND_DATE_FLAG', 'BMI_DATE_FLAG',
                    'BP_DATE_FLAG', 'CHOL_DATE_FLAG', 'HBA1C_DATE_FLAG'] 

    # This list is used below to help calculate the MAX_RECORD_DATE for each record
    # I.e. .withColumn('MAX_RECORD_DATE', F.greatest(*dates_list))
    dates_list = ['CREATININE_DATE', 'ALBUMIN_DATE', 'EYE_EXAM_DATE', 'FOOT_EXAM_DATE',
                    'SMOKING_DATE', 'ED_OFFER_DATE', 'ED_ATTEND_DATE', 'BMI_DATE',
                    'BP_DATE', 'CHOLESTEROL_DATE', 'HBA1C_DATE']

    # Now that we have joined all required fields, we can derive the additonal fields needed to 
    # identify the best record. Note that 'MAX_DATE_RECORD' refers to the most recent date in each
    # record - not the most recent date for each person
    record_scores = (
        record_scores
            .withColumn('CREATININE_DATE_FLAG', creatinine_date_flag)
            .withColumn('ALBUMIN_DATE_FLAG',    albumin_date_flag)
            .withColumn('EYE_DATE_FLAG',        eye_date_flag)
            .withColumn('FOOT_DATE_FLAG',       foot_date_flag)
            .withColumn('SMOKING_DATE_FLAG',    smoking_date_flag)
            .withColumn('ED_OFFER_DATE_FLAG',   ed_offer_date_flag)
            .withColumn('ED_ATTEND_DATE_FLAG',  ed_attend_date_flag)
            .withColumn('BMI_DATE_FLAG',        bmi_date_flag)
            .withColumn('BP_DATE_FLAG',         bp_date_flag)
            .withColumn('CHOL_DATE_FLAG',       cholesterol_date_flag)
            .withColumn('HBA1C_DATE_FLAG',      hba1c_date_flag)
        )

    record_scores = (
        record_scores
            .withColumn('DATE_COUNT', sum(record_scores[col] for col in flags_to_sum))
            .withColumn('MAX_DATE_RECORD', F.greatest(*dates_list))
    )

    # Now that we have the MAX_DATE_RECORD for each record, we can identify the MAX_DATE_PERSON
    max_date_by_person = (
        record_scores
            .groupBy('NHS_NUMBER')
            .agg(
                F.max('DATE_COUNT').alias('MAX_DATE_COUNT'),
                F.max('MAX_DATE_RECORD').alias('MAX_DATE_PERSON'))
    )

    # We assign a 'RECORD_SCORE' to each record according to whether that record contains the max 
    # dates for the person. If a record has both the 'MAX_DATE_COUNT' and 'MAX_DATE_PERSON' the it 
    # would score the maximum possible 110
    record_scores = (
        record_scores
            .join(max_date_by_person, on='NHS_NUMBER', how='left')
            .select(record_scores["*"], 
                    max_date_by_person['MAX_DATE_COUNT'],
                    max_date_by_person['MAX_DATE_PERSON'])
            .withColumn(
                'RECORD_SCORE', 
                    (F.when(F.col('MAX_DATE_RECORD')  == F.col('MAX_DATE_PERSON'), 100).otherwise(0)) +
                     F.when(F.col('DATE_COUNT') == F.col('MAX_DATE_COUNT'), 10).otherwise(0))
    )

    return record_scores


def identify_best_record(record_scores: DF) -> DF:
    """
    This function identifies the best record in the nda_demo data for each person. Calculating the
    best record involves lots of steps so it is better to encapsulate this logic. The function
    should return a dataframe with one row per person and the ORGANISATION_CODE of the best record.

    Args:
        The 'record_scores' table. This table is record-grain and includes the 'RECORD_SCORE' field.
        
    Returns:
        A dataframe containing the columns NHS_NUMBER | ORGANISATION_CODE

    Example:
        best_record = identify_best_record(record_scores=record_scores)
    """

    max_scores_per_person = (
        record_scores
        .groupBy('NHS_NUMBER')
        .agg(F.max('RECORD_SCORE').alias('MAX_RECORD_SCORE'))
    )

    # Even once we have assigned a 'RECORD_SCORE', it is still possible to find situations where two
    # records would tie - i.e., a person could have two GP records with a score of 110. To resolve
    # tie-breaks, we select the GP practice with the lowest ORGANISATION_CODE
    best_record = (
        record_scores
            .join(max_scores_per_person, on='NHS_NUMBER', how='left')
            .select(record_scores["*"], 
                    max_scores_per_person['MAX_RECORD_SCORE'])
            .filter(F.col('RECORD_SCORE') == F.col('MAX_RECORD_SCORE'))
            .groupBy('NHS_NUMBER')
            .agg(F.min('ORGANISATION_CODE').alias('BEST_RECORD'))
    )

    return best_record


def create_golden_record_table(record_scores: DF, best_record: DF) -> DF:
    """
    The golden record should return the best values for each person. The basis for the golden record
    therefore is the list of unique people in the data
    
    The record_scores table already contains many of the fields that will end up in the golden
    record. The problem is that the record_scores table also contains rows that should not be included
    in the golden record. 
    
    In order to cut down the record_scores table to only those rows we are interested in, we can use 
    an inner join with the 'best_record' table.
    
    Args:
        The 'record_scores' table. This table is record-grain and includes the 'RECORD_SCORE' field.
        The 'best_record' table. This table is person-grain and includes the 'BEST_RECORD' field
        
    Returns:
        A dataframe with all of the columns from record_scores but retaining only those rows
        identified as 'BEST_RECORD'

    Example:
        golden_record = create_golden_record_table(
                            record_scores=record_scores, 
                            best_record=best_record)
    """
    golden_record = (
        record_scores
            .join(best_record, 
                    (record_scores.NHS_NUMBER        == best_record.NHS_NUMBER) & 
                    (record_scores.ORGANISATION_CODE == best_record.BEST_RECORD), how='inner')
            .select(record_scores['*'])
    )

    return golden_record


def enrich_golden_record(golden_record: DF, hes_diabetes: DF, imd_scores: DF) -> DF:
    """
    This function takes the bare golden record table produced by create_golden_record_table() and 
    derives additional fields that will be used for calculations. 

    Args:
        The golden record table
        
    Returns:
        The golden record table with additional fields

    Example:
        golden_record = enrich_golden_record(golden_record=golden_record)
    """
    # We use the publication end date to set AGE_UPPER boundary
    AGE_LOWER = F.to_date(F.lit('1907-01-01'))
    AGE_UPPER = F.to_date(F.lit(params['publication_end_date'])) 

    # Now that we have a person-level record we can join on any additional fields we need.

    # The HES_Diabetes_comps_XXX table defines whether a person gets CVD_admission
    golden_record = (
        golden_record
            .join(hes_diabetes, (golden_record.NHS_NUMBER == hes_diabetes.NHS_NUMBER), how='left')
            .join(imd_scores, (golden_record.LSOA == imd_scores.LSOA) , how='left')
            .select(golden_record['*'], 
                    hes_diabetes['CVD_ADMISSION'],
                    imd_scores['IMD_QUINTILE'])
    )

    golden_record = (
        golden_record
            .withColumn('REGISTRATIONS', registrations_denominator)
            .withColumn('BP_NUM', bp_numerator)
            .withColumn('BP_DENOM', bp_denominator)
            .withColumn('BP_140_80_TT_NUM', bp_140_80_tt_num)
            .withColumn('BP_TT_DENOM', bp_tt_denom)
            .withColumn('AGE_CLEANED', age_cleaned)
            .withColumn('CHOLESTEROL_NUM', cholesterol_numerator)
            .withColumn('CHOLESTEROL_DENOM', cholesterol_denominator)
            .withColumn('DIABETES_TYPE_PUB', diabetes_type_publication)
            .withColumn('AGE_GROUP', age_group_publication)
            .withColumn('HBA1C_NUM', hba1c_numerator)
            .withColumn('HBA1C_<=_48_TT_NUM', hba1c_less_or_equal_48_tt_numerator)
            .withColumn('HBA1C_<=_53_TT_NUM', hba1c_less_or_equal_53_tt_numerator)
            .withColumn('HBA1C_<=_58_TT_NUM', hba1c_less_or_equal_58_tt_numerator)
            .withColumn('HBA1C_<=_75_TT_NUM', hba1c_less_or_equal_75_tt_numerator)
            .withColumn('HBA1C_<=_86_TT_NUM', hba1c_less_or_equal_86_tt_numerator)
            .withColumn('HBA1C_TT_DENOM', hba1c_tt_denominator)
            .withColumn('BMI_NUM', bmi_numerator)
            .withColumn('BMI_DENOM', bmi_denominator)
            .withColumn('DIABETES_TYPE_PUB', diabetes_type_publication)
            .withColumn('GENDER', gender)
            .withColumn('NO_CVD_ON_STATIN_40_TO_80', no_cvd_on_statins_40_to_80)
            .withColumn('NO_CVD_40_TO_80', no_cvd_40_to_80)
            .withColumn('CVD_ADMISSION_STATIN_FLAG', cvd_admission_statin_flag)
            .withColumn('WITH_CVD_ADMISSION', with_cvd_admission)
            .withColumn('COMBINED_NUM', combined_numerator)
            .withColumn('COMBINED_DENOM', combined_denominator)
            .withColumn('ALL_8_CP', all_8_cp)
            .withColumn('SMOKING_NUM', smoking_numerator)
            .withColumn('SMOKING_DENOM', smoking_denominator)
            .withColumn('ALBUMIN_NUM', albumin_numerator)
            .withColumn('ALBUMIN_DENOM', albumin_denominator)
            .withColumn('CREATININE_NUM', creatinine_numerator)
            .withColumn('CREATININE_DENOM', creatinine_denominator)
            .withColumn('CHOLESTEROL_<5', cholesterol_less_than_5)
            .withColumn('CHOLESTEROL_TT_DENOM', cholesterol_TT_denominator)
            .withColumn('FOOT_NUM', foot_num)
            .withColumn('FOOT_DENOM', foot_denom)
            .withColumn('ALL_3_TT_NUM', all_3_tt_num)
            .withColumn('ALL_3_TT_DENOM', all_3_tt_denom)
            .withColumn('TT_3_NEW_NUM', tt_3_new_num)
            .withColumn('TT_3_NEW_DENOM', tt_3_new_denom)
            .withColumn('ETHNICITY_GROUP', ethnicity_group)
            .withColumn('NEWLY_DIAGNOSED', newly_diagnosed)
            .withColumn('ND_OFFERED_12_M', offered_12_month)
            .withColumn('ND_ATTENDED_12_M', attended_12_month)
            .withColumn('ND_OFFERED_EVER', offered_ever)
            .withColumn('ND_ATTENDED_EVER', attended_ever)
    )

    return golden_record


def produce_aggregates(golden_record: DF) -> DF:
    """
    This function takes the golden records and aggregates each of the breakdowns into its own
    dataframe. Those dataframes are then unioned and returned as the final tidy table.

    Args:
        The golden record table
        
    Returns:
        A DataFrame containing all of the aggregate data in tidy format

    Example:
        final_table = produce_aggregates(golden_record)
    """
    
    # Use this list to specify the order of the columns to be returned
    # !!!! As we add more fields we need to expand this list!!!!
    column_order = [
        'CCG_CODE', 'ORGANISATION_CODE',
        'BREAKDOWN_FIELD', 'BREAKDOWN_VALUE_1', 'BREAKDOWN_VALUE_2', 'BREAKDOWN_VALUE_3',
        'REGISTRATIONS',
        'CHOLESTEROL_NUM', 'CHOLESTEROL_DENOM', 'CHOLESTEROL_PCT',
        'BMI_NUM', 'BMI_DENOM', 'BMI_PCT',
        'SMOKING_NUM', 'SMOKING_DENOM', 'SMOKING_PCT',
        'CHOLESTEROL_<5_NUM', 'CHOLESTEROL_<5_DENOM', 'CHOLESTEROL_<5_PCT',
        'FOOT_NUM', 'FOOT_DENOM', 'FOOT_PCT',
        'CREATININE_NUM', 'CREATININE_DENOM', 'CREATININE_PCT',
        'ALBUMIN_NUM', 'ALBUMIN_DENOM', 'ALBUMIN_PCT',
        'HBA1C_NUM', 'HBA1C_DENOM', 'HBA1C_PCT',
        'HBA1C_<=_48_TT_NUM', 'HBA1C_<=_48_TT_DENOM', 'HBA1C_<=_48_TT_PCT',
        'HBA1C_<=_53_TT_NUM', 'HBA1C_<=_53_TT_DENOM', 'HBA1C_<=_53_TT_PCT',
        'HBA1C_<=_58_TT_NUM', 'HBA1C_<=_58_TT_DENOM', 'HBA1C_<=_58_TT_PCT',
        'HBA1C_<=_75_TT_NUM', 'HBA1C_<=_75_TT_DENOM', 'HBA1C_<=_75_TT_PCT',
        'HBA1C_<=_86_TT_NUM', 'HBA1C_<=_86_TT_DENOM', 'HBA1C_<=_86_TT_PCT',
        'NO_CVD_ON_STATIN_NUM_40_TO_80', 'NO_CVD_DENOM_40_TO_80', 'NO_CVD_ON_STATIN_40_TO_80_PCT',
        'SEC_PREVENT_NUM', 'SEC_PREVENT_DENOM', 'WITH_CVD_ON_STATINS_PCT',
        'COMBINED_NUM', 'COMBINED_DENOM', 'COMBINED_PCT',
        'BP_NUM', 'BP_DENOM', 'BP_PCT',
        'BP_140_80_NUM', 'BP_140_80_DENOM', 'BP_140_80_PCT',
        'ALL_8_NUM', 'ALL_8_DENOM', 'ALL_8_PCT',
        'ALL_3_TT_NUM', 'ALL_3_TT_DENOM', 'ALL_3_TT_PCT',
        'TT_3_NEW_NUM', 'TT_3_NEW_DENOM', 'TT_3_NEW_PCT',
        'NEWLY_DIAGNOSED', 'ND_OFFERED_12_M', 'ND_ATTENDED_12_M', 'ND_OFFERED_EVER', 'ND_ATTENDED_EVER'
    ]

    # All England breakdown
    try:
        all_agg  = (
            golden_record
                .agg(*aggregate_fields)
                .withColumn('CCG_CODE', F.lit('England'))
                .withColumn('ORGANISATION_CODE', F.lit('England'))
                .withColumn('BREAKDOWN_FIELD', F.lit('England'))
                .withColumn('BREAKDOWN_VALUE_1', F.lit('England'))
                .withColumn('BREAKDOWN_VALUE_2', F.lit('England'))
                .withColumn('BREAKDOWN_VALUE_3', F.lit('England'))
                .select(*column_order)
        )
    except pyspark.sql.utils.AnalysisException as e:
        logger.error(f"This is a PySpark error. Please check your variable: {str(e).split(';')[0]}")
        sys.exit(1)

    diabetes_type_agg  = (
        golden_record
            .groupBy('DIABETES_TYPE_PUB')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.lit('England'))
            .withColumn('ORGANISATION_CODE', F.lit('England'))
            .withColumn('BREAKDOWN_FIELD', F.lit('DiabetesType'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.lit(None))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    diabetes_type_age_agg = (
        golden_record
            .groupBy('DIABETES_TYPE_PUB', 'AGE_GROUP')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.lit('England'))
            .withColumn('ORGANISATION_CODE', F.lit('England'))
            .withColumn('BREAKDOWN_FIELD', F.lit('DiabetesType_Age'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('AGE_GROUP'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    diabetes_type_ethnicity_agg  = (
        golden_record
            .groupBy('DIABETES_TYPE_PUB', 'ETHNICITY_GROUP')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.lit('England'))
            .withColumn('ORGANISATION_CODE', F.lit('England'))
            .withColumn('BREAKDOWN_FIELD', F.lit('DiabetesType_Ethnicity'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('ETHNICITY_GROUP'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )
    diabetes_type_deprivation_imd_agg  = (
        golden_record
            .groupBy('DIABETES_TYPE_PUB', 'IMD_QUINTILE')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.lit('England'))
            .withColumn('ORGANISATION_CODE', F.lit('England'))
            .withColumn('BREAKDOWN_FIELD', F.lit('DiabetesType_Deprivation_Imd'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('IMD_QUINTILE'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    diabetes_type_deprivation_imd_agg  = (
        golden_record
            .groupBy('DIABETES_TYPE_PUB', 'IMD_QUINTILE')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.lit('England'))
            .withColumn('ORGANISATION_CODE', F.lit('England'))
            .withColumn('BREAKDOWN_FIELD', F.lit('DiabetesType_Deprivation_Imd'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('IMD_QUINTILE'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    diabetes_type_gender_agg  = (
        golden_record
            .groupBy('DIABETES_TYPE_PUB', 'GENDER')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.lit('England'))
            .withColumn('ORGANISATION_CODE', F.lit('England'))
            .withColumn('BREAKDOWN_FIELD', F.lit('DiabetesType_Gender'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('GENDER'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_code_diabetes_type_agg  = (
        golden_record
            .groupBy('CCG_CODE', 'DIABETES_TYPE_PUB')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('CCG_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_DiabetesType'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.lit(None))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_organisation_code_diabetes_type_agg  = (
        golden_record
            .groupBy('ORGANISATION_CODE', 'CCG_CODE', 'DIABETES_TYPE_PUB')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('ORGANISATION_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_ORGANISATION_DiabetesType'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.lit(None))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_code_diabetes_type_age_agg  = (
        golden_record
            .groupBy('CCG_CODE', 'DIABETES_TYPE_PUB', 'AGE_GROUP')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('CCG_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_DiabetesType_Age'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('AGE_GROUP'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_code_diabetes_type_ethnicity_agg = (
        golden_record
            .groupBy('CCG_CODE', 'DIABETES_TYPE_PUB', 'ETHNICITY_GROUP')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('CCG_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_DiabetesType_Ethnicity'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('ETHNICITY_GROUP'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_code_diabetes_type_deprivation_imd_agg = (
        golden_record
            .groupBy('CCG_CODE', 'DIABETES_TYPE_PUB', 'IMD_QUINTILE')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('CCG_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_DiabetesType_Deprivation_Imd'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('IMD_QUINTILE'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_code_diabetes_type_gender_agg  = (
        golden_record
            .groupBy('CCG_CODE', 'DIABETES_TYPE_PUB', 'GENDER')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('CCG_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_DiabetesType_Gender'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('GENDER'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_organisation_code_diabetes_type_age_agg  = (
        golden_record
            .groupBy('ORGANISATION_CODE', 'CCG_CODE', 'DIABETES_TYPE_PUB', 'AGE_GROUP')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('ORGANISATION_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_ORGANISATION_DiabetesType_Age'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('AGE_GROUP'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_organisation_code_diabetes_type_gender_agg  = (
        golden_record
            .groupBy('ORGANISATION_CODE', 'CCG_CODE', 'DIABETES_TYPE_PUB', 'GENDER')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('ORGANISATION_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_ORGANISATION_DiabetesType_Gender'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('GENDER'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    ccg_organisation_code_diabetes_type_ethnicity_agg  = (
        golden_record
            .groupBy('ORGANISATION_CODE', 'CCG_CODE', 'DIABETES_TYPE_PUB', 'ETHNICITY_GROUP')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('ORGANISATION_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_ORGANISATION_DiabetesType_Ethnicity'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('ETHNICITY_GROUP'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )
    ccg_organisation_code_diabetes_type_deprivation_imd_agg  = (
        golden_record
            .groupBy('ORGANISATION_CODE', 'CCG_CODE', 'DIABETES_TYPE_PUB', 'IMD_QUINTILE')
            .agg(*aggregate_fields)
            .withColumn('CCG_CODE', F.col('CCG_CODE'))
            .withColumn('ORGANISATION_CODE', F.col('ORGANISATION_CODE'))
            .withColumn('BREAKDOWN_FIELD', F.lit('CCG_ORGANISATION_DiabetesType_Deprivation_Imd'))
            .withColumn('BREAKDOWN_VALUE_1', F.col('DIABETES_TYPE_PUB'))
            .withColumn('BREAKDOWN_VALUE_2', F.col('IMD_QUINTILE'))
            .withColumn('BREAKDOWN_VALUE_3', F.lit(None))
            .select(*column_order)
    )

    final_table = (
        all_agg
            .union(diabetes_type_agg)
            .union(diabetes_type_age_agg)
            .union(diabetes_type_gender_agg)
            .union(diabetes_type_ethnicity_agg)
            .union(diabetes_type_deprivation_imd_agg)
            .union(ccg_code_diabetes_type_agg)
            .union(ccg_code_diabetes_type_age_agg)
            .union(ccg_code_diabetes_type_gender_agg)
            .union(ccg_code_diabetes_type_ethnicity_agg)
            .union(ccg_code_diabetes_type_deprivation_imd_agg)
            .union(ccg_organisation_code_diabetes_type_agg)
            .union(ccg_organisation_code_diabetes_type_age_agg)
            .union(ccg_organisation_code_diabetes_type_gender_agg)
            .union(ccg_organisation_code_diabetes_type_ethnicity_agg)
            .union(ccg_organisation_code_diabetes_type_deprivation_imd_agg)
    )

    return final_table