"""
This module holds definitions of fields - i.e. how to derive new columns.

These definitions can be passed into the .withColumn() method to create new fields. E.g.,
    df = (
        df.withColumn('CHOLESTEROL_NUM', cholesterol_numerator)
    )

These definitions should also be tested using unit tests and dummy data.
"""
from pyspark.sql.types import NullType
import toml
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import dataframe as DF
from diabetes_code.params import params


# The lower age range stays constant but the upper age range is defined by the publication end date
AGE_LOWER = F.to_date(F.lit('1907-01-01'))
AGE_UPPER = F.to_date(F.lit(params['publication_end_date']))

START_DATE = F.to_date(F.lit(params['publication_start_date']))
END_DATE = F.to_date(F.lit(params['publication_end_date']))

NEWLY_DIAGNOSED_YEAR = int(params['newly_diagnosed_year'])

age_cleaned = (
    F.when(
            (F.col('BIRTH_DATE').between(AGE_LOWER, AGE_UPPER)) &
            (F.col('AGE') >= 0),
            (F.col('AGE'))
            )
        .when(
            (F.col('BIRTH_DATE').between(AGE_LOWER, AGE_UPPER)) &
            ((F.col('AGE') == -1) | (F.col('AGE') == -2)),
            0)
        .otherwise(None)
)

all_3_tt_num = (
    F.when(
        (F.col('AGE') >= 12) &
        (F.col('BP_140_80_TT_NUM') == 1) &
        (F.col('HBA1C_<=_58_TT_NUM') == 1) &
        (F.col('CHOLESTEROL_<5') == 1),
        1)
    .when(
        (F.col('AGE') < 12) &
        (F.col('HBA1C_<=_58_TT_NUM') == 1),
        1)
    .otherwise(0)
)

all_3_tt_denom = (
    F.when(
        (F.col('AGE') >= 12) &
        (F.col('BP_TT_DENOM') == 1) & 
        (F.col('HBA1C_TT_DENOM') == 1) & 
        (F.col('CHOLESTEROL_TT_DENOM') == 1),
        1)
    .when(
        (F.col('AGE') < 12) &
        (F.col('HBA1C_TT_DENOM') == 1),
        1)
    .otherwise(0)
)

tt_3_new_num = (
    F.when(
        (F.col('AGE') < 12) &
        (F.col('HBA1C_<=_58_TT_NUM') == 1),
        1)
    .when(
        ((F.col('AGE').between(12, 39)) | (F.col('AGE') > 80)) &
        ((F.col('CVD_ADMISSION') == 1) | (F.col('IHD_VALUE') == 1)) &
        (F.col('HBA1C_<=_58_TT_NUM') == 1) &
        (F.col('BP_140_80_TT_NUM') == 1) &
        (F.col('STATIN_FLAG') == 1),
        1)
    .when(
        ((F.col('AGE').between(12, 39)) | (F.col('AGE') > 80)) &
        ((F.col('CVD_ADMISSION').isNull()) & (F.col('IHD_VALUE').isNull())) &
        (F.col('HBA1C_<=_58_TT_NUM') == 1) &
        (F.col('BP_140_80_TT_NUM') == 1),
        1)
    .when(
        (F.col('AGE').between(40, 80)) &
        (F.col('HBA1C_<=_58_TT_NUM') == 1) & 
        (F.col('BP_140_80_TT_NUM') == 1) &
        (F.col('STATIN_FLAG') == 1),
        1)
    .otherwise(0)
)

tt_3_new_denom = (
    F.when(
        (F.col('AGE') >= 12) &
        (F.col('HBA1C_TT_DENOM') == 1) &
        (F.col('BP_TT_DENOM') == 1),
        1)
    .when(
        (F.col('AGE') < 12) &
        (F.col('HBA1C_TT_DENOM') == 1),
        1)
    .otherwise(0)
)

creatinine_date_flag = (
    F.when(F.col("CREATININE_DATE").isNotNull(), 1).otherwise(0)
)

albumin_date_flag = (
    F.when(F.col("ALBUMIN_DATE").isNotNull(), 1).otherwise(0)
)

eye_date_flag = (
    F.when(F.col("EYE_EXAM_DATE").isNotNull(), 1).otherwise(0)
)

foot_date_flag = (
    F.when(F.col("FOOT_EXAM_DATE").isNotNull(), 1).otherwise(0)
)

smoking_date_flag = (
    F.when(F.col("SMOKING_DATE").between(START_DATE, END_DATE), 1)
    .when((F.col("BIRTH_DATE").isNotNull()) & 
    (F.col("SMOKING_DATE").between(AGE_LOWER, AGE_UPPER)) &
    (F.col("SMOKING_VALUE") == 4) & 
    (F.col("AGE") >= 25) &
    (F.floor((F.months_between(F.col('SMOKING_DATE'), F.col('BIRTH_DATE'))/12)) > 25), 1)
    .otherwise(0)
)

ed_offer_date_flag = (
    F.when(F.col("ED_OFFER_DATE").isNotNull(), 1).otherwise(0)
)

ed_attend_date_flag = (
    F.when(F.col("ED_ATTEND_DATE").isNotNull(), 1).otherwise(0)
)

bmi_date_flag = (
    F.when(F.col("BMI_DATE").isNotNull(), 1).otherwise(0)
)

bp_date_flag = (
    F.when(F.col("BP_DATE").isNotNull(), 1).otherwise(0)
)

cholesterol_date_flag = (
    F.when(F.col("CHOLESTEROL_DATE").isNotNull(), 1).otherwise(0)
)

hba1c_date_flag = (
    F.when(F.col("HBA1C_DATE").isNotNull(), 1).otherwise(0)
)

all_8_cp = (
    F.when(
        (
            (F.col('AGE') >= 12) &
            (F.col('HBA1C_DATE_FLAG') == 1) &
            (F.col('BP_DATE_FLAG') == 1) &
            (F.col('CHOL_DATE_FLAG') == 1) &
            (F.col('CREATININE_DATE_FLAG') == 1) &
            (F.col('FOOT_DATE_FLAG') == 1) &
            (F.col('ALBUMIN_DATE_FLAG') == 1) &
            (F.col('BMI_DATE_FLAG') == 1) &
            (
                (
                    F.col('SMOKING_DATE').between(START_DATE, END_DATE)
                )   
                |
                (   
                    (F.col('SMOKING_DATE').isNotNull()) &
                    (F.col('SMOKING_VALUE') == 4) &
                    (F.col('AGE') >= 25) &
                    (F.floor((F.months_between(F.col('SMOKING_DATE'), F.col('BIRTH_DATE'))/12)) > 25)
                )   
                
            )
        )
        |
        (
            (F.col('AGE') < 12) & 
            (F.col('HBA1C_DATE_FLAG') == 1)
        )
        , 1).otherwise(0)
)

cholesterol_numerator = (
    F.when((F.col('AGE') >= 12) & (F.col('CHOL_DATE_FLAG') == 1), 1).otherwise(0)
)

cholesterol_denominator = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
)

hba1c_numerator = (
    F.when((F.col('HBA1C_DATE_FLAG') == 1), 1).otherwise(0)
)

hba1c_less_or_equal_48_tt_numerator = (
    F.when((F.col('HBA1C_DATE_FLAG') == 1) & (F.col('HBA1C_VALUE') <= 48), 1).otherwise(0)
)

hba1c_less_or_equal_53_tt_numerator = (
    F.when((F.col('HBA1C_DATE_FLAG') == 1) & (F.col('HBA1C_VALUE') <= 53), 1).otherwise(0)
)

hba1c_less_or_equal_58_tt_numerator = (
    F.when((F.col('HBA1C_DATE_FLAG') == 1) & (F.col('HBA1C_VALUE') <= 58), 1).otherwise(0)
)

hba1c_less_or_equal_75_tt_numerator = (
    F.when((F.col('HBA1C_DATE_FLAG') == 1) & (F.col('HBA1C_VALUE') <= 75), 1).otherwise(0)
)

hba1c_less_or_equal_86_tt_numerator = (
    F.when((F.col('HBA1C_DATE_FLAG') == 1) & (F.col('HBA1C_VALUE') <= 86), 1).otherwise(0)
)

hba1c_tt_denominator = (
    F.when(
        (F.col('HBA1C_VALUE').isNotNull()) &
        (F.col('HBA1C_DATE_FLAG') == 1), 
        1).otherwise(0)
)

albumin_numerator = (
    F.when((F.col('AGE') >= 12) & (F.col('ALBUMIN_DATE_FLAG') == 1), 1).otherwise(0)
)

albumin_denominator = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
)

creatinine_numerator = (
    F.when((F.col('AGE') >= 12) & (F.col("CREATININE_DATE_FLAG") == 1), 1).otherwise(0)
)

creatinine_denominator = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
)

cholesterol_less_than_5 = (
    F.when(
        (F.col('AGE') >= 12) &
        (F.col("CHOLESTEROL_DATE").isNotNull()) &
        (F.col("CHOLESTEROL_VALUE") < 5)
        , 1).otherwise(0)
)

cholesterol_TT_denominator = (
    F.when(
        (F.col('AGE') >= 12) & 
        (F.col("CHOLESTEROL_VALUE").isNotNull()) &
        (F.col("CHOLESTEROL_DATE").isNotNull()), 
        1).otherwise(0)
)

smoking_numerator = (
    F.when((F.col('AGE') >= 12) & (F.col('SMOKING_DATE_FLAG') == 1), 1).otherwise(0)
)

smoking_denominator = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
)

bmi_numerator = (
    F.when(
        (F.col('AGE') >= 12) & (F.col('BMI_DATE_FLAG') == 1), 1).otherwise(0)
)

bmi_denominator = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
)

foot_num = (
    F.when((F.col('AGE') >= 12) & (F.col("FOOT_DATE_FLAG") == 1), 1).otherwise(0)
)

foot_denom = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
)

no_cvd_on_statins_40_to_80 = (
    F.when((F.col('CVD_ADMISSION').isNull()) &
    (F.col('IHD_VALUE').isNull()) &
    (F.col('STATIN_FLAG') == 1) &
    (F.col('AGE') >= 40) &
    (F.col('AGE') <= 80), 1).otherwise(0)
)

no_cvd_40_to_80 = (
    F.when((F.col('CVD_ADMISSION').isNull()) &
    (F.col('IHD_VALUE').isNull()) &
    (F.col('AGE') >= 40) & (F.col('AGE') <= 80), 1).otherwise(0)
)

cvd_admission_statin_flag = (
    F.when(
        ((F.col('CVD_ADMISSION') == 1) | (F.col('IHD_VALUE') == 1)) &
        (F.col('STATIN_FLAG') == 1), 1).otherwise(0)
)

with_cvd_admission = (
    F.when((F.col('CVD_ADMISSION') == 1) | (F.col('IHD_VALUE') == 1), 1).otherwise(0)
)

combined_numerator = (
    F.when(((F.col('NO_CVD_ON_STATIN_40_TO_80') == 1) | (F.col('CVD_ADMISSION_STATIN_FLAG') == 1)), 1).otherwise(0)
)

combined_denominator = (
    F.when (((F.col('AGE') >= 40) & (F.col('AGE') <= 80)) |
    (F.col('CVD_ADMISSION') == 1) |
    (F.col('IHD_VALUE') == 1), 1).otherwise(0)
)

bp_numerator = (
    F.when((F.col('BP_DATE_FLAG') == 1) &
    (F.col('AGE') >= 12), 1).otherwise(0)
)

bp_denominator = (
    F.when(F.col('AGE') >= 12, 1).otherwise(0)
)

bp_140_80_tt_num = (
    F.when(
        (F.col('BP_DATE_FLAG') == 1) &
        (F.col('SYSTOLIC_VALUE') <= 140) &
        (F.col('DIASTOLIC_VALUE') <= 80) &
        (F.col('AGE') >= 12),
    1).otherwise(0)
)

bp_tt_denom = (
    F.when(
        (F.col('BP_DATE_FLAG') == 1) &
        (F.col('SYSTOLIC_VALUE').isNotNull()) &
        (F.col('DIASTOLIC_VALUE').isNotNull()) &
        (F.col('AGE') >= 12),
    1).otherwise(0)
)

registrations_denominator = (
    F.when(F.col('NHS_NUMBER').isNotNull(), 1).otherwise(0)
)

diabetes_type_publication = (
    F.when(F.col('DIABETES_TYPE') == 1, 'TYPE 1')
     .otherwise('TYPE 2 AND OTHERS')
)

diabetes_type_recoded = (
    F.when(F.col('DIABETES_TYPE') == 1, '01')
    .when(F.col('DIABETES_TYPE') == 2, '02')
    .when(F.col('DIABETES_TYPE') == 6, '06')
    .when(F.col('DIABETES_TYPE') == 8, '08')
    .when(F.col('DIABETES_TYPE') == 99, '99')
    .otherwise(F.lit(None))
)

diabetes_class = (
    F.when(F.col("DIABETES_TYPE") == 1, "TYPE 1").otherwise("OTHER")
)

age_group_publication = (
    F.when(F.col('AGE') < 40, 'Aged under 40')
    .when((F.col('AGE') >= 40) & (F.col('AGE') <= 64), 'Aged 40 to 64')
    .when((F.col('AGE') >= 65) & (F.col('AGE') <= 79), 'Aged 65 to 79')
    .when(F.col('AGE') >= 80, 'Aged 80 and over')
    .otherwise('Age unknown')
)

gender = (
    F.when(F.col("SEX") == 1, "Male")
    .when(F.col("SEX") == 2, 'Female')
    .otherwise('Unknown sex')
)

org_code_to_country = (
    F.when(F.col('ORGANISATION_CODE').like('7%'), 'Wales')
    .when(F.col('ORGANISATION_CODE').like('W%'), 'Wales')
    .when(F.col('ORGANISATION_CODE').isNull(), F.lit(None))
    .otherwise('England')
)

ethnicity_group = (
    F.when(F.col('ETHNICITY').isin('A', 'B', 'C', 'T'), 'White')
    .when((F.col('ETHNICITY') == 'Z') | (F.col('ETHNICITY').isNull()), 'Unknown')
    .otherwise(F.lit('Minority'))
)

deprivation_imd_group = (
    F.when(F.col('COMBINED_QUINTILE') == 1, 'IMD most deprived')
    .when(F.col('COMBINED_QUINTILE') == 2, 'IMD 2nd most deprived')
    .when(F.col('COMBINED_QUINTILE') == 3, 'IMD 3rd most deprived')
    .when(F.col('COMBINED_QUINTILE') == 4, 'IMD 2nd least deprived')
    .when(F.col('COMBINED_QUINTILE') == 5, 'IMD least deprived')
    .otherwise(F.lit('IMD unknown'))
)


newly_diagnosed = (
    F.when(
        (F.col('DIAGNOSIS_YEAR') == NEWLY_DIAGNOSED_YEAR) & 
        (F.col('NHS_NUMBER').isNotNull()),
        1).otherwise(0)
)

offered_12_month = (
    F.when( 
        (F.col('NEWLY_DIAGNOSED') == 1) &
        (F.datediff('ED_OFFER_DATE','DIAGNOSIS_DATE') < 366), 
        1).otherwise(0)
)

attended_12_month = (
    F.when(
        (F.col('NEWLY_DIAGNOSED') == 1) &
        (F.datediff('ED_ATTEND_DATE','DIAGNOSIS_DATE') < 366),
        1).otherwise(0)
)

offered_ever = (
    F.when(
        (F.col('NEWLY_DIAGNOSED') == 1) &
        (F.datediff('ED_OFFER_DATE','DIAGNOSIS_DATE') < 100000),
        1).otherwise(0)
)

attended_ever = (
    F.when(
        (F.col('NEWLY_DIAGNOSED') == 1) &
        (F.datediff('ED_ATTEND_DATE','DIAGNOSIS_DATE') < 100000),
        1).otherwise(0)
)


# This is the list of columns we want returned in aggregate form - i.e. the columns for the
# output data. We can pass a list of fields to be aggregated.
# E.g. analysis_table.agg(*aggregate_fields)
aggregate_fields = (
    F.sum('REGISTRATIONS').alias('REGISTRATIONS'),
    F.sum('CHOLESTEROL_NUM').alias('CHOLESTEROL_NUM'), # CHOLESTEROL
    F.sum('CHOLESTEROL_DENOM').alias('CHOLESTEROL_DENOM'),
    F.round(F.sum('CHOLESTEROL_NUM') * 100 /F.sum('CHOLESTEROL_DENOM'), 2).alias('CHOLESTEROL_PCT'),
    F.sum('ALL_8_CP').alias('ALL_8_NUM'), 
    F.count('NHS_NUMBER').alias('ALL_8_DENOM'),
    F.round(F.sum('ALL_8_CP') * 100 /F.count('NHS_NUMBER'), 2).alias('ALL_8_PCT'),
    F.sum('ALL_3_TT_NUM').alias('ALL_3_TT_NUM'), 
    F.sum('ALL_3_TT_DENOM').alias('ALL_3_TT_DENOM'),
    F.round(F.sum('ALL_3_TT_NUM') * 100 / F.sum('ALL_3_TT_DENOM'), 2).alias('ALL_3_TT_PCT'),
    F.sum('TT_3_NEW_NUM').alias('TT_3_NEW_NUM'), 
    F.sum('TT_3_NEW_DENOM').alias('TT_3_NEW_DENOM'),
    F.round(F.sum('TT_3_NEW_NUM') * 100 / F.sum('TT_3_NEW_DENOM'), 2).alias('TT_3_NEW_PCT'),
    F.sum('HBA1C_NUM').alias('HBA1C_NUM'), 
    F.count('NHS_NUMBER').alias('HBA1C_DENOM'),
    F.round(F.sum('HBA1C_NUM') * 100 /F.count('NHS_NUMBER'), 2).alias('HBA1C_PCT'),
    F.sum('HBA1C_<=_48_TT_NUM').alias('HBA1C_<=_48_TT_NUM'),
    F.sum('HBA1C_TT_DENOM').alias('HBA1C_<=_48_TT_DENOM'),
    F.round(F.sum('HBA1C_<=_48_TT_NUM') * 100 /F.sum('HBA1C_TT_DENOM'), 2).alias('HBA1C_<=_48_TT_PCT'),
    F.sum('HBA1C_<=_53_TT_NUM').alias('HBA1C_<=_53_TT_NUM'),
    F.sum('HBA1C_TT_DENOM').alias('HBA1C_<=_53_TT_DENOM'),
    F.round(F.sum('HBA1C_<=_53_TT_NUM') * 100 /F.sum('HBA1C_TT_DENOM'), 2).alias('HBA1C_<=_53_TT_PCT'),
    F.sum('HBA1C_<=_58_TT_NUM').alias('HBA1C_<=_58_TT_NUM'),
    F.sum('HBA1C_TT_DENOM').alias('HBA1C_<=_58_TT_DENOM'),
    F.round(F.sum('HBA1C_<=_58_TT_NUM') * 100 /F.sum('HBA1C_TT_DENOM'), 2).alias('HBA1C_<=_58_TT_PCT'),
    F.sum('HBA1C_<=_75_TT_NUM').alias('HBA1C_<=_75_TT_NUM'),
    F.sum('HBA1C_TT_DENOM').alias('HBA1C_<=_75_TT_DENOM'),
    F.round(F.sum('HBA1C_<=_75_TT_NUM') * 100 /F.sum('HBA1C_TT_DENOM'), 2).alias('HBA1C_<=_75_TT_PCT'),
    F.sum('HBA1C_<=_86_TT_NUM').alias('HBA1C_<=_86_TT_NUM'),
    F.sum('HBA1C_TT_DENOM').alias('HBA1C_<=_86_TT_DENOM'),
    F.round(F.sum('HBA1C_<=_86_TT_NUM') * 100 /F.sum('HBA1C_TT_DENOM'), 2).alias('HBA1C_<=_86_TT_PCT'),
    F.sum('ALBUMIN_NUM').alias('ALBUMIN_NUM'), 
    F.sum('ALBUMIN_DENOM').alias('ALBUMIN_DENOM'),
    F.round(F.sum('ALBUMIN_NUM') * 100 /F.sum('ALBUMIN_DENOM'), 2).alias('ALBUMIN_PCT'),
    F.sum('CREATININE_NUM').alias('CREATININE_NUM'),
    F.sum('CREATININE_DENOM').alias('CREATININE_DENOM'),
    F.round(F.sum('CREATININE_NUM') * 100 /F.sum('CREATININE_DENOM'), 2).alias('CREATININE_PCT'),
    F.sum('CHOLESTEROL_<5').alias('CHOLESTEROL_<5_NUM'), 
    F.sum('CHOLESTEROL_TT_DENOM').alias('CHOLESTEROL_<5_DENOM'),
    F.round(F.sum('CHOLESTEROL_<5') * 100 /F.sum('CHOLESTEROL_TT_DENOM'), 2).alias('CHOLESTEROL_<5_PCT'),
    F.sum('BMI_NUM').alias('BMI_NUM'),
    F.sum('BMI_DENOM').alias('BMI_DENOM'),
    F.round(F.sum('BMI_NUM') * 100 /F.sum('BMI_DENOM'), 2).alias('BMI_PCT'),
    F.sum('SMOKING_NUM').alias('SMOKING_NUM'),
    F.sum('SMOKING_DENOM').alias('SMOKING_DENOM'),
    F.round(F.sum('SMOKING_NUM') * 100 /F.sum('SMOKING_DENOM'), 2).alias('SMOKING_PCT'),
    F.sum('FOOT_NUM').alias('FOOT_NUM'),
    F.sum('FOOT_DENOM').alias('FOOT_DENOM'),
    F.round(F.sum('FOOT_NUM') * 100 /F.sum('FOOT_DENOM'), 2).alias('FOOT_PCT'),
    F.sum('NO_CVD_ON_STATIN_40_TO_80').alias('NO_CVD_ON_STATIN_NUM_40_TO_80'),
    F.sum('NO_CVD_40_TO_80').alias('NO_CVD_DENOM_40_TO_80'),
    F.round(F.sum('NO_CVD_ON_STATIN_40_TO_80') * 100 /F.sum('NO_CVD_40_TO_80'), 2).alias('NO_CVD_ON_STATIN_40_TO_80_PCT'),
    F.sum('CVD_ADMISSION_STATIN_FLAG').alias('SEC_PREVENT_NUM'),
    F.sum('WITH_CVD_ADMISSION').alias('SEC_PREVENT_DENOM'),
    F.round(F.sum('CVD_ADMISSION_STATIN_FLAG') * 100 /F.sum('WITH_CVD_ADMISSION'), 2).alias('WITH_CVD_ON_STATINS_PCT'),
    F.sum('COMBINED_NUM').alias('COMBINED_NUM'),
    F.sum('COMBINED_DENOM').alias('COMBINED_DENOM'),
    F.round(F.sum('COMBINED_NUM') * 100 /F.sum('COMBINED_DENOM'), 2).alias('COMBINED_PCT'),
    F.sum('BP_NUM').alias('BP_NUM'),
    F.sum('BP_DENOM').alias('BP_DENOM'),
    F.round(F.sum('BP_NUM') * 100 /F.sum('BP_DENOM'), 2).alias('BP_PCT'),
    F.sum('BP_140_80_TT_NUM').alias('BP_140_80_NUM'),
    F.sum('BP_TT_DENOM').alias('BP_140_80_DENOM'),
    F.round(F.sum('BP_140_80_TT_NUM') * 100 /F.sum('BP_TT_DENOM'), 2).alias('BP_140_80_PCT'),
    F.sum('NEWLY_DIAGNOSED').alias('NEWLY_DIAGNOSED'),
    F.sum('ND_OFFERED_12_M').alias('ND_OFFERED_12_M'),
    F.round(F.sum('ND_OFFERED_12_M') * 100 /F.sum('NEWLY_DIAGNOSED'), 2).alias('ND_OFFERED_12_M_PCT'),
    F.sum('ND_ATTENDED_12_M').alias('ND_ATTENDED_12_M'),
    F.round(F.sum('ND_ATTENDED_12_M') * 100 /F.sum('NEWLY_DIAGNOSED'), 2).alias('ND_ATTENDED_12_M_PCT'),
    F.sum('ND_OFFERED_EVER').alias('ND_OFFERED_EVER'),
    F.round(F.sum('ND_OFFERED_EVER') * 100 /F.sum('NEWLY_DIAGNOSED'), 2).alias('ND_OFFERED_EVER_PCT'),
    F.sum('ND_ATTENDED_EVER').alias('ND_ATTENDED_EVER'),
    F.round(F.sum('ND_ATTENDED_EVER') * 100 /F.sum('NEWLY_DIAGNOSED'), 2).alias('ND_ATTENDED_EVER_PCT'),
)
