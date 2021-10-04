
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DateType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 2)
from diabetes_code.utilities.field_definitions import *


class TestRegistrationsDenominator(object):
    """
    registrations_denominator = (
        F.when(F.col('NHS_NUMBER').isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_registrations_returns_denom(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 123456789),
                (2, None),
            ],
            ["ID", "NHS_NUMBER"]
        )

        return_df = (input_df.withColumn('REGISTRATIONS', registrations_denominator))
        expected = [1, 0]
        actual = [row['REGISTRATIONS'] for row in return_df.collect()]
        assert actual == expected, f"When testing registrations_denom(), expected to find {expected} but found {actual}"


class TestCreatinineDateFlag(object):
    """
    creatinine_date_flag = (
        F.when(F.col("CREATININE_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_creatinine_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "CREATININE_DATE"]
        )

        return_df = (input_df.withColumn('CREATININE_DATE_FLAG', creatinine_date_flag))
        expected = [1, 0]
        actual = [row['CREATININE_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing creatinine_date_flag(), expected to find {expected} but found {actual}"


class TestAlbuminDateFlag(object):
    """
    albumin_date_flag = (
        F.when(F.col("ALBUMIN_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_albumin_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "ALBUMIN_DATE"]
        )

        return_df = (input_df.withColumn('ALBUMIN_DATE_FLAG', albumin_date_flag))
        expected = [1, 0]
        actual = [row['ALBUMIN_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing albumin_date_flag(), expected to find {expected} but found {actual}"


class TestEyeDateFlag(object):
    """
    eye_date_flag = (
        F.when(F.col("EYE_EXAM_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_eye_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "EYE_EXAM_DATE"]
        )

        return_df = (input_df.withColumn('EYE_EXAM_DATE_FLAG', eye_date_flag))
        expected = [1, 0]
        actual = [row['EYE_EXAM_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing eye_date_flag(), expected to find {expected} but found {actual}"


class TestFootDateFlag(object):
    """
    foot_date_flag = (
        F.when(F.col("FOOT_EXAM_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_foot_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "FOOT_EXAM_DATE"]
        )

        return_df = (input_df.withColumn('FOOT_EXAM_DATE_FLAG', foot_date_flag))
        expected = [1, 0]
        actual = [row['FOOT_EXAM_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing foot_date_flag(), expected to find {expected} but found {actual}"


class TestSmokingDateFlag(object):
    """
    smoking_date_flag = (
        F.when(F.col("SMOKING_DATE").between(START_DATE, END_DATE), 1)
         .when((F.col("BIRTH_DATE").isNotNull()) & 
        (F.col("SMOKING_DATE").between(AGE_LOWER, AGE_UPPER)) &
        (F.col("SMOKING_VALUE") == 4) & 
        (F.col("AGE") >= 25) &
        (F.floor((F.months_between(F.col('SMOKING_DATE'), F.col('BIRTH_DATE'))/12)) > 25), 1)
        .otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_smoking_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, None, 4, '2001-01-01'),
                (12, '1970-01-01', 4, '2020-01-01'),
                (12, '1970-01-01', 4, '1970-01-31'),
                (11, '1970-01-01', 4, '2020-01-01'),
                (12, '1970-01-01', 1, '2020-01-01'),
                (12, '1970-01-01', 1, None),
                (12, '1970-01-01', 3, '2018-01-01'),
                (12, '2000-01-01', 4, '2018-01-01')
            ],
            ["AGE", "BIRTH_DATE", "SMOKING_VALUE", "SMOKING_DATE"]
        )

        return_df = (input_df.withColumn('SMOKING_DATE_FLAG', smoking_date_flag))
        expected = [0, 1, 0, 1, 1, 0, 0, 0]
        actual = [row['SMOKING_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing smoking_date_flag(), expected to find {expected} but found {actual}"


class TestEDOfferDateFlag(object):
    """
    ed_offer_date_flag = (
        F.when(F.col("ED_OFFER_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_edoffer_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "ED_OFFER_DATE"]
        )

        return_df = (input_df.withColumn('ED_OFFER_DATE_FLAG', ed_offer_date_flag))
        expected = [1, 0]
        actual = [row['ED_OFFER_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing ed_offer_date_flag(), expected to find {expected} but found {actual}"


class TestEDAttendDateFlag(object):
    """
    ed_attend_date_flag = (
        F.when(F.col("ED_ATTEND_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_edattend_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "ED_ATTEND_DATE"]
        )

        return_df = (input_df.withColumn('ED_ATTEND_DATE_FLAG', ed_attend_date_flag))
        expected = [1, 0]
        actual = [row['ED_ATTEND_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing ed_attend_date_flag(), expected to find {expected} but found {actual}"


class TestBMIDateFlag(object):
    """
    bmi_date_flag = (
        F.when(F.col("BMI_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_bmi_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "BMI_DATE"]
        )

        return_df = (input_df.withColumn('BMI_DATE_FLAG', bmi_date_flag))
        expected = [1, 0]
        actual = [row['BMI_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing bmi_date_flag(), expected to find {expected} but found {actual}"


class TestBPDateFlag(object):
    """
    bp_date_flag = (
        F.when(F.col("BP_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_bp_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "BP_DATE"]
        )

        return_df = (input_df.withColumn('BP_DATE_FLAG', bp_date_flag))
        expected = [1, 0]
        actual = [row['BP_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing bp_date_flag(), expected to find {expected} but found {actual}"



class TestCholesterolDateFlag(object):
    """
    cholesterol_date_flag = (
        F.when(F.col("CHOLESTEROL_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_chol_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "CHOLESTEROL_DATE"]
        )

        return_df = (input_df.withColumn('CHOLESTEROL_DATE_FLAG', cholesterol_date_flag))
        expected = [1, 0]
        actual = [row['CHOLESTEROL_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing cholesterol_date_flag(), expected to find {expected} but found {actual}"


class TestHBA1CDateFlag(object):
    """
    hba1c_date_flag = (
        F.when(F.col("HBA1C_DATE").isNotNull(), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_date_returns_correct_hba1c_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '2021-02-13'),
                (2, None),
            ],
            ["ID", "HBA1C_DATE"]
        )

        return_df = (input_df.withColumn('HBA1C_DATE_FLAG', hba1c_date_flag))
        expected = [1, 0]
        actual = [row['HBA1C_DATE_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When testing hba1c_date_flag(), expected to find {expected} but found {actual}"


class TestCholesterolNumerator(object):
    """
    cholesterol_numerator = (
    F.when((F.col('AGE') >= 12) & (F.col("CHOLESTEROL_DATE").isNotNull()), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_chol_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, 1),
                (0,  1),
                (11, 1),
                (12, 1),
                (13, 1),
                (20, 1),
                (None, 1)
            ],
            ["AGE", "CHOL_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('CHOLESTEROL_NUM', cholesterol_numerator))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['CHOLESTEROL_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking cholesterol numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_cholesterol_date_returns_correct_chol_numer(self, spark_session):
        input_df = spark_session.createDataFrame(
            [(13,    None),
            (20000, 1),
            (20,    1)],
            ["AGE", "CHOL_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('CHOLESTEROL_NUM', cholesterol_numerator))

        expected = [0, 1, 1]
        actual = [row['CHOLESTEROL_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking cholesterol numerator, expected to find {expected} but found {actual}"


class TestCholesterolDenominator(object):
    """
    cholesterol_denominator = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_chol_denom(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, '2021-02-13'),
                (0,  '2021-02-13'),
                (11, '2021-02-13'),
                (12, '2021-02-13'),
                (13, '2021-02-13'),
                (20, '2021-02-13'),
                (None, '2021-02-13')
            ],
            ["AGE", "CHOLESTEROL_DATE"]
        )

        return_df = (input_df.withColumn('CHOLESTEROL_DEN', cholesterol_denominator))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['CHOLESTEROL_DEN'] for row in return_df.collect()]
        assert actual == expected, f"When checking cholesterol denominator, expected to find {expected} but found {actual}"


################################

class TestOrgCodeToCountry(object):
    """
    org_code_to_country = (
    F.when(F.col('ORGANISATION_CODE').like('7%'), 'Wales')
    .when(F.col('ORGANISATION_CODE').like('W%'), 'Wales')
    .when(F.col('ORGANISATION_CODE').isNull(), F.lit(None))
    .otherwise('England')
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_recoded_org_code_returns_correct_country(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, '72'),
                (2, '7'),
                (3, '723333'),
                (4,  'We'),
                (5,  'W'),
                (6,  'WAAA'),
                (7,  None),
                (8, 'En'),
                (9, ' 72'),
                (10, ' We'),
                (11, ''),
                (12, '0')
            ],
            ["ID", "ORGANISATION_CODE"]
        )

        return_df = (input_df.withColumn('ORGANISATION_CODE', org_code_to_country))

        expected = ['Wales', 'Wales', 'Wales', 'Wales', 'Wales', 'Wales', None,
                    'England', 'England' , 'England' , 'England' , 'England']
        actual = [row['ORGANISATION_CODE'] for row in return_df.collect()]
        assert actual == expected, f"When testing org_code_to_country(), expected to find {expected} but found {actual}"


################################

class TestDiabetesTypeRecoded(object):
    """
    diabetes_type_recoded = (
    F.when(F.col('DIABETES_TYPE') == 1, '01')
    .when(F.col('DIABETES_TYPE') == 2, '02')
    .when(F.col('DIABETES_TYPE') == 6, '06')
    .when(F.col('DIABETES_TYPE') == 8, '08')
    .when(F.col('DIABETES_TYPE') == 99, '99')
    .otherwise(F.lit(None))
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_diabetes_types_recoded_successfully(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 1),
                (2, 2),
                (3, 6),
                (4, 8),
                (5, 99),
                (6, 100),
                (7, 1000),
                (8, 0),
                (9, -3),
                (10, 30),
                (11, None)
            ],
            ["ID", "DIABETES_TYPE"]
        )

        return_df = (input_df.withColumn('DIABETES_TYPE', diabetes_type_recoded))

        expected = ['01', '02', '06', '08', '99', None, None, None, None, None, None]
        actual = [row['DIABETES_TYPE'] for row in return_df.collect()]
        assert actual == expected, f"When testing diabetes_type_recoded, expected to find {expected} but found {actual}"


################################

class TestDiabetesTypePublication(object):
    """
    diabetes_type_publication = (
    F.when(F.col('DIABETES_TYPE') == 1, 'TYPE 1')
     .when(F.col('DIABETES_TYPE').isin([2,6,8]), 'TYPE 2 AND OTHERS')
     .otherwise('ERROR')
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_diabetes_types(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 1),
                (2, 2),
                (3, 6),
                (4, 8),
                (5, 99),
                (6, 100),
                (7, 1000),
                (8, 0),
                (9, -3),
                (10, 30),
                (11, None)
            ],
            ["ID", "DIABETES_TYPE"]
        )

        return_df = (input_df.withColumn('DIABETES_TYPE', diabetes_type_publication))

        expected = ['TYPE 1', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS', 'TYPE 2 AND OTHERS']
        actual = [row['DIABETES_TYPE'] for row in return_df.collect()]
        assert actual == expected, f"When testing diabetes_types, expected to find {expected} but found {actual}"


################################
class TestBPNumerator(object):
    """
    bp_numerator = (
        F.when((F.col('BP_DATE_FLAG') == 1) &
                (F.col('AGE') >= 12), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_date_returns_correct_bp_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (10, 1),
                (11, 1),
                (12, 1),
                (13, 1),
                (13, 0),
                (13, None)
            ],
            ["AGE", "BP_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('BP_NUM', bp_numerator))

        expected = [0, 0, 1, 1, 0, 0]
        actual = [row['BP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When testing bp numerator, expected to find {expected} but found {actual}"


class TestBPDenominator(object):
    """
    bp_denominator = (
    F.when(F.col('AGE') >= 12, 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_corrcet_bp_denom(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 0),
                (2, 11),
                (3, 12),
                (4, 13),
                (5, 14),
                (12, -1)
            ],
            ["ID", "AGE"]
        )

        return_df = (input_df.withColumn('BP_DENOM', bp_denominator))

        expected = [0, 0, 1, 1, 1, 0]
        actual = [row['BP_DENOM'] for row in return_df.collect()]
        assert actual == expected, f"When testing bp denominator, expected to find {expected} but found {actual}"





################################
class TestAgeCleanedField(object):
    """
    age_cleaned_field = (
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
    """
    @pytest.mark.usefixtures("spark_session")
    def test_ages_above_below_zero_return_correct_age_cleaned(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                ('1981-11-28', -3),
                ('1981-11-28', -2),
                ('1981-11-28', -1),
                ('1981-11-28', 0),
                ('1981-11-28', 1),
                ('1982-02-27', 2),
                ('1981-11-28', 39),
            ],
            ["BIRTH_DATE", "AGE"]
        )

        return_df = (input_df.withColumn('RETURN', age_cleaned))

        expected = [None, 0, 0, 0, 1, 2, 39]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When testing ages above and below zero, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_DOB_outside_upper_lower_bound_return(self, spark_session):

        # The definition is 'between(AGE_LOWER, AGE_UPPER)'
        # This is inclusive - i.e. if a DOB falls on the date, it should be counted as valid
        input_df = spark_session.createDataFrame(
            [
                ('1900-11-28', 10),
                ('1907-01-01', 10),
                ('1981-11-28', 10),
                ('2020-12-31', 10),
                ('2021-01-01', 10),
            ],
            ["BIRTH_DATE", "AGE"]
        )

        return_df = (input_df.withColumn('RETURN', age_cleaned))

        expected = [None, 10, 10, 10, None]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When testing DOB against upper and lower bound, expected to find {expected} but found {actual}"


################################
class TestHba1cLessOrEqual53ttNumerator(object):
    """
    hba1c_less_or_equal_53_tt_numerator = (
    F.when((F.col('HBA1C_Date').isNotNull()) & (F.col('HBA1C_VALUE') <= 53), 1).otherwise(0)
    )
    """

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_HBA1C_VALUE_returns_correct_hba1c_53_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (0,  1),
            (52, 1),
            (53, 1),
            (54, 1),
            (None, 1),
            (52, 0),
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_53_TT_NUM', hba1c_less_or_equal_53_tt_numerator))

        expected = [1, 1, 1, 0, 0, 0]
        actual = [row['HBA1C_<=_53_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_53_TT numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_HBA1C_value_returns_correct_hba1c_53_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (53, 1),
            (53, 0),
            (10, 0)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_53_TT_NUM', hba1c_less_or_equal_53_tt_numerator))

        expected = [1, 0, 0]
        actual = [row['HBA1C_<=_53_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_53_TT numerator, expected to find {expected} but found {actual}"


################################
class TestHba1cLesOrEqual48ttNumerator(object):
    """
    hba1c_less_or_equal_48_tt_numerator = (
    F.when((F.col('HbA1c_Date').between(HBA1C_LOWER, HBA1C_UPPER)) & (F.col('HBA1C_VALUE') <= 48), 1).otherwise(0)
    )
    """

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_HBA1C_VALUE_returns_correct_hba1c_48_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (48, 1),
            (0,  1),
            (40, 1),
            (70, 1),
            (13, 1),
            (200, 1),
            (None, 1)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_48_TT_NUM', hba1c_less_or_equal_48_tt_numerator))

        expected = [1, 1, 1, 0, 1, 0, 0]
        actual = [row['HBA1C_<=_48_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_48_TT numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_HBA1C_date_returns_correct_hba1c_48_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (48, 1),
            (48, 0),
            (10, 1),
            (10, 0)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_48_TT_NUM', hba1c_less_or_equal_48_tt_numerator))

        expected = [1, 0, 1, 0]
        actual = [row['HBA1C_<=_48_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_48_TT numerator, expected to find {expected} but found {actual}"

################################
class TestHba1cLesOrEqual58ttNumerator(object):
    """
    hba1c_less_or_equal_58_tt_numerator = (
    F.when((F.col('HbA1c_Date').between(HBA1C_LOWER, HBA1C_UPPER)) & (F.col('HBA1C_VALUE') <= 58), 1).otherwise(0)
    )
    """

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_hba1c_value_returns_correct_hba1c_58_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (58, 1),
            (0,  1),
            (40, 1),
            (70, 1),
            (13, 1),
            (200, 1),
            (None, 1)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_58_TT_NUM', hba1c_less_or_equal_58_tt_numerator))

        expected = [1, 1, 1, 0, 1, 0, 0]
        actual = [row['HBA1C_<=_58_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_58_TT numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_HBA1C_date_returns_correct_hba1c_58_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (58, 1),
            (58, 0),
            (10, 1),
            (10, 0)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_58_TT_NUM', hba1c_less_or_equal_58_tt_numerator))

        expected = [1, 0, 1, 0]
        actual = [row['HBA1C_<=_58_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_58_TT numerator, expected to find {expected} but found {actual}"


################################
class TestHba1cLesOrEqual75ttNumerator(object):
    """
    hba1c_less_or_equal_75_tt_numerator = (
    F.when((F.col('HbA1c_Date').between(HBA1C_LOWER, HBA1C_UPPER)) & (F.col('HBA1C_VALUE') <= 75), 1).otherwise(0)
    )
    """

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_hba1c_value_returns_correct_hba1c_75_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (75, 1),
            (0,  1),
            (40, 1),
            (80, 1),
            (13, 1),
            (200, 1),
            (None, 1)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_75_TT_NUM', hba1c_less_or_equal_75_tt_numerator))

        expected = [1, 1, 1, 0, 1, 0, 0]
        actual = [row['HBA1C_<=_75_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_75_TT numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_hba1c_date_returns_correct_hba1c_75_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (75, 1),
            (75, 0),
            (10, 1),
            (10, 0)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_75_TT_NUM', hba1c_less_or_equal_75_tt_numerator))

        expected = [1, 0, 1, 0]
        actual = [row['HBA1C_<=_75_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_75_TT numerator, expected to find {expected} but found {actual}"


################################
class TestHba1cLesOrEqual86ttNumerator(object):
    """
    hba1c_less_or_equal_86_tt_numerator = (
    F.when((F.col('HbA1c_Date').between(HBA1C_LOWER, HBA1C_UPPER)) & (F.col('HBA1C_VALUE') <= 86), 1).otherwise(0)
    )
    """

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_hba1c_value_returns_correct_hba1c_86_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (86, 1),
            (0,  1),
            (40, 1),
            (90, 1),
            (13, 1),
            (200, 1),
            (None, 1)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_86_TT_NUM', hba1c_less_or_equal_86_tt_numerator))

        expected = [1, 1, 1, 0, 1, 0, 0]
        actual = [row['HBA1C_<=_86_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_86_TT numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_hba1c_date_returns_correct_hba1c_86_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (86, 1),
            (86, 0),
            (10, 1),
            (10, 0)
            ],
            ["HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_<=_86_TT_NUM', hba1c_less_or_equal_86_tt_numerator))

        expected = [1, 0, 1, 0]
        actual = [row['HBA1C_<=_86_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C_<=_86_TT numerator, expected to find {expected} but found {actual}"


################################
class TestHba1cNumerator(object):
    """
    hba1c_numerator = (
     F.when(F.col('HbA1c_Date').isNotNull(), 1).otherwise(0)
    )
    """

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_HBA1C_date_returns_correct_hba1c_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 1),
            (5, 1),
            (6, None)
            ],
            ["!D", "HBA1C_DATE_FLAG"]
        )

        HBA1C_LOWER = F.to_date(F.lit(params['publication_start_date']))
        HBA1C_UPPER = F.to_date(F.lit(params['publication_end_date']))

        return_df = (input_df.withColumn('HBA1C_NUM', hba1c_numerator))

        expected = [1, 1, 1, 1, 1, 0]
        actual = [row['HBA1C_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C numerator, expected to find {expected} but found {actual}"


class TestHba1cTTDenominator(object):
    """
    hba1c_tt_denominator = (
    F.when(
        (F.col('HBA1C_VALUE').isNotNull()) &
        (F.col('HBA1C_Date').isNotNull()), 
        1).otherwise(0)
)
    """

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_HBA1C_value_returns_correct_hba1c_numerator(self, spark_session):

        input_df = spark_session.createDataFrame(
            [
            (1, 1, 1),
            (2, 0, 1),
            (3, 10, 1),
            (4, None, 1),
            (5, 1, 0),
            ],
            ["ID", "HBA1C_VALUE", "HBA1C_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('HBA1C_DENOM', hba1c_tt_denominator))

        expected = [1, 1, 1, 0, 0]
        actual = [row['HBA1C_DENOM'] for row in return_df.collect()]
        assert actual == expected, f"When checking HBA1C denominator, expected to find {expected} but found {actual}"


################################
class TestAlbuminNumerator(object):
    """
    albumin_numerator = (
    F.when((F.col('AGE') >= 12) & (F.col("ALBUMIN_DATE").isNotNull()), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_albumin_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, 1),
                (0,  1),
                (11, 1),
                (12, 1),
                (13, 1),
                (20, 1),
                (None, 1)
            ],
            ["AGE", "ALBUMIN_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('ALBUMIN_NUM', albumin_numerator))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['ALBUMIN_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking albumin numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_albumin_date_returns_correct_albumin_numer(self, spark_session):
        input_df = spark_session.createDataFrame(
            [(13,    None),
            (20000, 1),
            (20,    1)],
            ["AGE", "ALBUMIN_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('ALBUMIN_NUM', albumin_numerator))

        expected = [0, 1, 1]
        actual = [row['ALBUMIN_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking albumin numerator, expected to find {expected} but found {actual}"


class TestAlbuminDenominator(object):
    """
    albumin_denominator = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_albumin_denom(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, '2021-02-13'),
                (0,  '2021-02-13'),
                (11, '2021-02-13'),
                (12, '2021-02-13'),
                (13, '2021-02-13'),
                (20, '2021-02-13'),
                (None, '2021-02-13')
            ],
            ["AGE", "ALBUMIN_DATE"]
        )

        return_df = (input_df.withColumn('ALBUMIN_DEN', albumin_denominator))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['ALBUMIN_DEN'] for row in return_df.collect()]
        assert actual == expected, f"When checking albumin denominator, expected to find {expected} but found {actual}"


################################
class TestCholesterolLessThan5(object):
    """
    cholesterol_less_than_5 = (
        F.when(
            (F.col('AGE') >= 12) &
            (F.col("CHOLESTEROL_DATE").isNotNull()) &
            (F.col("CHOLESTEROL_VALUE") < 5)
            , 1).otherwise(0)
    )

    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_chol_less_than_5(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, 4, '2021-02-021'),
                (0,  4, '2021-02-021'),
                (11, 4, '2021-02-021'),
                (12, 4, '2021-02-021'),
                (13, 4, '2021-02-021'),
                (20, 4, '2021-02-021'),
                (None, 4, '2021-02-021')
            ],
            ["AGE", "CHOLESTEROL_VALUE", "CHOLESTEROL_DATE"]
        )

        return_df = (input_df.withColumn('RETURN', cholesterol_less_than_5))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When checking cholesterol less than 5, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_cholesterol_value_returns_correct_chol_less_than_5(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 0, '2021-02-021'),
                (12, 4, '2021-02-021'),
                (12, 5, '2021-02-021'),
                (12, 6, '2021-02-021'),
                (11, 6, '2021-02-021'),
                (None, 4, '2021-02-021'),
                (12, None, '2021-02-021')
            ],
            ["AGE", "CHOLESTEROL_VALUE", "CHOLESTEROL_DATE"]
        )

        return_df = (input_df.withColumn('RETURN', cholesterol_less_than_5))

        expected = [1, 1, 0, 0, 0, 0, 0]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When checking cholesterol less than 5, expected to find {expected} but found {actual}"


class TestCholesterolTTDenominator(object):
    """
    cholesterol_TT_denominator = (
    F.when((F.col('AGE') >= 12) & (F.col("CHOLESTEROL_VALUE").isNotNull()), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_data_returns_correct_chol_tt_denom(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, 0, '2021-02-021'),
                (0,  1, '2021-02-021'),
                (11, 50, '2021-02-021'),
                (12, 0, '2021-02-021'),
                (13, 2, '2021-02-021'),
                (20, None, '2021-02-021'),
                (None, 0, '2021-02-021')
            ],
            ["AGE", "CHOLESTEROL_VALUE", "CHOLESTEROL_DATE"]
        )

        return_df = (input_df.withColumn('CHOLESTEROL_TT_DEN', cholesterol_TT_denominator))

        expected = [0, 0, 0, 1, 1, 0, 0]
        actual = [row['CHOLESTEROL_TT_DEN'] for row in return_df.collect()]
        assert actual == expected, f"When checking cholesterol tt denominator, expected to find {expected} but found {actual}"


################################
class TestSmokeDenominator(object):
    """
    smoking_denominator = (
        F.when((F.col('AGE') >= 12), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_smoke_denom(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, '3217225422'),
                (0,  '3217225422'),
                (11, '3217225422'),
                (12, '3217225422'),
                (13, '3217225422'),
                (20, '3217225422'),
                (None, '3217225422')
            ],
            ["AGE", "NHS_NUMBER"]
        )

        return_df = (input_df.withColumn('SMOKE_DENOM', smoking_denominator))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['SMOKE_DENOM'] for row in return_df.collect()]
        assert actual == expected, f"When checking smoking denominator, expected to find {expected} but found {actual}"

class TestSmokeNumerator(object):
    """
    smoking_numerator = (
        F.when((F.col('AGE') >= 12) & (F.col('SMOKING_DATE_FLAG') == 1), 1).otherwise(0)
    )
    """

    @pytest.mark.usefixtures('spark_session')
    def test_smoking_date_above_below_start_date_return_correct_smoking_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 25, 4, '1981-11-28'),
                (1, 39, 4, '1981-11-28'),
                (None,  24, 4, '1981-11-28'),
                (1, 25, 4, '1981-11-28'),
                (None, 19, 4, '1981-11-28'),
                (1, 25, 4, '1981-11-28')
            ],
            ['SMOKING_DATE_FLAG', 'AGE', 'SMOKING_VALUE','BIRTH_DATE']
        )

        return_df = (input_df.withColumn('RETURN', smoking_numerator))

        expected = [1, 1, 0, 1, 0, 1]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f' {expected} but found {actual}'

    @pytest.mark.usefixtures('spark_session')
    def test_smoking_age_above_below_12__return_correct_smoking_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 12, 4, '1981-11-28'),
                (1, 12, 4, '1981-11-28'),
                (1, 25, 4, '1981-11-28'),
                (1, 11, 4, '1981-11-28'),
                (1, 11, 4, '1981-11-28'),
                (1, 13, 4, '1981-11-28')
            ],
            ['SMOKING_DATE_FLAG', 'AGE', 'SMOKING_VALUE','BIRTH_DATE']
        )

        return_df = (input_df.withColumn('RETURN', smoking_numerator))

        expected = [1, 1, 1, 0, 0, 1]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f' {expected} but found {actual}'


################################
class TestBmiNumerator(object):
    """
    bmi_numerator = (
    F.when(
        (F.col('AGE') >= 12) & (F.col('BMI_DATE_FLAG') == 1), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_bmi_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, 1),
                (0,  1),
                (11, 1),
                (12, 1),
                (13, 1),
                (20, 1),
                (None, 1)
            ],
            ["AGE", "BMI_DATE_FLAG"]
        )

        START_DATE = F.to_date(F.lit(params['publication_start_date']))
        END_DATE = F.to_date(F.lit(params['publication_end_date']))

        return_df = (input_df.withColumn('BMI_NUM', bmi_numerator))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['BMI_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking bmi numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_bmi_date_returns_correct_bmi_numer(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (13,    None),
                (20000, 1),
                (20000, 1),
                (20,    1),
                (20,    1)
            ],
            ["AGE", "BMI_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('BMI_NUM', bmi_numerator))

        expected = [0, 1, 1, 1, 1]
        actual = [row['BMI_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking bmi numerator, expected to find {expected} but found {actual}"


class TestBmiDenomerator(object):
    """
    bmi_denominator = (
        F.when((F.col('AGE') >= 12), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_bmi_denominator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, '3217225422'),
                (0,  '3217225422'),
                (11, '3217225422'),
                (12, '3217225422'),
                (13, '3217225422'),
                (20, '3217225422'),
                (None, '3217225422')
            ],
            ["AGE", "NHS_NUMBER"]
        )

        return_df = (input_df.withColumn('BMI_DENOM', bmi_denominator))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['BMI_DENOM'] for row in return_df.collect()]
        assert actual == expected, f"When checking bmi denominator, expected to find {expected} but found {actual}"


################################
class TestFootNumField(object):
    """
    foot_num = (
        F.when((F.col('AGE') >= 12) & (F.col("FOOT_EXAM_DATE").isnotnull()), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_ages_above_below_12_return_correct_foot_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, 1),
                (0,  1),
                (11, 1),
                (12, 1),
                (13, 1)
            ],
            ["AGE", "FOOT_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('RETURN', foot_num))

        expected = [0,0,0, 1, 1]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When testing ages above and below 12, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_foot_date_flag_above_below_1_return_correct_foot_num(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, 1),
                (0,  1),
                (11, 1),
                (12, 1),
                (13, 1),
                (13, None)
            ],
            ["AGE", "FOOT_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('RETURN', foot_num))

        expected = [0,0,0, 1, 1, 0]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When testing ages above and below 12, expected to find {expected} but found {actual}"


class TestFootDenomField(object):
    """
    foot_denom = (
        F.when((F.col('AGE') >= 12) , 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_ages_above_below_12_return_correct_foot_denom(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                -1,
                0,
                11,
                12,
                13
            ], IntegerType()
        ).toDF("AGE")


        return_df = (input_df.withColumn('RETURN', foot_denom))

        expected = [0,0,0, 1, 1]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When testing foot denominator above and below 12, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_corrcet_nhs_number_return_correct_foot_denom(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                -1,
                 0,
                12,
                20,
                10
            ], IntegerType()
        ).toDF("AGE")

        return_df = (input_df.withColumn('RETURN', foot_denom))

        expected = [0,0,1, 1, 0]
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When testing foot denominator NHS number, expected to find {expected} but found {actual}"


class TestNoCVDOnStatins40to80(object):
    '''
    no_cvd_on_statins_40_to_80 = (
    F.when((F.col('CVD_ADMISSION').isNull()) &
        (F.col('IHD_VALUE').isNull()) &
        (F.col('STATIN_FLAG') == 1) &
        (F.col('AGE') >= 40) &
        (F.col('AGE') <= 80), 1).otherwise(0)
    )
    '''
    @pytest.mark.usefixtures("spark_session")
    def test_correct_values_return_correct_flag_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (None, None, 1, 40),
                (1, 23, 1, 39),
                (1, 23, 1, 41),
                (1, 23, 0, 45),
                (1, 23, 0, None),
                (None, None, 1, 48),
                (1, None, 1, 45),
                (None, None, 0, 45),
                (None, 23, 1, 25),
                (1, 23, 1, 25)
            ],
            ["CVD_ADMISSION", "IHD_VALUE", "STATIN_FLAG", "AGE"]
        )

        return_df = (input_df.withColumn('NO_CVD_ON_STATINS_40_TO_80', no_cvd_on_statins_40_to_80))

        expected = [1, 0, 0, 0, 0, 1, 0, 0, 0, 0]
        actual = [row['NO_CVD_ON_STATINS_40_TO_80'] for row in return_df.collect()]
        assert actual == expected, f"When checking no CVD on Statins 40 to 80, expected to find {expected} but found {actual}"

class TestNoCVD40to80(object):
    '''
    no_cvd_40_to_80 = (
    F.when((F.col('CVD_ADMISSION').isNull()) &
        (F.col('IHD_VALUE').isNull()) &
        (F.col('AGE') >= 40) & (F.col('AGE') <= 80), 1).otherwise(0)
    )
    '''
    @pytest.mark.usefixtures("spark_session")
    def test_correct_values_return_correct_flag_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (None, None, 40),
                (1, 23, 39),
                (1, 23, 41),
                (1, 23, 45),
                (1, 23, None),
                (None, None, 48),
                (1, None, 45),
                (None, None, 45),
                (None, 23, 25),
                (1, 23, 25)
            ],
            ["CVD_ADMISSION", "IHD_VALUE", "AGE"]
        )

        return_df = (input_df.withColumn('NO_CVD_40_TO_80', no_cvd_40_to_80))

        expected = [1, 0, 0, 0, 0, 1, 0, 1, 0, 0]
        actual = [row['NO_CVD_40_TO_80'] for row in return_df.collect()]
        assert actual == expected, f"When checking no cvd 40 to 80, expected to find {expected} but found {actual}"

class TestStatinFlag(object):
    '''
    cvd_admission_statin_flag = (
        F.when(((F.col('CVD_ADMISSION') == 1) |
        (F.col('IHD_VALUE') == 1)) &
        (F.col('STATIN_FLAG') == 1), 1).otherwise(0)
    )
    '''
    @pytest.mark.usefixtures("spark_session")
    def test_correct_values_return_correct_flag_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 1, 1),
                (1, 1, None),
                (1, 1, 41),
                (1, None, 1),
                (1, None, None),
                (1, 1, 1),
                (1, None, 45),
                (1, 1, 1),
                (None, 1, 1),
                (1, 23, 1)
            ],
            ["CVD_ADMISSION", "IHD_VALUE", "STATIN_FLAG"]
        )

        return_df = (input_df.withColumn('CVD_ADMISSION_STATIN_FLAG', cvd_admission_statin_flag))

        expected = [1, 0, 0, 1, 0, 1, 0, 1, 1, 1]
        actual = [row['CVD_ADMISSION_STATIN_FLAG'] for row in return_df.collect()]
        assert actual == expected, f"When checking cvd admission statin flag, expected to find {expected} but found {actual}"

class TestWithCVDAdmission(object):
    '''
    with_cvd_admission = (
        F.when(((F.col('CVD_ADMISSION') == 1) | (F.col('IHD_VALUE') == 1)), 1).otherwise(0)
    )
    '''
    @pytest.mark.usefixtures("spark_session")
    def test_correct_values_return_correct_flag_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 1),
                (1, 0),
                (-1, -1),
                (1, None),
                (1, None),
                (None, None),
                (1, None),
                (1, 1),
                (None, 1),
                (77, 23)
            ],
            ["CVD_ADMISSION", "IHD_VALUE"]
        )

        return_df = (input_df.withColumn('WITH_CVD_ADMISSION', with_cvd_admission))

        expected = [1, 1, 0, 1, 1, 0, 1, 1, 1, 0]
        actual = [row['WITH_CVD_ADMISSION'] for row in return_df.collect()]
        assert actual == expected, f"When checking with cvd admission, expected to find {expected} but found {actual}"

class TestCombinedNumerator(object):
    '''
    combined_numerator = (
        F.when(((F.col('NO_CVD_ON_STATIN_40_TO_80') == 1) | (F.col('CVD_ADMISSION_STATIN_FLAG') == 1)), 1).otherwise(0)
    )

    '''
    @pytest.mark.usefixtures("spark_session")
    def test_correct_values_return_correct_numerator_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 1),
                (1, 0),
                (-1, -1),
                (1, None),
                (1, None),
                (None, None),
                (1, None),
                (1, 1),
                (None, 1),
                (77, 23)
            ],
            ["NO_CVD_ON_STATIN_40_TO_80", "CVD_ADMISSION_STATIN_FLAG"]
        )

        return_df = (input_df.withColumn('COMBINED_NUMERATOR', combined_numerator))

        expected = [1, 1, 0, 1, 1, 0, 1, 1, 1, 0]
        actual = [row['COMBINED_NUMERATOR'] for row in return_df.collect()]
        assert actual == expected, f"When checking combined numerator, expected to find {expected} but found {actual}"

class TestCombinedDenominator(object):
    '''
    combined_denominator = (
        F.when (((F.col('AGE') >= 40) & (F.col('AGE') <= 80)) |
        (F.col('CVD_ADMISSION') == 1) |
        (F.col('IHD_VALUE') == 1), 1).otherwise(0)
    )

    '''
    @pytest.mark.usefixtures("spark_session")
    def test_correct_values_return_correct_denominator_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (None, None, None),
                (None, 1, None),
                (1, None, 41),
                (1, 23, 45),
                (1, None, None),
                (None, None, 48),
                (1, None, 45),
                (None, None, 45),
                (None, 23, 25),
                (1, 23, 25)
            ],
            ["CVD_ADMISSION", "IHD_VALUE", "AGE"]
        )

        return_df = (input_df.withColumn('COMBINED_DENOMINATOR', combined_denominator))

        expected = [0, 1, 1, 1, 1, 1, 1, 1, 0, 1]
        actual = [row['COMBINED_DENOMINATOR'] for row in return_df.collect()]
        assert actual == expected, f"When checking combined denominator, expected to find {expected} but found {actual}"


class TestCreatinineNumerator(object):
    """
    creatinine_numerator = (
    F.when((F.col('AGE') >= 12) & (F.col("CREATININE_DATE").isNotNull()), 1).otherwise(0)
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_age_returns_correct_creatinine_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (-1, 1),
                (0,  1),
                (11, 1),
                (12, 1),
                (13, 1),
                (20, 1),
                (None, 1)
            ],
            ["AGE", "CREATININE_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('CREATININE_NUM', creatinine_numerator))

        expected = [0, 0, 0, 1, 1, 1, 0]
        actual = [row['CREATININE_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking creatinine numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_valid_invalid_creatinine_date_returns_correct_creatinine_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
            (13, None),
            (20, 1),
            (20, 1)
            ],
            ["AGE", "CREATININE_DATE_FLAG"]
        )

        return_df = (input_df.withColumn('CREATININE_NUM', creatinine_numerator))

        expected = [0, 1, 1]
        actual = [row['CREATININE_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking creatinine numerator, expected to find {expected} but found {actual}"


class TestCreatinineDenominator(object):
    """
    creatinine_denominator = (
    F.when((F.col('AGE') >= 12), 1).otherwise(0)
    )
    """

    def test_valid_invalid_age_returns_correct_creatinine_denominator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (1, 0),
                (2, 11),
                (3, 12),
                (4, 20),
                (5, None)
            ],
            ["ID", "AGE"]
        )

        return_df = (input_df.withColumn('CREATININE_DENOM', creatinine_denominator))

        expected = [0, 0, 1, 1, 0]
        actual = [row['CREATININE_DENOM'] for row in return_df.collect()]
        assert actual == expected, f"When checking bmi denominator, expected to find {expected} but found {actual}"


class TestEthnicityGroup(object):
    """
    ethnicity_group = (
        F.when(F.col('ETHNICITY').isin('A', 'B', 'C', 'T'), 'White')
        .when((F.col('ETHNICITY') == 'Z') | (F.col('ETHNICITY').isNull()), 'Unknown')
        .otherwise(F.lit('Minority'))
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_ethnicity_return_correct_group_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                'B',
                'A',
                'Z',
                None,
                'T',
                'Z',
                'E'
            ], StringType()
        ).toDF("ETHNICITY")


        return_df = (input_df.withColumn('RETURN', ethnicity_group))

        expected = ['White', 'White', 'Unknown', 'Unknown', 'White', 'Unknown', 'Minority']
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When testing ethnicity group, expected to find {expected} but found {actual}"


class TestIMDGroup(object):
    """
    deprivation_imd_group = (
        F.when(F.col('COMBINED_QUINTILE') == 1, 'IMD most deprived')
        .when(F.col('COMBINED_QUINTILE') == 2, 'IMD 2nd most deprived')
        .when(F.col('COMBINED_QUINTILE') == 3, 'IMD 3rd most deprived')
        .when(F.col('COMBINED_QUINTILE') == 4, 'IMD 2nd least deprived')
        .when(F.col('COMBINED_QUINTILE') == 5, 'IMD least deprived')
        .otherwise(F.lit('IMD unknown'))
    )

    """
    @pytest.mark.usefixtures("spark_session")
    def test_imd_deprivation_return_correct_group_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                3,
                8,
                4,
                None,
                5,
                2,
                9
            ], IntegerType()
        ).toDF('COMBINED_QUINTILE')

        return_df = (input_df.withColumn('RETURN', deprivation_imd_group))

        expected = ['IMD 3rd most deprived', 'IMD unknown', 'IMD 2nd least deprived', 'IMD unknown', 'IMD least deprived', 'IMD 2nd most deprived', 'IMD unknown']
        actual = [row['RETURN'] for row in return_df.collect()]
        assert actual == expected, f"When testing IMD Deprivation group, expected to find {expected} but found {actual}"


class TestAll8CPNumerator(object):
    """
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
    """
    @pytest.mark.usefixtures("spark_session")
    def test_age_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (13, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (11, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (None, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 1, 1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_or_operator_cases_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (13, 0, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (11, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (11, 0, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 0, 1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_hba1c_date_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (12, 0, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (11, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (11, 0, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 0, 1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_bp_date_flag_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (12, 1, 0, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_chol_date_flag_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (12, 1, 1, 0, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_creatinine_date_flag_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (12, 1, 1, 1, 0, 1, 1, 1, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_foot_date_flag_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (12, 1, 1, 1, 1, 0, 1, 1, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_albumin_date_flag_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (12, 1, 1, 1, 1, 1, 0, 1, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_bmi_date_flag_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01',3,'2020-01-01'),
                (12, 1, 1, 1, 1, 1, 1, 0, '2020-01-01',3,'2020-01-01')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [1, 0]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator, expected to find {expected} but found {actual}"

    @pytest.mark.usefixtures("spark_session")
    def test_age_over_or_equal_25_with_smoking_value_returns_correct_all_8_cp_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (25, 0, 1, 1, 1, 1, 1, 1, '2020-01-01', 4,'1981-11-28'),
                (24, 0, 1, 1, 1, 1, 1, 1, '2020-01-01', 4,'1981-11-28'),
                (25, 0, 1, 1, 1, 1, 1, 1, '2020-01-01', 3,'1981-11-28'),
                (25, 0, 1, 1, 1, 1, 1, 1, '2020-01-01', 4,'2020-01-01'),
                (25, 0, None, None, None, None, None, None, None, 4,'1981-11-28'),
                (11, 0, None, None, None, None, None, None, None, 4, '2020-01-01'),
                (25, 1, 1, 1, 1, 1, 1, 1, '2020-01-01', 4,'1981-11-28'),
                (12, 1, 1, 1, 1, 1, 1, 1, '2020-01-01', 4,'2008-06-17'),
                (11, 1, 1, 1, 1, 1, 1, 1, '2020-01-01', 4,'2009-06-17')
            ],
            ["AGE", "HBA1C_DATE_FLAG", "BP_DATE_FLAG", "CHOL_DATE_FLAG", "CREATININE_DATE_FLAG", "FOOT_DATE_FLAG",  "ALBUMIN_DATE_FLAG", "BMI_DATE_FLAG", "SMOKING_DATE","SMOKING_VALUE","BIRTH_DATE"]
        )

        return_df = (input_df.withColumn('ALL_8_CP_NUM', all_8_cp))

        expected = [0, 0, 0, 0, 0, 0, 1, 1, 1]
        actual = [row['ALL_8_CP_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 8 cp numerator with age above 25 , expected to find {expected} but found {actual}"


class TestAgeGroupPublication(object):
    """
    age_group_publication = (
        F.when(F.col('AGE') < 40, 'Aged under 40')
        .when((F.col('AGE') >= 40) & (F.col('AGE') <= 64), 'Aged 40 to 64')
        .when((F.col('AGE') >= 65) & (F.col('AGE') <= 79), 'Aged 65 to 79')
        .when(F.col('AGE') >= 80, 'Aged 80 and over')
        .otherwise('Age unknown')
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_ages_are_grouped_correctly(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (11, -1),
                (1,	5),
                (2,	25),
                (3, 39),
                (4,	40),
                (5,	64),
                (6,	65),
                (7,	79),
                (8,	80),
                (9,	81),
                (10,99),
                (12, None)
            ],
            ['ID', 'AGE']
        )

        return_df = (input_df.withColumn('AGE_GROUP', age_group_publication))

        expected = [('Aged under 40'), ('Aged under 40'), ('Aged under 40'), ('Aged under 40'),
                    ('Aged 40 to 64'), ('Aged 40 to 64'), 
                    ('Aged 65 to 79'), ('Aged 65 to 79'), 
                    ('Aged 80 and over'), ('Aged 80 and over'), ('Aged 80 and over'),
                    ('Age unknown')]
        actual = [row['AGE_GROUP'] for row in return_df.collect()]
        assert actual == expected, f"When checking age_group_publication(), expected to find {expected} but found {actual}"


class TestGender(object):
    """
    gender = (
        F.when(F.col("SEX") == 1, "Male")
         .when(F.col("SEX") == 2, 'Female')
         .otherwise('Unknown sex')
    )
    """
    @pytest.mark.usefixtures("spark_session")
    def test_gender_recoded_correctly(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
            (1,	1),
            (2,	2),
            (3,	None),
            (4,	3),
            (6,	0)
            ],
            ['ID', 'SEX']
        )

        return_df = (input_df.withColumn('GENDER', gender))
        expected = [
            ('Male'),
            ('Female'),
            ('Unknown sex'),
            ('Unknown sex'),
            ('Unknown sex')
                    ]
        actual = [row['GENDER'] for row in return_df.collect()]
        assert actual == expected, f"When checking gender(), expected to find {expected} but found {actual}"
class TestAll3TTNumerator(object):
    """
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
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_all_3_tt_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (None,	1,	1,	1),
                (12,	1,	1,	1),
                (13,	1,	1,	1),
                (13,	0,	1,	1),
                (13,	None,	1,	1),
                (13,	1,	0,	1),
                (13,	1,	None,	1),
                (13,	1,	1,	0),
                (13,	1,	1,	None),
                (11,	None,	1,	None),
                (11,	None,	0,	None)
            ],
            ["AGE", 'BP_140_80_TT_NUM', 'HBA1C_<=_58_TT_NUM', 'CHOLESTEROL_<5']
        )

        return_df = (input_df.withColumn('ALL_3_TT_NUM', all_3_tt_num))

        expected = [0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0]
        actual = [row['ALL_3_TT_NUM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 3 TT numerator, expected to find {expected} but found {actual}."


class TestAll3TTDenominator(object):
    """    
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
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_denominator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (None,	1,	1,	1),
                (12,	1,	1,	1),
                (13,	1,	1,	1),
                (13,	0,	1,	1),
                (13,	None,	1,	1),
                (13,	1,	0,	1),
                (13,	1,	None,	1),
                (13,	1,	1,	0),
                (13,	1,	1,	None),
                (11,	None,	1,	None),
                (11,	None,	0,	None)
            ],
            ["AGE", 'BP_TT_DENOM', 'HBA1C_TT_DENOM', 'CHOLESTEROL_TT_DENOM']
        )

        return_df = (input_df.withColumn('ALL_3_TT_DENOM', all_3_tt_denom))

        expected = [0,1,1,0,0,0,0,0,0,1,0]
        actual = [row['ALL_3_TT_DENOM'] for row in return_df.collect()]
        assert actual == expected, f"When checking all 3 TT denominator, expected to find {expected} but found {actual}."


class TestTT3NewNumerator(object):
    """
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
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                (11,	None,	None,	1,  	None,	None),
                (11,	None,	None,	None,	None,	None),
                (12,	1,	    0,	    1,	    1,	    1),
                (12, 	1,	    None,	1,	    1,	    1),   
                (12,	0,	    1,	    1,	    1,	    1),
                (12,	None,	1,	    1,	    1,	    1),
                (39,	1,	    1,	    1,	    1,	    1),
                (81,	1,	    1,	    1,	    1,	    1),
                (39,	None,	None,	1,	    1,	    1),
                (39,	None,	0,	    1,	    1,	    1),
                (39,	0,	    None,	1,	    1,	    1),
                (40,	0,   	0,   	1,	    1,	    1),
                (40,	None,	None,	1,  	1,  	1),
                (50,	0,  	0,   	1,  	1,  	1),
                (80,	None,	None,	1,  	1,  	1),
                (80,	None,	None,	0,  	1,  	1),
                (80,	None,	None,	1,  	0,  	1),
                (80,	None,	None,	1,  	1,  	0)
            ],
            ['AGE', 'CVD_ADMISSION', 'IHD_VALUE', 'HBA1C_<=_58_TT_NUM', 'BP_140_80_TT_NUM',	'STATIN_FLAG']
        )

        return_df = (input_df.withColumn('TT_3_NEW_NUMERATOR', tt_3_new_num))
        expected = [1,0,1,1,1,1,1,1,1,0,0,1,1,1,1,0,0,0]
        actual = [row['TT_3_NEW_NUMERATOR'] for row in return_df.collect()]
        assert actual == expected, f"When checking TT 3 New Numerator, expected to find {expected} but found {actual}."



class TestTT3NewDenominator(object):
    """
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
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_numerator(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
            (11,	1,	None),
            (11,	0,	None),
            (11,	None,	None),
            (12,	1,	1),
            (13,	1,	1),
            (13,	0,	1),
            (13,	None,	1),
            (13,	1,	0),
            (13,	1,	None)
            ],
            ['AGE' , 'HBA1C_TT_DENOM', 'BP_TT_DENOM']
        )

        return_df = (input_df.withColumn('TT_3_NEW_DENOMINATOR', tt_3_new_denom))
        expected = [1,0,0,1,1,0,0,0,0]
        actual = [row['TT_3_NEW_DENOMINATOR'] for row in return_df.collect()]
        assert actual == expected, f"When checking TT 3 New denominator, expected to find {expected} but found {actual}."


class TestNewlyDiagnosed(object):
    """
    newly_diagnosed = (
    F.when(
        (F.col('DIAGNOSIS_YEAR') == NEWLY_DIAGNOSED_YEAR) & 
        (F.col('NHS_NUMBER').isNotNull()),
        1).otherwise(0)
    )   
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_nd_flag(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
            (None,	'123456789'),
            (2019,	None),
            (2018,	'123456789'),
            (2020,	'123456789'),
            (2019,	'123456789')
            ],
            ['DIAGNOSIS_YEAR' , 'NHS_NUMBER']
        )
        NEWLY_DIAGNOSED_YEAR = 2019
        return_df = (input_df.withColumn('NEWLY_DIAGNOSED', newly_diagnosed))
        expected = [0, 0, 0, 0, 1]
        actual = [row['NEWLY_DIAGNOSED'] for row in return_df.collect()]
        assert actual == expected, f"When checking SE Newly Diagnosed, expected to find {expected} but found {actual}."


class TestOffered12Month(object):
    """
    offered_12_month = (
    F.when( 
        (F.col('NEWLY_DIAGNOSED') == 1) &
        (F.datediff('ED_OFFER_DATE','DIAGNOSIS_DATE') < 366), 
        1).otherwise(0)
    )  
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_nd_12_month_offered(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
            (1,	'2019-01-01', '2019-01-01'),
            (1,	'2019-01-01', '2019-12-31'),
            (1,	'2019-01-01', '2020-12-01'),
            (1,	'2019-01-01', '2020-01-02'),
            (1,	None, '2020-01-02'),
            (1,	'2019-01-01',	None),
            (0,	'2019-01-01',	'2020-01-02')
            ],
            ['NEWLY_DIAGNOSED' , 'DIAGNOSIS_DATE', 'ED_OFFER_DATE']
        )

        return_df = (input_df.withColumn('ND_OFFERED_12_M', offered_12_month))
        expected = [1, 1, 0, 0, 0, 0, 0]
        actual = [row['ND_OFFERED_12_M'] for row in return_df.collect()]
        assert actual == expected, f"When checking SE offered 12 month, expected to find {expected} but found {actual}."


class TestAttended12Month(object):
    """
    attended_12_month = (
    F.when(
        (F.col('NEWLY_DIAGNOSED') == 1) &
        (F.datediff('ED_ATTEND_DATE','DIAGNOSIS_DATE') < 366),
        1).otherwise(0)
    )  
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_nd_12_month_attended(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
            (1,	'2019-01-01', '2019-01-01'),
            (1,	'2019-01-01', '2019-12-31'),
            (1,	'2019-01-01', '2020-12-01'),
            (1,	'2019-01-01', '2020-01-02'),
            (1,	None, '2020-01-02'),
            (1,	'2019-01-01',	None),
            (0,	'2019-01-01',	'2020-01-02')
            ],
            ['NEWLY_DIAGNOSED' , 'DIAGNOSIS_DATE', 'ED_ATTEND_DATE']
        )

        return_df = (input_df.withColumn('ND_ATTENDED_12_M', attended_12_month))
        expected = [1, 1, 0, 0, 0, 0, 0]
        actual = [row['ND_ATTENDED_12_M'] for row in return_df.collect()]
        assert actual == expected, f"When checking SE attended 12 month, expected to find {expected} but found {actual}."


class TestOfferedEver(object):
    """
    offered_ever = (
    F.when(
        (F.col('NEWLY_DIAGNOSED') == 1) &
        (F.datediff('ED_OFFER_DATE','DIAGNOSIS_DATE') < 100000),
        1).otherwise(0)
    ) 
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_nd_12_month_offered(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
            (1,	'2019-01-01', '2010-01-01'),
            (1,	'2019-01-01', '1908-12-31'),
            (1,	'2019-01-01', '2020-12-02'),
            (1,	None, '2020-01-02'),
            (1,	'2019-01-01',	None),
            (0,	'2019-01-01',	'2020-01-02')
            ],
            ['NEWLY_DIAGNOSED' , 'DIAGNOSIS_DATE', 'ED_OFFER_DATE']
        )

        return_df = (input_df.withColumn('ND_OFFERED_EVER', offered_ever))
        expected = [1, 1, 1, 0, 0, 0]
        actual = [row['ND_OFFERED_EVER'] for row in return_df.collect()]
        assert actual == expected, f"When checking SE offered ever, expected to find {expected} but found {actual}."


class TestAttendedEver(object):
    """
    attended_ever = (
    F.when(
        (F.col('NEWLY_DIAGNOSED') == 1) &
        (F.datediff('ED_ATTEND_DATE','DIAGNOSIS_DATE') < 100000),
        1).otherwise(0)
    ) 
    """
    @pytest.mark.usefixtures("spark_session")
    def test_returns_correct_nd_12_month_attended(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
            (1,	'2019-01-01', '2010-01-01'),
            (1,	'2019-01-01', '1908-12-31'),
            (1,	'2019-01-01', '2020-12-02'),
            (1,	None, '2020-01-02'),
            (1,	'2019-01-01',	None),
            (0,	'2019-01-01',	'2020-01-02')
            ],
            ['NEWLY_DIAGNOSED' , 'DIAGNOSIS_DATE', 'ED_ATTEND_DATE']
        )

        return_df = (input_df.withColumn('ND_ATTENDED_EVER', attended_ever))
        expected = [1, 1, 1, 0, 0, 0]
        actual = [row['ND_ATTENDED_EVER'] for row in return_df.collect()]
        assert actual == expected, f"When checking SE attended ever, expected to find {expected} but found {actual}."
