params ={
    'publication_year':'202021',
    'publication_version':'E3',
    'publication_start_date':'2020-01-01',
    'publication_end_date':'2020-12-31',
    'newly_diagnosed_year': '2019',
    'nda_demo_table': {'database':'NDA_DPP_DMS', 'table':'lve.NDA_DEMO_E3_202021'},
    'nda_bmi_table': {'database':'NDA_DPP_DMS', 'table':'lve.NDA_BMI_E3_202021'},
    'nda_bp_table': {'database':'NDA_DPP_DMS', 'table':'lve.NDA_BP_E3_202021'},
    'nda_chol_table': {'database':'NDA_DPP_DMS', 'table':'lve.NDA_CHOL_E3_202021'},
    'nda_hba1c_table': {'database':'NDA_DPP_DMS', 'table':'lve.NDA_HBA1C_E3_202021'},
    'nda_drug_table': {'database':'NDA_DPP_DMS', 'table':'lve.NDA_DRUG_E3_202021'},
    'ccg_map': {'database':'CASU_DIABETES', 'table':'core_map.LATEST_GP_CCG_map'},
    'hes_diabetes_table': {'database':'CASU_DIABETES', 'table':'dbo.HES_Diabetes_comps_1011to1920'},
    'imd_scores':{'database':'CASU_DIABETES', 'table':'ref.Deprivation_combined_EW_2019'},
    'data_size':'full', # only run the 'full' in 'RAP_TEMP' or 'lite' in  the individual 'RAP_TEMP_**'
    'work_db':'RAP_TEMP',
}
