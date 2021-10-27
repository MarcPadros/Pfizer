

sheet1  <- as.data.table(colnames(readRDS(paste0(populations_dir,"M_Studycohort.rds"))))[, table := "M_Studycohort"]
setnames(sheet1, "V1", "Column_name")

sheet1  <- sheet1[Column_name == "person_id", CDM_Table := "ALL"]
sheet1  <- sheet1[Column_name == "sex_at_instance_creation", CDM_Table := "PERSONS"]

sheet1  <- sheet1[Column_name == "op_start_date", CDM_Table := "OBSERVATION_PERIODS"]
sheet1  <- sheet1[Column_name == "op_end_date", CDM_Table := "OBSERVATION_PERIODS"]

sheet1  <- sheet1[Column_name == "num_spell", MANIPULATION := "Result of procedure to select op_start_date and op_end_date representing the number of periods that their whereinnthe database"]

sheet1  <- sheet1[Column_name == "op_start_date", MANIPULATION := "Merge overlapping periods (op_end_date - op_start_date) or periods with less then 20 days gab -> Select latests"]
sheet1  <- sheet1[Column_name == "op_end_date", MANIPULATION := sheet1[Column_name == "op_start_date",.(MANIPULATION)]]


sheet1  <- sheet1[Column_name == "birth_date" , CDM_Table := "PERSONS"]
sheet1  <- sheet1[Column_name == "death_date" , CDM_Table := "PERSONS"]
sheet1  <- sheet1[Column_name == "birth_date" , MANIPULATION := "concatenate(year_of_birth,month_of_birth,day_of_birth) If missing month_of_birth INPUTE 6 if missing day_of_birth INPUTE 16   "]
sheet1  <- sheet1[Column_name == "death_date" , MANIPULATION := "concatenate(year_of_death,month_of_death,day_of_death) If missing month_of_death INPUTE 6 if missing day_of_death INPUTE 16   "]


sheet1  <- sheet1[Column_name %in% c("FIRST_OTHER","FIRST_PFIZER","SECOND_OTHER","SECOND_PFIZER","LAST_INFL_VAC" ) , CDM_Table := "VACCINES"]
sheet1  <- sheet1[Column_name == "FIRST_COV_INF" , CDM_Table := "EVENTS"]

sheet1  <- sheet1[Column_name %in% c("start_follow_up","end_follow_up","op_start_date","op_end_date","birth_date","death_date","op_start_date","op_end_date","FIRST_OTHER","FIRST_PFIZER","SECOND_OTHER","SECOND_PFIZER","LAST_INFL_VAC","FIRST_COV_INF" ) , FORMAT := "DATE"]
sheet1  <- sheet1[Column_name %in% c("num_spell") , FORMAT := "NUMERIC"]
sheet1  <- sheet1[is.na(FORMAT) , FORMAT := "CHAR"]

sheet1  <- sheet1[Column_name %in% c("FIRST_OTHER"),  MANIPULATION := "First covid vx_admin_date by person_id -> select cases with vx_admin_date between op_start_date and op_end_date and not from PFIZER in column vx_manufacturer"]
sheet1  <- sheet1[Column_name %in% c("SECOND_OTHER"),  MANIPULATION := "Second covid vx_admin_date by person_id -> select cases with vx_admin_date between op_start_date and op_end_date and not from PFIZER in column vx_manufacturer"]
sheet1  <- sheet1[Column_name %in% c("FIRST_PFIZER"),  MANIPULATION := "First covid vx_admin_date by person_id -> select cases with vx_admin_date between op_start_date and op_end_date and from PFIZER in column vx_manufacturer"]
sheet1  <- sheet1[Column_name %in% c("SECOND_PFIZER"),  MANIPULATION := "Second covid vx_admin_date by person_id -> select cases with vx_admin_date between op_start_date and op_end_date and from PFIZER in column vx_manufacturer"]

sheet1  <- sheet1[Column_name %in% c("LAST_INFL_VAC"),  MANIPULATION := "Last influenza vx_admin_date by person_id -> select cases with vx_admin_date between op_start_date and op_end_date"]
sheet1  <- sheet1[Column_name %in% c("FIRST_COV_INF"),  MANIPULATION := "Firs covid start_date_record by person_id -> select cases with vx_admin_date between op_start_date and op_end_date"]


sheet2  <- as.data.table(colnames(readRDS(paste0(populations_dir,"NB_Diagnoses.rds"))))[, table := "NB_Diagnoses"]
setnames(sheet2, "V1", "Column_name")

sheet2  <- sheet2[Column_name == "rn" , CDM_Table := "ALL"]
sheet2  <- sheet2[Column_name == "rn" , Column_name := "person_id"]
sheet2  <- sheet2[Column_name == "month" , CDM_Table := "NOT FROM CDM"]
sheet2  <- sheet2[Column_name == "SUM_year" , CDM_Table := "EVENTS"]

sheet2  <- sheet2[Column_name %in% c("month"),  MANIPULATION := "ALL month year combination from start_study_date to end_study_date"]
sheet2  <- sheet2[Column_name %in% c("SUM_year"),  MANIPULATION := "Cumulative sum of the sum of diagnosis by person_id and by month 12 months prior"]



sheet2  <- sheet2[Column_name %in% c("SUM_year") , FORMAT := "NUMERIC"]
sheet2  <- sheet2[is.na(FORMAT) , FORMAT := "CHAR"]

#install.packages("writexl")
library(writexl)


write_xlsx(list("M_Studycohort" = sheet1, "NB_Diagnoses" = sheet2  ), "H:/codebook.xlsx")


#sheet2$Column_name
