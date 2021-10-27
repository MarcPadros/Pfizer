#Author: Roel Elbers Drs.
#email: r.j.h.elbers@umcutrecht.nl
#Organisation: UMC Utrecht, Utrecht, The Netherlands
#Date: 15/07/2021


############################################################################################
#Get cdm_source file name
cdm_source_file<-list.files(path_dir, pattern="^CDM_SOURCE")
#Get DAP info and date createion fro CDM_SOURCE
CDM_SOURCE<-fread(paste0(path_dir, cdm_source_file))
date_creation<-CDM_SOURCE[,date_creation]
data_access_provider_name<-CDM_SOURCE[,data_access_provider_name]
data_source_name<-CDM_SOURCE[,data_source_name]
recommended_end_date <- as.Date(as.character(CDM_SOURCE$recommended_end_date),"%Y%m%d")
rm(CDM_SOURCE, cdm_source_file)
############################################################################################

#########################################################
#date transformations
#######################################################
start_study_date <- as.Date(start_study_date,"%Y%m%d")
end_study_date <- as.Date(end_study_date,"%Y%m%d")
date_creation<-as.Date(end_study_date,"%Y%m%d")
intv <- as.Date(c(start_study_date, end_study_date))
##########################################################
print("Check date creation and recommend_end_date. If prior to end_study_date change end_study_date accordingly")
end_study_date <- min(end_study_date,date_creation,recommended_end_date,na.rm = T)

start_study_date2 <- paste0(year(start_study_date),sprintf("%02d",month(start_study_date)),sprintf("%02d",day(start_study_date)))
end_study_date2 <- paste0(year(end_study_date),sprintf("%02d",month(end_study_date)),sprintf("%02d",day(end_study_date)))



FirstExclusion <- list(
  No_observation_time = expression(!is.na(num_spell) & !is.na(op_start_date) & !is.na(op_end_date)),
  No_op_start_date = expression(!is.na(op_start_date)),
  No_year_of_birth = expression(!is.na(year_of_birth)),
  No_year_of_death = expression(!(is.na(year_of_death) & (!is.na(day_of_death) | !is.na(month_of_death)))),
  OP_START_DATE_before_OP_END_DATE = expression(op_start_date < op_end_date),
  Study_Period_and_spell_overlap  = expression(op_start_date %between% intv| op_end_date %between% intv | (op_start_date  < start_study_date & op_end_date > end_study_date)),
  Spells_less_then_lookback_period = expression(op_end_date - op_start_date > lookback_period),
  Remaning_time_to_end_study_date_less_then_lookback_period = expression(end_study_date - op_start_date > lookback_period)
  
)


#metadata_file<-list.files(path_dir, pattern="^METADATA")
#METADATA<-fread(paste0(path_dir, metadata_file))


