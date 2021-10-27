
#Author:Roel Elbers MSc.
#email: r.j.h.elbers@umcutrecht.nl
#Organisation: UMC Utrecht, Utrecht, The Netherlands
#Date: 26/07/2021

rm(list=ls())
if(!require(rstudioapi)){install.packages("rstudioapi")}
library(rstudioapi)

projectFolder<-dirname(rstudioapi::getSourceEditorContext()$path)
#setwd(projectFolder)
#StudyName <- "BIG"
StudyName <- "Pfizer"
#StudyName <- "Medium"

###################################################
#Parameters
#################################################
#females age
#min_age_preg<-12
#max_age_preg<-55
#Set parameters basic parameters


#Start Defined as launch of the vaccin in relevant country (SAP) (Date creation?)
start_study_date <- "20210301"

#Date of last vaccinated subject according to SAP, then check CDM_Source if recommend end date is earlier
end_study_date <- "20221231"

lookback_period <- 365
max_spells_gap <- 20
#Age_min <- 0
#Age_max <- 120 ##??


source(paste0(projectFolder,"/packages.R"))
source(paste0(projectFolder,"/99_path.R"))

#source(paste0(pre_dir, "info.R"))
#setwd(projectFolder)


#Load functions
source(paste0(pre_dir,"functions/", "CreateSpells_v_10.R"))
source(paste0(pre_dir,"functions/", "CountPersonTimeV12.5.R"))
source(paste0(pre_dir,"functions/", "CountPersonTimeV13.4.R"))
source(paste0(pre_dir,"functions/", "ConceptsTimingSelection_V1.R"))
source(paste0(pre_dir,"functions/", "FUNCTIONS.R"))
source(paste0(pre_dir,"functions/", "FUNCTIONS_SQLite.R"))
source(paste0(pre_dir,"functions/", "CreateConcepts_V2.R"))



#Set parameters
source(paste0(pre_dir,"Step_00_SetParameters.R"))
#if(file.exists(paste0(tmp,"database.db"))) file.remove(paste0(tmp,"database.db"))

#Preparation of analyses input tables to generate the main table M_Studycohort 
system.time(source(paste0(pre_dir,"Step_01_CreateSpells.R")))
system.time(source(paste0(pre_dir,"Step_02_CreateSourceTableAndInputeDates.R")))
system.time(source(paste0(pre_dir,"Step_03_CreateStudyPopulation.R")))

#Create a R database 
system.time(source(paste0(pre_dir,"Step_04_RDSDatabaseMatching.R")))
system.time(source(paste0(pre_dir,"Step_04_RDSDatabaseMatchingLoop.R")))
system.time(source(paste0(pre_dir,"Step_05a_CreateVariablesMatchingVaccins.R")))
system.time(source(paste0(pre_dir,"Step_05b_CreateVariablesMatchingAESI.R")))


#Create a SQL lite database with indexes 
system.time(source(paste0(pre_dir,"Step_04_SetSQLiteDatabaseMatching.R")))


#Create variables that are needed to generate the Cohorts 

system.time(source(paste0(pre_dir,"Step_05a_CreateVariablesMatchingVaccinsSQLite.R")))
system.time(source(paste0(pre_dir,"Step_05b_CreateVariablesMatchingAESISQLite.R")))

system.time(source(paste0(pre_dir,"Step_04b_SetSQLiteDatabaseConcepts.R")))
system.time(source(paste0(pre_dir,"Step_04b_SetSQLiteDatabaseConceptParallel.R")))


#Matching

system.time(source(paste0(pre_dir,"06_test_matching_loop2.R")))
#system.time(source(paste0(pre_dir,"06_Prematch.R")))





#source(paste0(pre_dir,"Step_06_Timing_Prior_Matching.R"))
#source(paste0(pre_dir,"Step_07_Matching_create_two_datasets.R"))
#source(paste0(pre_dir,"Step_08_SelectEventsPerCohort.R"))





