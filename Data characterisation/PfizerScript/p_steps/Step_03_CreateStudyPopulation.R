
#Author: Roel Elbers MSc.
#email: r.j.h.elbers@umcutrecht.nl
#Organisation: UMC Utrecht, Utrecht, The Netherlands
#Date: 15/07/2021



FlowChartSourcetoStudy <- list()

SOURCE <- readRDS(paste0(tmp, "source_population.rds"))
    
print('Exclude subjects according to SelectionCriteria specified in the object FirstExclusion. FlowChartSourcetoStudy is writen to g_intermediate/tmp for review.')
    
    
for (j in 1:length(FirstExclusion)){
  
  before <- nrow(SOURCE)
  SOURCE <- SOURCE[eval(FirstExclusion[[j]]),]
  after <- nrow(SOURCE)
  
  FlowChartSourcetoStudy[[paste("Step_",j)]]$step <- names(FirstExclusion)[j]
  FlowChartSourcetoStudy[[paste("Step_",j)]]$before <- before
  FlowChartSourcetoStudy[[paste("Step_",j)]]$after <- after
  
  rm(before,after)
  gc()
} 


FlowChartSourcetoStudy <- as.data.table(do.call(rbind,FlowChartSourcetoStudy))

print('Delete abundant columns and tables')  
lapply(c("day_of_death","month_of_death","year_of_death","day_of_birth","month_of_birth","year_of_birth"), function (x) SOURCE <- SOURCE[,eval(x) := NULL])


print("Set start_follow up date and end follow_up_date ")
#STUDY_POPULATION <- SOURCE[,start_follow_up := max(start_study_date,op_start_date + lookback_period),by = list(row.names(SOURCE))]
STUDY_POPULATION <- SOURCE[,start_follow_up := max(start_study_date,op_start_date),by = list(row.names(SOURCE))]
STUDY_POPULATION <- STUDY_POPULATION[,end_follow_up := min(end_study_date,op_end_date,date_creation,recommended_end_date),by = list(row.names(SOURCE))]

#aatest <- STUDY_POPULATION[1:100,]

rm(SOURCE)
gc()

before <- nrow(STUDY_POPULATION)
STUDY_POPULATION <- STUDY_POPULATION[start_follow_up < end_follow_up ,]
#STUDY_POPULATION <- STUDY_POPULATION[(start_follow_up - op_start_date) >= lookback_period ,]
after <- nrow(STUDY_POPULATION)

if(after != before) print("Errors in start_follow_up and/or end_follow_up")
rm(before,after)


#mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))

#dbWriteTable(mydb, "M_Studycohort",STUDY_POPULATION,overwrite = F, append = F)
#dbDisconnect(mydb)


saveRDS(STUDY_POPULATION,file = paste0(populations_dir,"M_Studycohort.rds"))
saveRDS(FlowChartSourcetoStudy,file = paste0(tmp,"FlowChartSourcetoStudy.rds"))
rm(STUDY_POPULATION, FlowChartSourcetoStudy)
gc()

























