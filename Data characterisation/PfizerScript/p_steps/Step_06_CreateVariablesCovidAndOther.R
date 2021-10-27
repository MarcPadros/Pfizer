
COVID <- readRDS(paste0(concepts_dir,"COVIDNARROW.rds"))
study_population <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))

before <- nrow(COVID)
COVID <- ConceptTimeSelection(
  ID = "person_id",
  Concept = COVID,
  v.date = "start_date_record",
  population = study_population[,.(person_id, op_start_date, op_end_date)],
  v.obs.start = "op_start_date",
  #t.prior =  1000,
  method = "d.range",
  v.obs.end = "op_end_date",
  join = "inner"
)

after <- nrow(COVID)
print(paste0(before - after," covid infection dates removed because they do not fall between op_start_date and op_end_date or because the person is not in M_cohort"))
rm(before,after)



print("Select first covid infection and add this to M_Studycohort")
setorder(COVID, person_id, start_date_record)
COVID <- COVID[,NB := seq_len(.N) , by = person_id][NB == 1,]
setnames(COVID,"start_date_record","FIRST_COV_INF")

#study_population <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
study_population <- merge(x = study_population, y = COVID[,.(person_id,FIRST_COV_INF)], by = "person_id", all.x = T, all.y = F, allow.cartesian = F)
study_population <- study_population[,YEAR_BIRTH := year(birth_date)]
study_population <- study_population[!is.na(FIRST_PFIZER),month_t0 := paste0(sprintf("%02d",month(FIRST_PFIZER)),"-",year(FIRST_PFIZER))]


saveRDS(study_population,paste0(populations_dir,"M_Studycohort.rds"))

rm(COVID, study_population)
gc()


