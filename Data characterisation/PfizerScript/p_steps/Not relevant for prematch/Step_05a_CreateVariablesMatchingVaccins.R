
VAC <- readRDS(paste0(vaccins_dir,"VAC_COVID19.rds"))

print("Add column to assign first or second vaccination")
setorder(VAC,person_id, Date)
VAC <- VAC[,NB := seq_len(.N) , by = person_id]
if(max(VAC$NB) > 2) print("Cases with more than 2 covid vaccins are in the data")

print("Join to covid vaccins to cohort on between op_start_date/op_end_date to select the vaccins administered within the window of interrest")
study_population <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))

before <- nrow(VAC)
VAC <- ConceptTimeSelection(
  ID = "person_id",
  Concept = VAC,
  v.date = "Date",
  population = study_population[,.(person_id, op_start_date, op_end_date)],
  v.obs.start = "op_start_date",
  #t.prior =  1000,
  method = "d.range",
  v.obs.end = "op_end_date",
  join = "inner"
)

after <- nrow(VAC)
print(paste0(before - after," covid vaccination dates removed because they do not fall between op_start_date and op_end_date or because the person is not in M_cohort"))
rm(before,after)

print("Reform table to make 4 columns per subject. Pfizer dose 1 date, Pfizer dose 2 date, Other vaccin dose 1 date, and Other vaccin dose 2 date.")
VAC <- VAC[NB < 3,]
VAC <- VAC[, v.man := fifelse(like(vx_manufacturer, "pfizer", ignore.case = T ),"PFIZER","OTHER") ][, var := paste0(NB,"_",v.man)][,.(person_id,var,Date)]
VAC <- dcast(VAC, person_id  ~  var, value.var = "Date")
setnames(VAC,c("1_PFIZER","2_PFIZER","1_OTHER","2_OTHER"),c("FIRST_PFIZER","SECOND_PFIZER","FIRST_OTHER","SECOND_OTHER"))



print("Add new variables to M_Studycohort")
study_population <- merge(x = study_population, y = VAC, by = "person_id", all.x = T, all.y = F, allow.cartesian = F)
saveRDS(study_population,paste0(populations_dir,"M_Studycohort.rds"))
rm(VAC)

print("Join to influenza vaccins to cohort on between op_start_date/op_end_date to select the vaccins administered within the time they were followed??")
INF <- readRDS(paste0(vaccins_dir,"VAC_INFLUENZA.rds"))[,.(person_id,Date)]

before <- nrow(INF)
INF <- INF[Date > start_study_date - (366 * 5),]
after <- nrow(INF)
print(paste0(before - after," influenza vaccination dates removed because they are more then 5 years before start_study_date"))
rm(before,after)

before <- nrow(INF)

INF <- ConceptTimeSelection(
  ID = "person_id",
  Concept = INF,
  v.date = "Date",
  population = study_population[,.(person_id, op_start_date, op_end_date)],
  v.obs.start = "op_start_date",
  #t.prior =  1000,
  method = "d.range",
  v.obs.end = "op_end_date",
  join = "inner"
)

after <- nrow(INF)
print(paste0(before - after," influenza vaccination dates removed because they do not fall between op_start_date and op_end_date"))
rm(before,after)

print("Select most recent influenza vaccination and add this to M_Studycohort")
#setorder(INF, person_id, -Date)
INF <- INF[,NB := seq_len(.N) , by = person_id][,.(person_id,Date,NB)]
#setnames(INF,"vx_admin_date","LAST_INFL_VAC")

saveRDS(INF,paste0(populations_dir,"INFLUENZA.rds"))

rm(study_population,INF)








