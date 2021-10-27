


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

#Test matching with join in database environment since in R it crashes
#mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))
#dbWriteTable(mydb, "M_Studycohort",study_population, overwrite = F, append = F)
#p <- dbSendStatement(mydb, paste0("CREATE INDEX M_Studycohort_index ON M_Studycohort(sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH,month_t0,person_id)"))
#dbClearResult(p)

rm(COVID,study_population)

#####################################################

#start_study_date <- (start_study_date - (3*366))


AESI <- unique(readRDS(paste0(concepts_dir,"AESI.rds"))[,.(person_id, start_date_record,sheet)])[, month := month(start_date_record)][, year := year(start_date_record)]
AESI <- AESI[,label := paste0(sprintf("%02d",month),"-",year)]


comb <- as.data.table(expand.grid(1:12,c((year(start_study_date)-1):year(end_study_date))))[,label := paste0(sprintf("%02d",Var1),"-",Var2)]
comb <- comb[, order := as.numeric(row.names(comb))]
comb <- comb[Var1 == month(start_study_date) & Var2 == year(start_study_date), TIME := 0]
comb <- comb[, TIME2 := order - comb[TIME == 0, order]]
comb <- comb[TIME2 > -13, ]

AESI2 <- merge(x = comb[,.(label,TIME2)], y = AESI, by = "label", all.x = T)
AESI2 <- AESI2[, .(NB = sum(!is.na(start_date_record))), by = c("person_id", "label", "TIME2")]
setorder(AESI2,TIME2)
AESI3 <- as.matrix(dcast(AESI2, person_id  ~  TIME2, value.var = "NB")[!is.na(person_id),], rownames = "person_id")

AESI3[is.na(AESI3)] <- 0

TEMP <- matrix(NA,nrow = nrow(AESI3), ncol = ncol(AESI3) - 12)
colnames(TEMP) <- comb[TIME2 >= 0,label]
rownames(TEMP) <- row.names(AESI3)

system.time(for(i in 13:max(as.numeric(ncol(AESI3)))) TEMP[,i-12] <- rowSums(AESI3[, (i-12):(i-1)])) 

# system.time(for(i in 13:max(as.numeric(ncol(AESI3)))){
#   
#   if(i == 13) {
#     TEMP2 <- as.data.table(rowSums(AESI3[, (i-12):(i-1)]), keep.rownames = T)
#     colnames(TEMP2) <- c("person_id", colnames(AESI3)[i])
#   
#   }
#   
#   if(i > 13) {
#     TEMP3 <- as.data.table(rowSums(AESI3[, (i-12):(i-1)]), keep.rownames = T)
#     colnames(TEMP3) <- c("person_id", colnames(AESI3)[i])
#     TEMP2 <- merge(TEMP2,TEMP3,by = "person_id", all = T)
#     rm(TEMP3)
#     gc()
#     }
# })  

#as.matrix(TEMP2[1:nrow(TEMP2),2:ncol(TEMP2)]) == TEMP

TEMP <- as.data.table(TEMP, keep.rownames = T)
TEMP2 <- melt(TEMP, id.vars = "rn", measure.vars =  colnames(TEMP)[!colnames(TEMP) %in% "rn"], variable.name = "month", value.name = "SUM_year")
TEMP2 <- TEMP2[SUM_year > 0,]


saveRDS(TEMP2,paste0(populations_dir,"NB_Diagnoses.rds"))

####
#dbWriteTable(mydb, "NB_Diagnoses",TEMP2, overwrite = F, append = F)

#p <- dbSendStatement(mydb, paste0("CREATE INDEX NB_Diagnoses_index ON NB_Diagnoses(rn, month)"))
#dbClearResult(p)
#dbDisconnect(mydb)
####


rm(TEMP,TEMP2,comb,AESI,AESI2,AESI3)
gc()



