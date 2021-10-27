



#install.packages("rollmatch")
#library(rollmatch)

#install.packages("MatchIt")
#library("MatchIt")

study_population <- readRDS(paste0(populations_dir,"ALL_study_population.rds"))[,.(person_id,sex_at_instance_creation,birth_date)]

VAC <- readRDS(paste0(vaccins_dir,"VAC_COVID19_T.rds"))
setorder(VAC,person_id,Date)

VAC <- VAC[,NB := seq_len(.N) , by = person_id][NB == 1] 

#& medicinal_product_id == "184284",]

TEMP <- merge(x = study_population, y = VAC[,.(person_id,Date,medicinal_product_id)], by = "person_id", all.x = T)

TEMP <- TEMP[!is.na(Date), Age_vac := floor(time_length(interval(birth_date, Date),"year")),]
#TEMP <- TEMP[is.na(NB),NB := 0]

FIRST_VAC <- min(TEMP[["Date"]], na.rm = T)
TEMP <- TEMP[, Time := as.integer(Date - FIRST_VAC)]

LAST_VAC <- max(TEMP[["Time"]], na.rm = T)
TEMP <- TEMP[is.na(Time),Time := as.integer(1 + LAST_VAC)]
TEMP <- TEMP[,TREAT := fifelse(is.na(Date),0,1)]


setorder(TEMP,Time)

TEMP2 <- copy(TEMP)
Exposed <- vector()
Control <- vector()

i = 1
k= 1
while(i == 1){


Exposed[k] <- TEMP2[1, "person_id" ]
Control[k] <- TEMP2[sample(1+1:nrow(TEMP2),1), "person_id"]

#TEMP3 <- TEMP2[sex_at_instance_creation == TEMP2[1,sex_at_instance_creation] & person_id != Exposed[k],]
#Control[k] <- TEMP3[sample(1:nrow(TEMP3),1), person_id]
#rm(TEMP3)
#gc()

TEMP2 <- TEMP2[!person_id %in% c(Exposed[k],Control[k]),]
#TEMP2 <- TEMP2[!person_id %in% c(Exposed[k],Control[k]),]

if(sum(TEMP2[["TREAT"]]) == 0) i = 0
k <- k + 1

}

rm(study_population,TEMP,TEMP2)


Exposed <- unlist(Exposed)
Control <- unlist(Control)   
MATCH <- as.data.table(cbind.data.frame(Exposed, Control))[, match_id := seq_len(.N)]
MATCH <-merge(x = MATCH, y = VAC, all.y = F, by.x = "Exposed", by.y = "person_id")


study_population <- readRDS(paste0(populations_dir,"ALL_study_population.rds"))[,.(person_id,sex_at_instance_creation,birth_date, start_follow_up, end_follow_up)]


study_population2 <- merge(x = MATCH[,.(match_id,Control,Exposed,Date, medicinal_product_id)], y = study_population[,.(person_id, start_follow_up, end_follow_up) ]  , all.x = T, all.y = F, by.x = "Exposed", by.y = "person_id")
setnames(study_population2, c("start_follow_up", "end_follow_up"), c("start_exposed", "end_exposed"))

study_population2 <- merge(x = study_population2, y = study_population[,.(person_id, start_follow_up, end_follow_up) ]  , all.x = T, all.y = F, by.x = "Control", by.y = "person_id")
setnames(study_population2, c("start_follow_up", "end_follow_up"), c("start_control", "end_control"))

setnames(VAC,"Date","DateControl")
study_population2 <- merge(x = study_population2, y = VAC[,.(person_id,DateControl)], all.x = T, all.y = F, by.x = "Control", by.y = "person_id")

study_population2 <- study_population2[DateControl < Date, check := "Error"]

study_population2 <- study_population2[, start_follow_up_exp := Date]
study_population2 <- study_population2[, end_follow_up_exp := min(DateControl,end_exposed,end_control, na.rm = T), by = row.names(study_population2)]
study_population2 <- study_population2[, start_follow_up_con := max(Date, start_control, na.rm = T), by = row.names(study_population2)] #check this
study_population2 <- study_population2[, end_follow_up_con := min(DateControl,end_exposed,end_control, na.rm = T), by = row.names(study_population2)]


Controls <- study_population2[,.(Control, match_id,medicinal_product_id, start_follow_up_con, end_follow_up_con )]
setnames(Controls, c("Control","start_follow_up_con", "end_follow_up_con"), c("person_id","start_follow_up", "end_follow_up"))

Vaccinated <- study_population2[,.(Exposed, match_id,medicinal_product_id, start_follow_up_exp, end_follow_up_exp )]
setnames(Vaccinated, c("Exposed","start_follow_up_exp", "end_follow_up_exp"), c("person_id","start_follow_up", "end_follow_up"))

saveRDS(Vaccinated,paste0(populations_dir,"VACCINATED_COHORT.rds"))
saveRDS(Controls,paste0(populations_dir,"CONTROL_COHORT.rds"))



rm(Vaccinated,VAC,study_population2,MATCH,FILE,Controls)
gc()





