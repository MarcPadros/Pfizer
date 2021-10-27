

M_Studycohort <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
HIST <- readRDS(paste0(populations_dir,"HIST.rds"))

FILE_EXPOSED <- copy(M_Studycohort)[!is.na(FIRST_PFIZER),][,.(person_id, sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH, month_t0)]
FILE_CONTROL <- copy(M_Studycohort)[,.(person_id,sex_at_instance_creation,FIRST_PFIZER,FIRST_OTHER,YEAR_BIRTH)]
FILE_EXPOSED[duplicated(FILE_EXPOSED[,.(sex_at_instance_creation,FIRST_PFIZER,YEAR_BIRTH)]),]
comb <- unique(FILE_EXPOSED[,.(sex_at_instance_creation,FIRST_PFIZER,YEAR_BIRTH)])

#x <- paste0(FILE_EXPOSED[["sex_at_instance_creation"]],"|",FILE_EXPOSED[["FIRST_PFIZER"]],"|", FILE_EXPOSED[["YEAR_BIRTH"]])
#comb <- unique(as.data.table(do.call(rbind ,strsplit(x, "|", fixed = T))))
#colnames(comb) <- c("sex_at_instance_creation","FIRST_PFIZER","YEAR_BIRTH")
#comb <-comb[, YEAR_BIRTH := as.numeric(YEAR_BIRTH)]
#comb <-comb[, FIRST_PFIZER := as.Date(FIRST_PFIZER)]

setorder(comb,FIRST_PFIZER,YEAR_BIRTH)

i = 1

k = 0
nb_exposed <- nrow(FILE_EXPOSED)

system.time(for(i in 1:nrow(comb)){

Exposed <- FILE_EXPOSED[
                      sex_at_instance_creation == comb[["sex_at_instance_creation"]][i] & 
                      FIRST_PFIZER == comb[["FIRST_PFIZER"]][i] & 
                      YEAR_BIRTH == comb[["YEAR_BIRTH"]][i],.(person_id)][, id := i][,t0 := comb[["FIRST_PFIZER"]][i]] 

setnames(Exposed, "person_id", "Exposed")


if(nrow(Exposed) == 0){
  rm(Exposed)
  }else{

        Controls <- FILE_CONTROL[
                        sex_at_instance_creation == comb[["sex_at_instance_creation"]][i] & 
                      (FIRST_PFIZER > comb[["FIRST_PFIZER"]][i] | FIRST_OTHER > comb[["FIRST_PFIZER"]][i] | (is.na(FIRST_PFIZER) & is.na(FIRST_OTHER))) & 
                        YEAR_BIRTH %between% list(comb[["YEAR_BIRTH"]][i] - 1,  comb[["YEAR_BIRTH"]][i] + 1)
                      ,.(person_id)][, id := i][,t0 := comb[["FIRST_PFIZER"]][i]]
        setnames(Controls, "person_id", "Control")
        
        Exposed[,t0_month := paste0(sprintf("%02d",month(t0)),"-",year(t0))]
        Controls[,t0_month := paste0(sprintf("%02d",month(t0)),"-",year(t0))]
        
        Exposed <- merge(x = Exposed, y = HIST, by.x = c("Exposed","t0_month"), by.y = c("person_id","month"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := F][is.na(FIRST_COV_INF), FIRST_COV_INF := F]
        Controls <- merge(x = Controls, y = HIST, by.x = c("Control","t0_month"), by.y = c("person_id","month"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := F][is.na(FIRST_COV_INF), FIRST_COV_INF := F]
        
        TEMP <- unique(merge(x = Exposed[id == i,], y = Controls[id == i,], by = c("id","SUM_year","FIRST_COV_INF"), allow.cartesian = T, all.x = T))
        if(i == 1) MATCHED <- TEMP[0][,.(Exposed,Control)]
        
        MATCHED <- rbindlist(list(MATCHED , TEMP[sample(.N), c(.SD[1], .N), keyby = Exposed][,.(Exposed, Control)]))
        
        #print(i)
        k = k + nrow(Exposed)
        print(paste0(k, " matched from ",nb_exposed))
        
        rm(Controls,Exposed, TEMP)
        gc()
  }
        
}
)



MATCHED <- MATCHED[, id := row.names(MATCHED)]
MATCHED <- merge(MATCHED, FILE_EXPOSED[,.(person_id, FIRST_PFIZER)], by.x = "Exposed", by.y = "person_id", all.x = T)
setnames(MATCHED, "FIRST_PFIZER", "T0")

saveRDS(MATCHED,paste0(populations_dir,"MATCH_PAIRS.rds"))

rm(FILE_CONTROL, FILE_EXPOSED, comb, MATCHED,HIST)










