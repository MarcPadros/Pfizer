

M_Studycohort <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
NB_Diagnoses <- readRDS(paste0(populations_dir,"NB_Diagnoses.rds"))

days <- seq(as.Date("19500101", "%Y%m%d"),as.Date("20200413", "%Y%m%d"),1)
sample(days,nrow(M_Studycohort), replace = T)

aatest <- M_Studycohort
aatest[["birth_date"]] <- sample(days,nrow(M_Studycohort), replace = T)
aatest <- aatest[!is.na(FIRST_OTHER), FIRST_PFIZER := FIRST_OTHER ]
aatest <- aatest[!is.na(FIRST_OTHER), FIRST_OTHER := NA ]
M_Studycohort <- aatest




FILE_EXPOSED <- copy(M_Studycohort)[!is.na(FIRST_PFIZER),][,.(person_id,sex_at_instance_creation,FIRST_PFIZER,FIRST_OTHER,birth_date)][, birth_date := year(birth_date)]
FILE_CONTROL <- copy(M_Studycohort)[,.(person_id,sex_at_instance_creation,FIRST_PFIZER,FIRST_OTHER,birth_date)][, birth_date := year(birth_date)]

comb <- as.data.table(unique(expand.grid(unique(FILE_EXPOSED[["sex_at_instance_creation"]]),unique(FILE_EXPOSED[["FIRST_PFIZER"]]),unique(FILE_EXPOSED[["birth_date"]])), stringsAsFactors = F))
colnames(comb) <- c("sex_at_instance_creation","FIRST_PFIZER","birth_date")
comb <- comb[,sex_at_instance_creation := as.character(sex_at_instance_creation)]
comb <- comb[,sex_at_instance_creation := as.character(sex_at_instance_creation)]

setorder(comb,FIRST_PFIZER,birth_date)

#i = 94

k = 0

system.time(for(i in 1:nrow(comb)){

Exposed <- FILE_EXPOSED[sex_at_instance_creation == comb[["sex_at_instance_creation"]][i] & FIRST_PFIZER == comb[["FIRST_PFIZER"]][i] & birth_date == comb[["birth_date"]][i],.(person_id)][, id := i][,t0 := comb[["FIRST_PFIZER"]][i]] 

if(i == 1){
  Exposed_Total <- Exposed[0]
  Controls_Total <- Exposed[0]
}

if(nrow(Exposed) == 0){
  rm(Exposed)
  }else{

        Controls <- FILE_CONTROL[
                        sex_at_instance_creation == comb[["sex_at_instance_creation"]][i] & 
                      (FIRST_PFIZER > comb[["FIRST_PFIZER"]][i] | FIRST_OTHER > comb[["FIRST_PFIZER"]][i] | (is.na(FIRST_PFIZER) & is.na(FIRST_OTHER))) & 
                        birth_date %between% list(comb[["birth_date"]][i] - 1,  comb[["birth_date"]][i] + 1)
                      ,.(person_id)][, id := i][,t0 := comb[["FIRST_PFIZER"]][i]]
        
        
        Exposed_Total <-  rbindlist(list(Exposed, Exposed_Total))
        Controls_Total <-  rbindlist(list(Controls, Controls_Total))
        
        #print(i)
        k = k + 1
        #print(k)
        
        rm(Controls,Exposed)
        gc()
  }
        
})


Exposed_Total[,t0_month := paste0(sprintf("%02d",month(t0)),"-",year(t0))]
Controls_Total[,t0_month := paste0(sprintf("%02d",month(t0)),"-",year(t0))]

Exposed_Total2 <- merge(x = Exposed_Total, y = NB_Diagnoses, by.x = c("person_id","t0_month"), by.y = c("rn","month"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := 0]
Controls_Total2 <- merge(x = Controls_Total, y = NB_Diagnoses, by.x = c("person_id","t0_month"), by.y = c("rn","month"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := 0]


#i = 3715
#j = "ConCDM_SIM_200421_00002_986"


k=1
system.time(for(i in unique(Exposed_Total2[["id"]])){
  #print(i)
  TEMP <- unique(merge(x = Exposed_Total2[id == i,], y = Controls_Total2[id == i,], by = c("SUM_year","id"), allow.cartesian = T, all.x = T))
  if(k == 1)MATCHED <- TEMP[0][,.(person_id.x,person_id.y)]
  k = k + 1
  #print(k)
  #TEMP <- TEMP[person_id.x == "ConCDM_SIM_200421_00002_1",]
  #rm(TEMP)
  
  MATCHED <- rbindlist(list(MATCHED ,TEMP[sample(.N), c(.SD[1], .N), keyby = person_id.x][,.(person_id.x,person_id.y)]))
  
  rm(TEMP)
  gc()
}
)











