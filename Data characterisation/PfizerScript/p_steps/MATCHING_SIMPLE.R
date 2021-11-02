

M_Studycohort <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))

FILE_EXPOSED <- copy(M_Studycohort)[!is.na(FIRST_PFIZER),][,.(person_id, sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH, FIRST_COV_INF)]
FILE_EXPOSED <- FILE_EXPOSED[,FIRST_PFIZER := as.IDate(FIRST_PFIZER)]

FILE_CONTROL <- copy(M_Studycohort)[,.(person_id,sex_at_instance_creation,FIRST_PFIZER,FIRST_OTHER,YEAR_BIRTH,FIRST_COV_INF)]

rm(M_Studycohort)
gc()

FILE_CONTROL <- FILE_CONTROL[, VAC_DATE1 := fifelse(!is.na(FIRST_PFIZER),FIRST_PFIZER,FIRST_OTHER) ][, FIRST_PFIZER := NULL ][, FIRST_OTHER := NULL ]
FILE_CONTROL <- FILE_CONTROL[,VAC_DATE1 := as.IDate(VAC_DATE1)]

Exposed[["FIRST_COV_INF"]] < Exposed[["FIRST_PFIZER"]] 

MATCHED <- matrix(NA, nrow = nrow(FILE_EXPOSED), ncol = 2)

system.time(for(i in 1:nrow(FILE_EXPOSED)){
Exposed <- FILE_EXPOSED[i,]

COV <- Exposed[["FIRST_COV_INF"]] < Exposed[["FIRST_PFIZER"]] 

if(is.na(COV)){
Controls <- FILE_CONTROL[
  sex_at_instance_creation == Exposed[["sex_at_instance_creation"]] & 
    (VAC_DATE1 > Exposed[["FIRST_PFIZER"]] | is.na(VAC_DATE1)) &
    YEAR_BIRTH %between% list(Exposed[["YEAR_BIRTH"]] - 1,  Exposed[["YEAR_BIRTH"]] + 1) &
    is.na(FIRST_COV_INF)
    
  ,][["person_id"]]
}

if(!is.na(COV)){
if(COV){
  Controls <- FILE_CONTROL[
    sex_at_instance_creation == Exposed[["sex_at_instance_creation"]] & 
      (VAC_DATE1 > Exposed[["FIRST_PFIZER"]] | is.na(VAC_DATE1)) &
      YEAR_BIRTH %between% list(Exposed[["YEAR_BIRTH"]] - 1,  Exposed[["YEAR_BIRTH"]] + 1) &
      FIRST_COV_INF < Exposed[["FIRST_PFIZER"]]
    
    ,][["person_id"]]
}

if(!COV){
  Controls <- FILE_CONTROL[
    sex_at_instance_creation == Exposed[["sex_at_instance_creation"]] & 
      (VAC_DATE1 > Exposed[["FIRST_PFIZER"]] | is.na(VAC_DATE1)) &
      YEAR_BIRTH %between% list(Exposed[["YEAR_BIRTH"]] - 1,  Exposed[["YEAR_BIRTH"]] + 1) &
      (FIRST_COV_INF >= Exposed[["FIRST_PFIZER"]] | is.na(FIRST_COV_INF))
    
    ,][["person_id"]]
}
}
  
MATCHED[i,1] <- Exposed[["person_id"]]

if(length(Controls) > 0)
MATCHED[i,2] <- sample(Controls, 1)else{
  MATCHED[i,2] <- NA
  
}
rm(Exposed,Controls)    
print(i)
}
)
              