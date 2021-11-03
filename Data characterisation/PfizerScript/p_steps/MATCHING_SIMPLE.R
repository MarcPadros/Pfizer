library("parallel")
n.cores <- detectCores()
nb_cores <- ceiling(n.cores/2)

M_Studycohort <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))

FILE_EXPOSED <- copy(M_Studycohort)[!is.na(FIRST_PFIZER),][,.(person_id, sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH, FIRST_COV_INF)]
FILE_EXPOSED <- FILE_EXPOSED[,FIRST_PFIZER := as.IDate(FIRST_PFIZER)]

FILE_CONTROL <- copy(M_Studycohort)[,.(person_id,sex_at_instance_creation,FIRST_PFIZER,FIRST_OTHER,YEAR_BIRTH,FIRST_COV_INF)]

rm(M_Studycohort)
gc()

FILE_CONTROL <- FILE_CONTROL[, VAC_DATE1 := fifelse(!is.na(FIRST_PFIZER),FIRST_PFIZER,FIRST_OTHER) ][, FIRST_PFIZER := NULL ][, FIRST_OTHER := NULL ]
FILE_CONTROL <- FILE_CONTROL[,VAC_DATE1 := as.IDate(VAC_DATE1)]


#x = 1:nrow(comb[1:6,])
x = 1:nrow(FILE_EXPOSED)

clust <- makeCluster(nb_cores, setup_timeout = 5000, outfile=paste0(populations_dir,"log2.txt"))
clusterExport(clust, varlist =  c("FILE_CONTROL","FILE_EXPOSED","MATCHED","populations_dir"))

system.time(MATCHED_TEMP <- parLapply(cl = clust, x , function(i){
  
            library("data.table")
            
            #system.time(for(i in 1:nrow(FILE_EXPOSED[1:834,])){
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
               
            MATCHED <- matrix(NA, nrow = 1, ncol = 2)
            MATCHED[1,1] <- Exposed[["person_id"]]
            
            if(length(Controls) > 0){
            MATCHED[1,2] <- sample(Controls, 1)}else{
              MATCHED[1,2] <- NA
              
            }
            return(MATCHED)
            print(i)
            rm(Exposed,Controls, MATCHED)    
#print(i)
}
)
)   

stopCluster(clust)

MATCHED <- as.data.table(do.call(rbind, MATCHED_TEMP))

MATCHED <- MATCHED[, id := row.names(MATCHED)]
MATCHED <- merge(MATCHED, FILE_EXPOSED[,.(person_id, FIRST_PFIZER)], by.x = "V1", by.y = "person_id", all.x = T)
setnames(MATCHED, c("FIRST_PFIZER","V1","V2"), c("T0", "Exposed","Control"))
MATCHED[, T0 := as.Date(T0)]

saveRDS(MATCHED,paste0(populations_dir,"MATCH_PAIRS_SIMPLE.rds"))
           





