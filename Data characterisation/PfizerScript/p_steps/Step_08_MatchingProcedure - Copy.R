M_Studycohort <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))

FILE_EXPOSED <- copy(M_Studycohort)[!is.na(FIRST_PFIZER),][,.(person_id, sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH, month_t0, FIRST_COV_INF)]
FILE_EXPOSED <- FILE_EXPOSED[,FIRST_PFIZER := as.IDate(FIRST_PFIZER)]

FILE_CONTROL <- copy(M_Studycohort)[,.(person_id,sex_at_instance_creation,FIRST_PFIZER,FIRST_OTHER,YEAR_BIRTH, FIRST_COV_INF)]

rm(M_Studycohort)
gc()

FILE_CONTROL <- FILE_CONTROL[, VAC_DATE1 := fifelse(!is.na(FIRST_PFIZER),FIRST_PFIZER,FIRST_OTHER) ][, FIRST_PFIZER := NULL ][, FIRST_OTHER := NULL ]
FILE_CONTROL <- FILE_CONTROL[,VAC_DATE1 := as.IDate(VAC_DATE1)]

##
comb <- unique(FILE_EXPOSED[,.(sex_at_instance_creation,FIRST_PFIZER,YEAR_BIRTH)])
setorder(comb,FIRST_PFIZER,YEAR_BIRTH)

m = 0
nb_exposed <- nrow(FILE_EXPOSED)
MATCHED <- data.table()

##########
library("parallel")
n.cores <- detectCores()
nb_cores <- ceiling(n.cores/2)

#x = 1:nrow(comb[1:6,])
x = 1:nrow(comb)

hist.cols <- c("INFP5","FIRST_COV_INF")
hist.cols2 <- paste0(hist.cols,"_2")

clust <- makeCluster(nb_cores, setup_timeout = 5000, outfile=paste0(populations_dir,"log.txt"))
clusterExport(clust, varlist =  c("FILE_CONTROL","FILE_EXPOSED","comb","MATCHED","populations_dir", "hist.cols", "hist.cols2"))



system.time(MATCHED_TEMP <- parLapply(cl = clust, x , function(i){
  
library("data.table")

#########

#system.time(for(i in 1:nrow(comb[1:6,])){

            Exposed <- FILE_EXPOSED[
            sex_at_instance_creation == comb[["sex_at_instance_creation"]][i] & 
            FIRST_PFIZER == comb[["FIRST_PFIZER"]][i] & 
            YEAR_BIRTH == comb[["YEAR_BIRTH"]][i],.(person_id, month_t0,FIRST_COV_INF)][, id := i][,t0 := comb[["FIRST_PFIZER"]][i]] 
    
            
            setnames(Exposed, "person_id", "Exposed")
            
            
                  if(nrow(Exposed) == 0){
                    rm(Exposed)
                    }else{
                  
                          Controls <- FILE_CONTROL[
                          sex_at_instance_creation == comb[["sex_at_instance_creation"]][i] & 
                          (VAC_DATE1 > comb[["FIRST_PFIZER"]][i] | is.na(VAC_DATE1)) &
                          #(FIRST_PFIZER > comb[["FIRST_PFIZER"]][i] | FIRST_OTHER > comb[["FIRST_PFIZER"]][i] | (is.na(FIRST_PFIZER) & is.na(FIRST_OTHER))) & 
                          YEAR_BIRTH %between% list(comb[["YEAR_BIRTH"]][i] - 1,  comb[["YEAR_BIRTH"]][i] + 1)
                          ,.(person_id, FIRST_COV_INF)][, id := i][,t0 := comb[["FIRST_PFIZER"]][i]]
                      
                          setnames(Controls, "person_id", "Control")
                      
                          #Exposed[,t0_month := paste0(sprintf("%02d",month(t0)),"-",year(t0))]
                          Controls[,month_t0 := paste0(sprintf("%02d",month(t0)),"-",year(t0))]
                          
                          ###
                          HIST <-readRDS(paste0(populations_dir,"Matching/",paste0(sprintf("%02d",month(comb[i, FIRST_PFIZER])),"-",year(comb[i, FIRST_PFIZER])),".rds"))
                          Exposed <- merge(x = Exposed, y = HIST, by.x = c("Exposed"), by.y = c("person_id"), all.x = T, all.y = F, allow.cartesian = F)
                          
                          
                          lapply(hist.cols, function(x) Exposed <- Exposed[, paste0(eval(x),"_2") := fifelse(get(x) < t0,T, F, na = F)][,eval(x) := NULL])
                          
                          Controls <- merge(x = Controls, y = HIST, by.x = c("Control"), by.y = c("person_id"), all.x = T, all.y = F, allow.cartesian = F)
                          lapply(hist.cols, function(x) Controls <- Controls[, paste0(eval(x),"_2") := fifelse(get(x) < t0, T, F, na = F)][,eval(x) := NULL])
                          
                          
                          
                          rm(HIST)
                          gc()
                          ###
                          #Exposed <- merge(x = Exposed, y = HIST, by.x = c("Exposed","t0_month"), by.y = c("person_id","month"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := F][is.na(FIRST_COV_INF), FIRST_COV_INF := F]
                          #Controls <- merge(x = Controls, y = HIST, by.x = c("Control","t0_month"), by.y = c("person_id","month"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := F][is.na(FIRST_COV_INF), FIRST_COV_INF := F]
                          
                          #scheme <- as.data.table(expand.grid(c(T,F),c(T,F)))
                          scheme <- unique(Exposed[,..hist.cols2])
                          
                                  #k = 1
                                  for(k in 1:nrow(scheme)){
                                            
                                            
                                    
                                            x <- paste0(hist.cols2," == scheme[['",hist.cols2,"']][k]", collapse = " & ")
                                            
                                            Exposed1 <- Exposed[eval(parse(text = x)),]
                                            
                                        
                                            if(nrow(Exposed1) > 0){
                                            Controls1 <- Controls[eval(parse(text = x)),]
                                              
                                            v.exp <- Exposed1[["Exposed"]]
                                            if(nrow(Controls1) > 0) v.control <- sample(Controls1[["Control"]], length(v.exp), replace = T)else{
                                              v.control <- rep(NA, length(v.exp))
                                              
                                            }
                                            MATCHED_TEMP <- as.data.table(cbind(v.exp,v.control))
                                            
                                            colnames(MATCHED_TEMP) <- c("Exposed","Control")
                                            
                                            if(nrow(MATCHED) == 0) MATCHED <- MATCHED_TEMP[0]
                                            
                                            MATCHED <- rbindlist(list(MATCHED ,MATCHED_TEMP ))
                                            
                                            rm(Controls1,MATCHED_TEMP)
                                            gc()
                                            } 
                                            rm(Exposed1, x)
                                            gc()
                                  }
                          
                                print(paste0(i," cycle from ", nrow(comb)," ",nrow(MATCHED)," cases matched during this cyclus" ))
                                
                                return(MATCHED)      
                          
                                rm(Controls,Exposed)
                                gc()
                    }
                          
}

)
)

stopCluster(clust)

MATCHED <- as.data.table(do.call(rbind, MATCHED_TEMP))

MATCHED <- MATCHED[, id := row.names(MATCHED)]
MATCHED <- merge(MATCHED, FILE_EXPOSED[,.(person_id, FIRST_PFIZER)], by.x = "Exposed", by.y = "person_id", all.x = T)
setnames(MATCHED, "FIRST_PFIZER", "T0")
MATCHED[, T0 := as.Date(T0)]
saveRDS(MATCHED,paste0(populations_dir,"MATCH_PAIRS.rds"))

rm(comb,FILE_EXPOSED,FILE_CONTROL,MATCHED,MATCHED_TEMP, hist.cols, hist.cols2)






