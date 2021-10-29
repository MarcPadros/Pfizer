library("parallel")
n.cores <- detectCores()
nb_cores <- ceiling(n.cores/2)

M_Studycohort <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
#HIST <- readRDS(paste0(populations_dir,"HIST.rds"))

FILE_EXPOSED <- copy(M_Studycohort)[!is.na(FIRST_PFIZER),][,.(person_id, sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH, month_t0)]
FILE_CONTROL <- copy(M_Studycohort)[,.(person_id,sex_at_instance_creation,FIRST_PFIZER,FIRST_OTHER,YEAR_BIRTH)]

rm(M_Studycohort)
gc()

FILE_CONTROL <- FILE_CONTROL[, VAC_DATE1 := fifelse(!is.na(FIRST_PFIZER),FIRST_PFIZER,FIRST_OTHER) ][, FIRST_PFIZER := NULL ][, FIRST_OTHER := NULL ]

#FILE_EXPOSED[duplicated(FILE_EXPOSED[,.(sex_at_instance_creation,FIRST_PFIZER,YEAR_BIRTH)]),]
comb <- unique(FILE_EXPOSED[,.(sex_at_instance_creation,FIRST_PFIZER,YEAR_BIRTH)])
setorder(comb,FIRST_PFIZER,YEAR_BIRTH)
#comb <- comb[, x := shift(FIRST_PFIZER,1)][x != FIRST_PFIZER , delete := T][, x :=  NULL]

#x <- paste0(FILE_EXPOSED[["sex_at_instance_creation"]],"|",FILE_EXPOSED[["FIRST_PFIZER"]],"|", FILE_EXPOSED[["YEAR_BIRTH"]])
#comb <- unique(as.data.table(do.call(rbind ,strsplit(x, "|", fixed = T))))
#colnames(comb) <- c("sex_at_instance_creation","FIRST_PFIZER","YEAR_BIRTH")
#comb <-comb[, YEAR_BIRTH := as.numeric(YEAR_BIRTH)]
#comb <-comb[, FIRST_PFIZER := as.Date(FIRST_PFIZER)]

#i = 4

m = 0
nb_exposed <- nrow(FILE_EXPOSED)
MATCHED <- data.table()

system.time(for(i in 1:nrow(comb)){

# if(comb[["delete"]]){
#   FILE_EXPOSED <- FILE_EXPOSED[FIRST_PFIZER >= comb[["FIRST_PFIZER"]][i],]
#   FILE_CONTROL <- FILE_CONTROL[VAC_DATE1 > comb[["FIRST_PFIZER"]][i] | is.na(VAC_DATE1),]
#   
#   }  
  
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
                        (VAC_DATE1 > comb[["FIRST_PFIZER"]][i] | is.na(VAC_DATE1)) &
                      #(FIRST_PFIZER > comb[["FIRST_PFIZER"]][i] | FIRST_OTHER > comb[["FIRST_PFIZER"]][i] | (is.na(FIRST_PFIZER) & is.na(FIRST_OTHER))) & 
                        YEAR_BIRTH %between% list(comb[["YEAR_BIRTH"]][i] - 1,  comb[["YEAR_BIRTH"]][i] + 1)
                      ,.(person_id)][, id := i][,t0 := comb[["FIRST_PFIZER"]][i]]
        
        setnames(Controls, "person_id", "Control")
        
        Exposed[,t0_month := paste0(sprintf("%02d",month(t0)),"-",year(t0))]
        Controls[,t0_month := paste0(sprintf("%02d",month(t0)),"-",year(t0))]
        
        ###
        HIST <-readRDS(paste0(populations_dir,"Matching/",paste0(sprintf("%02d",month(comb[i, FIRST_PFIZER])),"-",year(comb[i, FIRST_PFIZER])),".rds"))
        Exposed <- merge(x = Exposed, y = HIST, by.x = c("Exposed"), by.y = c("person_id"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := F][is.na(FIRST_COV_INF), FIRST_COV_INF := F]
        Controls <- merge(x = Controls, y = HIST, by.x = c("Control"), by.y = c("person_id"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := F][is.na(FIRST_COV_INF), FIRST_COV_INF := F]
        rm(HIST)
        gc()
        ###
        #Exposed <- merge(x = Exposed, y = HIST, by.x = c("Exposed","t0_month"), by.y = c("person_id","month"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := F][is.na(FIRST_COV_INF), FIRST_COV_INF := F]
        #Controls <- merge(x = Controls, y = HIST, by.x = c("Control","t0_month"), by.y = c("person_id","month"), all.x = T, all.y = F, allow.cartesian = F)[is.na(SUM_year), SUM_year := F][is.na(FIRST_COV_INF), FIRST_COV_INF := F]
        
        scheme <- as.data.table(expand.grid(c(T,F),c(T,F)))
        
        
        #k = 1
        for(k in 1:nrow(scheme)){
        
                  Exposed1 <- Exposed[SUM_year == scheme[["Var1"]][k] & FIRST_COV_INF == scheme[["Var2"]][k],]
                  
                  if(nrow(Exposed1) > 0){
                  Controls1 <- Controls[SUM_year == scheme[["Var1"]][k] & FIRST_COV_INF == scheme[["Var2"]][k],]
                    
                  if(nrow(Exposed1) >= (nb_cores * 10)){
                    rows <- ceiling(nrow(Exposed1)/nb_cores)
                    clust <- makeCluster(nb_cores, setup_timeout = 5000)
                    x <- c(1:nb_cores)
                    nb_cores2 <- nb_cores
                  }else{
                    rows <- nrow(Exposed)
                    clust <- makeCluster(1, setup_timeout = 5000)
                    x <- 1
                    nb_cores2 <- 1
                  }
                  
                  clusterExport(clust, varlist =  c("Exposed1","Controls1","nb_cores2","rows"))
                
                  
                  
                  #y <- 1
                  
                  
                  
                  system.time(MATCHED_TEMP <- parLapply(cl = clust,x, function(y){
                    
                    library("data.table")
                    
                    if(y < nb_cores2){
                      
                      Exposed1 <- Exposed1[(1 + ((y-1) * rows)) :(rows * y),]
                    
                    }else{
                      
                      Exposed1 <- Exposed1[(1 + ((y-1) * rows)) :nrow(Exposed1),]
                      
                    }
                    
                            Matches <-matrix(NA ,ncol = 2, nrow = nrow(Exposed1))
                            
                            for(j in 1:nrow(Exposed1)){
                              Matches[j,1] <- Exposed1[["Exposed"]][j] 
                              if(nrow(Controls1) > 0)Matches[j,2] <- sample(unique(Controls1[["Control"]]),1)  
                            }
                            
                            return(Matches)
                            #Matches <- as.data.table(Matches)
                            #colnames(Matches) <- c("Exposed","Control")
                  
                  }))
                  
                  stopCluster(clust)
                  
                  MATCHED_TEMP <- as.data.table(do.call(rbind, MATCHED_TEMP))
                  MATCHED_TEMP <- as.data.table(MATCHED_TEMP)
                  colnames(MATCHED_TEMP) <- c("Exposed","Control")
                  
                  if(nrow(MATCHED) == 0) MATCHED <- MATCHED_TEMP[0]
                  
                  MATCHED <- rbindlist(list(MATCHED ,MATCHED_TEMP ))
                  
                  rm(Controls1,MATCHED_TEMP)
                  gc()
                  } 
                  rm(Exposed1)
                  gc()
        }
        #TEMP <- unique(merge(x = Exposed[id == i,], y = Controls[id == i,], by = c("id","SUM_year","FIRST_COV_INF"), allow.cartesian = T, all.x = T))
        
        # TEMP <- sqldf("
        #               SELECT DISTINCT
        #               t1.Exposed,
        #               t2.Control,
        #               t1.t0
        #               
        #               FROM Exposed t1
        #               
        #               INNER JOIN Controls t2 ON (t1.id = t2.id AND t1.SUM_year = t2.SUM_year AND t1.FIRST_COV_INF = t2.FIRST_COV_INF)
        #               
        #               ")
        
        
        #if(i == 1) MATCHED <- TEMP[0][,.(Exposed,Control)]
        
        #MATCHED <- rbindlist(list(MATCHED , TEMP[sample(.N), c(.SD[1], .N), keyby = Exposed][,.(Exposed, Control)]))
        
        #print(i)
        m = m + nrow(Exposed)
        print(paste0(m, " matched from ",nb_exposed))
        
        rm(Controls,Exposed)
        gc()
  }
        
}
)




MATCHED <- MATCHED[, id := row.names(MATCHED)]
MATCHED <- merge(MATCHED, FILE_EXPOSED[,.(person_id, FIRST_PFIZER)], by.x = "Exposed", by.y = "person_id", all.x = T)
setnames(MATCHED, "FIRST_PFIZER", "T0")

saveRDS(MATCHED,paste0(populations_dir,"MATCH_PAIRS.rds"))

#rm(FILE_CONTROL, FILE_EXPOSED, comb, MATCHED,HIST)

rm(clust,comb,FILE_EXPOSED,FILE_CONTROL,MATCHED,scheme, nb_cores2)








