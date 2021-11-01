

print("Determine per month, within the study period, the status for matching criteria")
INF <- readRDS(paste0(populations_dir,"INFLUENZA.rds"))


#file <- INF
#c.date <- "Date"
#lookback <- 10
#id <- "person_id"

CountHistorical <- function(file, c.date, lookback, id){

  file <- file[, month := month(get(c.date))][, year := year(get(c.date))]
  file <- file[,label := paste0(sprintf("%02d",month),"-",year)]
  
  comb <- as.data.table(expand.grid(1:12,c((year(start_study_date) - lookback):year(end_study_date))))[,label := paste0(sprintf("%02d",Var1),"-",Var2)]
  comb <- comb[, order := as.numeric(row.names(comb))]
  comb <- comb[Var1 == month(start_study_date) & Var2 == year(start_study_date), TIME := 0]
  comb <- comb[, TIME2 := order - comb[TIME == 0, order]]
  comb <- comb[TIME2 > - (12 * lookback) - 1, ]

  file <- merge(x = comb[,.(label,TIME2)], y = file, by = "label", all.x = T)
  file <- file[, .(NB = sum(!is.na(eval(Date)))), by = c(id, "label", "TIME2")]
  setorder(file,TIME2)
  setnames(file, eval(id), "id")
  
  file <- as.matrix(dcast(file, id  ~  TIME2, value.var = "NB")[!is.na(id),], rownames = "id")
  
  file[is.na(file)] <- 0

  TEMP <- matrix(NA,nrow = nrow(file), ncol = ncol(file) - (12 * lookback))
  colnames(TEMP) <- comb[TIME2 >= 0,label]
  rownames(TEMP) <- row.names(file)
  
  for(i in ((12 * lookback) + 1):max(as.numeric(ncol(file)))) TEMP[,i - (12 * lookback)] <- rowSums(file[, (i- (12 * lookback)):(i-1)])
  
  #TEMP <- as.data.table(TEMP, keep.rownames = T)
  #TEMP <- melt(TEMP, id.vars = "rn", measure.vars =  colnames(TEMP)[!colnames(TEMP) %in% "rn"], variable.name = "month", value.name = "SUM_year")
  #TEMP <- TEMP[SUM_year > 0,]
  #setnames(TEMP, "rn", eval(id))
  
  return(TEMP)
  
  rm(TEMP,file)
  gc()
  
} 
  

TEMP <- CountHistorical(
  file = INF,
  c.date = "Date",
  lookback = 5,
  id = "person_id"
)

rm(INF)
gc()

###########
#i = "03-2021"
TEMP_COV <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))[,.(person_id, FIRST_COV_INF)][!is.na(FIRST_COV_INF),]
for(i in colnames(TEMP)){
  
  ref <- seq.Date(as.Date(paste0("01-",i), "%d-%m-%Y"), length.out = 2, by = "month")[2] - 1
  
  TEMP2 <- as.data.table(TEMP[,i], keep.rownames = T)
  colnames(TEMP2) <- c("person_id","SUM_year")
  
  TEMP3 <- merge(TEMP2, TEMP_COV, by = c("person_id"), all = T, allow.cartesian = F)
  TEMP3 <- TEMP3[, ref_dat := ref ]
  #TEMP3 <- TEMP3[FIRST_COV_INF <= ref_dat, FIRST_COV_INF2 := T ][,FIRST_COV_INF := NULL]
  TEMP3 <- TEMP3[FIRST_COV_INF <= ref_dat, ][,ref_dat := NULL]
  #TEMP3 <- TEMP3[!is.na(SUM_year), SUM_year2 := T ][,SUM_year := NULL][,ref_dat := NULL]
  #setnames(TEMP3,c("SUM_year2","FIRST_COV_INF2") , c("SUM_year","FIRST_COV_INF"))
  saveRDS(TEMP3,paste0(populations_dir,"Matching/",i,".rds"))
  rm(TEMP2,TEMP3,ref)
  gc()
}
###########  
  

#TEMP2 <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))[,.(person_id, FIRST_COV_INF)][!is.na(FIRST_COV_INF),]
#TEMP2 <- as.data.table(expand.grid(TEMP2[["person_id"]],unique(TEMP[["month"]])))
#colnames(TEMP2) <- c("person_id","month")

#TEMP3 <- merge(TEMP,TEMP2[,FIRST_COV_INF := T],by = c("person_id","month"), all = T)

# x <- colnames(TEMP3)[!colnames(TEMP3) %in% c("person_id","month","FIRST_COV_INF")]
# 
# lapply(x, function(x) {
#   TEMP3 <- TEMP3[!is.na(get(x)), paste0(x,"2") := T]
#   TEMP3 <- TEMP3[, eval(x) := NULL]
#   setnames(TEMP3, paste0(x,"2"), eval(x))
# 
#   } 
#       
# )
# 
# saveRDS(TEMP3,paste0(populations_dir,"HIST.rds"))
# 
# rm(TEMP,TEMP2,TEMP3,x)
rm(TEMP,TEMP_COV)
gc()

