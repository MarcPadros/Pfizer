mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))

p <- dbSendStatement(mydb,
                     
                     "

CREATE TABLE TEMP AS
SELECT
    ROW_NUMBER () OVER ( 
        PARTITION BY person_id,sheet
        ORDER BY start_date_record
    ) NB,
    person_id,
    sheet,
    start_date_record as Date
    
FROM
    EVENTS;




"
                     
)

dbClearResult(p)


p <- dbSendStatement(mydb,paste0("DROP TABLE EVENTS;"))
dbClearResult(p)


p <- dbSendStatement(mydb,paste0("ALTER TABLE TEMP RENAME TO EVENTS;"))
dbClearResult(p)


code <- ConceptTimeSelection_SQL(
  ID = "person_id",
  Concept = "(SELECT person_id,Date as FIRST_COV_INF,NB FROM EVENTS WHERE sheet = 'COVIDNARROW' AND NB = 1)",
  c.Concept = "NB",
  v.date = "FIRST_COV_INF",
  population = "(SELECT person_id, op_start_date, op_end_date FROM M_Studycohort)",
  v.obs.start = "op_start_date",
  #t.prior =  1000,
  method = "d.range",
  v.obs.end = "op_end_date",
  join = "inner"
)

p <- dbSendStatement(mydb,paste0("CREATE TABLE COVID AS ",code,";"))
dbClearResult(p)



rm(p,code)

#####################################################

#start_study_date <- (start_study_date - (3*366))

AESI <- as.data.table(dbGetQuery(mydb, "SELECT DISTINCT person_id, Date,sheet FROM  EVENTS"))[, Date := as.Date(Date, origin="1970-01-01")]



AESI <- AESI[, month := month(Date)][, year := year(Date)]
AESI <- AESI[,label := paste0(sprintf("%02d",month),"-",year)]


comb <- as.data.table(expand.grid(1:12,c((year(start_study_date)-1):year(end_study_date))))[,label := paste0(sprintf("%02d",Var1),"-",Var2)]
comb <- comb[, order := as.numeric(row.names(comb))]
comb <- comb[Var1 == month(start_study_date) & Var2 == year(start_study_date), TIME := 0]
comb <- comb[, TIME2 := order - comb[TIME == 0, order]]
comb <- comb[TIME2 > -13, ]

AESI2 <- merge(x = comb[,.(label,TIME2)], y = AESI, by = "label", all.x = T)
AESI2 <- AESI2[, .(NB = sum(!is.na(Date))), by = c("person_id", "label", "TIME2")]
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

dbWriteTable(mydb, "NB_Diagnoses",TEMP2, overwrite = F, append = F)

p <- dbSendStatement(mydb, paste0("CREATE INDEX NB_Diagnoses_index ON NB_Diagnoses(rn, month)"))
dbClearResult(p)
dbDisconnect(mydb)
####


rm(TEMP,TEMP2,comb,AESI,AESI2,AESI3,p)
gc()



