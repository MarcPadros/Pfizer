
library("parallel")
n.cores <- detectCores()

mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))

dbListTables(mydb)

#p <- dbSendStatement(mydb, paste0("CREATE INDEX EVENTS_index ON EVENTS(sheet,Date)"))
#dbClearResult(p)

x <- unlist(dbGetQuery(mydb,"Select DISTINCT sheet FROM EVENTS"))

#dbGetQuery(mydb,"SELECT person_id, op_start_date, op_end_date FROM M_Studycohort")

y = "COVDIAB"

dbDisconnect(mydb)

clust <- makeCluster(n.cores/2, setup_timeout = 5000)
clusterExport(clust, varlist =  c("mydb","tmp","ConceptTimeSelection_SQL","g_intermediate"))

parLapply(cl = clust,x, function(y){
  
  
  library("RSQLite")
  library("DBI")
  mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))
  #temp <- dbConnect(RSQLite::SQLite(), paste0(tmp,y,".db"))
  temp <- dbConnect(RSQLite::SQLite(), "")
  
  dbExecute(mydb, "PRAGMA busy_timeout = 1000")
  #dbExecute(mydb, "BEGIN IMMEDIATE")
  
  p <- dbSendStatement(temp, paste0("ATTACH DATABASE '",paste0(tmp,"database.db"),"' AS temp_",y))
  dbClearResult(p)
  
  
  print(y)
  p <- dbSendStatement(temp, paste0("CREATE TABLE TEMP_",y," AS SELECT DISTINCT person_id, Date, sheet, NB FROM EVENTS WHERE sheet = '",y,"';"))
  dbClearResult(p)
  
  
  p <- dbSendStatement(temp, paste0("CREATE INDEX TEMP_",y,"_index ON TEMP_",y,"(person_id, Date)"))
  dbClearResult(p)
  
  #x2 <- dbGetQuery(temp,paste0("Select DISTINCT person_id FROM TEMP_",y))
  
  p <- dbSendStatement(temp, paste0("CREATE TABLE TEMP2_",y," AS SELECT DISTINCT person_id, op_start_date,op_end_date FROM M_Studycohort;"))
  dbClearResult(p)
  
  p <- dbSendStatement(temp, paste0("CREATE INDEX TEMP2_",y,"_index ON TEMP2_",y,"(person_id, op_start_date, op_end_date)"))
  dbClearResult(p)
  
  code <- ConceptTimeSelection_SQL(
    ID = "person_id",
    Concept = paste0("TEMP_",y,""),
    c.Concept = "NB",
    v.date = "Date",
    population = paste0("TEMP2_",y,""),
    v.obs.start = "op_start_date",
    #t.prior =  1000,
    method = "d.range",
    v.obs.end = "op_end_date",
    join = "inner"
  )
  
  
  p <- dbGetQuery(temp,paste0(code,";"))
  
  dbListTables(temp)
  
  
  saveRDS(p,paste0(g_intermediate,"AESI/",y,".rds"))
  
  
  #p <- dbSendStatement(mydb, paste0("DROP TABLE TEMP_",y,""))
  #dbClearResult(p)
  
  #p <- dbSendStatement(mydb, paste0("DROP TABLE TEMP2_",y,""))
  #dbClearResult(p)
  
  rm(code,p,x2)
  dbDisconnect(mydb)
  dbDisconnect(temp)
  

}

)

stopCluster(clust)
#showConnections(clust)





