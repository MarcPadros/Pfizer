


mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))

dbListTables(mydb)

p <- dbSendStatement(mydb, paste0("CREATE INDEX EVENTS_index ON EVENTS(sheet,Date)"))
dbClearResult(p)

x <- unlist(dbGetQuery(mydb,"Select DISTINCT sheet FROM EVENTS"))

#dbGetQuery(mydb,"SELECT person_id, op_start_date, op_end_date FROM M_Studycohort")

#y = "COVDIAB"

lapply(x, function(y){

  print(y)
  p <- dbSendStatement(mydb, paste0("CREATE TABLE TEMP AS SELECT DISTINCT person_id, Date, sheet, NB FROM EVENTS WHERE sheet = '",y,"';"))
  dbClearResult(p)
  
  p <- dbSendStatement(mydb, paste0("CREATE INDEX TEMP_index ON TEMP(person_id, Date)"))
  dbClearResult(p)
  
  x2 <- dbGetQuery(mydb,"Select DISTINCT person_id FROM TEMP")
  
  p <- dbSendStatement(mydb, paste0("CREATE TABLE TEMP2 AS SELECT DISTINCT person_id, op_start_date,op_end_date FROM M_Studycohort;"))
  dbClearResult(p)
  
  p <- dbSendStatement(mydb, paste0("CREATE INDEX TEMP2_index ON TEMP2(person_id, op_start_date, op_end_date)"))
  dbClearResult(p)
  
  code <- ConceptTimeSelection_SQL(
    ID = "person_id",
    Concept = "TEMP",
    c.Concept = "NB",
    v.date = "Date",
    population = "TEMP2",
    v.obs.start = "op_start_date",
    #t.prior =  1000,
    method = "d.range",
    v.obs.end = "op_end_date",
    join = "inner"
  )
  
  
  p <- dbGetQuery(mydb,paste0(code,";"))
  
  
  saveRDS(p,paste0(g_intermediate,"AESI2/",y,".rds"))
  
  
  p <- dbSendStatement(mydb, "DROP TABLE TEMP")
  dbClearResult(p)
  
  p <- dbSendStatement(mydb, "DROP TABLE TEMP2")
  dbClearResult(p)
  
  rm(code,p,x2)
  
  
}

)

dbDisconnect(mydb)






