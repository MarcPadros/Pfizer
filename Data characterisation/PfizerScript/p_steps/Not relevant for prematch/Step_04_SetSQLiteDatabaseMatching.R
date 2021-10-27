

library("RSQLite")
library("DBI")

mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))

PERSONS_OF_INTEREST <- unique(readRDS(paste0(populations_dir,"M_Studycohort.rds"))[["person_id"]])

i = "EVENTS"

system.time(IMPORT_PATTERN_SQL(
  dir = path_dir, 
  pat = i,
  colls = c("person_id", "start_date_record","event_code","event_record_vocabulary"),
  date.colls = c("start_date_record"),
  exprss = expression(start_date_record > (start_study_date - 366 * 10) & start_date_record < end_study_date & person_id %in% PERSONS_OF_INTEREST),
  T.NAME = i
  
))

FILE <- fread(paste0(pre_dir,i,"_CODES.csv"))[,.(Coding_system,Code,sheet)]

system.time(CreateConceptDatasets_SQL(
  
  codesheet = FILE,
  c.voc = "Coding_system", 
  c.concept = "sheet",
  c.codes = "Code",
  c.startwith = "'ICD10/CM','ICD9CM','ICD9'",
  f.code = "Event_code",
  f.voc = "event_record_vocabulary",
  f.name = i
  
))

p <- dbSendStatement(mydb, paste0("CREATE INDEX ",i,"_index ON ",i,"(sheet, start_date_record, person_id)"))
dbClearResult(p)

rm(FILE,i,p)
gc()

i <- "VACCINES"

system.time(IMPORT_PATTERN_SQL(
  dir = path_dir, 
  pat = i,
  colls = c("person_id","vx_record_date","vx_admin_date","vx_atc","vx_manufacturer"),
  date.colls = c("vx_record_date","vx_admin_date"),
  exprss = expression(start_study_date - vx_record_date < (366*6) | start_study_date - vx_admin_date < (366*6) & person_id %in% PERSONS_OF_INTEREST),
  T.NAME = i
  
))

FILE <- fread(paste0(pre_dir,i,"_CODES.csv"))[, Coding_system := "ATC"]
exp <- expression(substring(ATC,nchar(ATC),nchar(ATC)+1) == "*")
FILE <- FILE[, Code := fifelse(eval(exp),gsub("\\*","\\.",ATC),ATC)]


p <- dbSendStatement(mydb,paste0("ALTER TABLE ",i," ADD VOC varchar"))
dbClearResult(p)
p <- dbSendStatement(mydb,paste0("UPDATE ",i," SET VOC = 'ATC' ;"))
dbClearResult(p)


system.time(CreateConceptDatasets_SQL(
  
  codesheet = FILE,
  c.voc = "Coding_system", 
  c.concept = "sheet",
  c.codes = "Code",
  f.code = "vx_atc",
  f.voc =  "VOC",
  c.startwith = c("ATC"),
  f.name = i
  
))

p <- dbSendStatement(mydb,paste0("UPDATE ",i," SET vx_admin_date = vx_record_date WHERE vx_admin_date IS NULL"))
dbClearResult(p)

p <- dbSendStatement(mydb,paste0(
                     "
                     ALTER TABLE ",i,"
                     RENAME COLUMN vx_admin_date TO Date
                     
                     ;
                     "
                     ))
dbClearResult(p)

p <- dbSendStatement(mydb,paste0(
  "
                     ALTER TABLE ",i,"
                     DROP COLUMN vx_record_date
                     
                     ;
                     "
))
dbClearResult(p)





p <- dbSendStatement(mydb, paste0("CREATE INDEX ",i,"_index ON ",i,"(sheet, Date, person_id)"))
dbClearResult(p)

rm(FILE,i,p)
gc()

dbListTables(mydb)



dbDisconnect(mydb)
