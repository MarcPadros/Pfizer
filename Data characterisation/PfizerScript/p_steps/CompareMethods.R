

mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))
dbListTables(mydb)

test1a <- dbReadTable(mydb,"M_Studycohort")
test1b <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))

test2a <- as.data.table(dbReadTable(mydb,"NB_Diagnoses"))
test2b <- readRDS(paste0(populations_dir,"NB_Diagnoses.rds"))

any((test2a == test2b)== F)

