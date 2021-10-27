


print("Add column to assign first or second vaccination")

mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))


p <- dbSendStatement(mydb,
              
                     "

CREATE TABLE TEMP AS
SELECT
    ROW_NUMBER () OVER ( 
        PARTITION BY person_id,sheet
        ORDER BY Date
    ) NB,
    person_id,
    sheet,
    Date,
    vx_manufacturer
FROM
    VACCINES;




"
                     
)

dbClearResult(p)


p <- dbSendStatement(mydb,paste0("DROP TABLE VACCINES;"))
dbClearResult(p)


p <- dbSendStatement(mydb,paste0("ALTER TABLE TEMP RENAME TO VACCINES;"))
dbClearResult(p)




#setorder(VAC,person_id, vx_admin_date)
#VAC <- VAC[,NB := seq_len(.N) , by = person_id]


#print("Join to covid vaccins to cohort on between op_start_date/op_end_date to select the vaccins administered within the window of interrest")
#study_population <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))

code <- ConceptTimeSelection_SQL(
  ID = "person_id",
  Concept = "(SELECT person_id,Date,NB,vx_manufacturer FROM VACCINES WHERE sheet = 'VAC_COVID19')",
  c.Concept = c("NB","vx_manufacturer"),
  v.date = "Date",
  population = "(SELECT person_id, op_start_date, op_end_date FROM M_Studycohort)",
  v.obs.start = "op_start_date",
  #t.prior =  1000,
  method = "d.range",
  v.obs.end = "op_end_date",
  join = "inner"
)

VAC <- as.data.table(dbGetQuery(mydb,code))  

print("Reform table to make 4 columns per subject. Pfizer dose 1 date, Pfizer dose 2 date, Other vaccin dose 1 date, and Other vaccin dose 2 date.")
VAC <- VAC[NB < 3,]
VAC <- VAC[, v.man := fifelse(like(vx_manufacturer, "pfizer", ignore.case = T ),"PFIZER","OTHER") ][, var := paste0(NB,"_",v.man)][,.(person_id,var,Date)]
VAC <- dcast(VAC, person_id  ~  var, value.var = "Date")
setnames(VAC,c("1_PFIZER","2_PFIZER","1_OTHER","2_OTHER"),c("FIRST_PFIZER","SECOND_PFIZER","FIRST_OTHER","SECOND_OTHER"))

#setkeyv(VAC_P, c("person_id","vx_admin_date","DUMMY"))
#TEMP <- foverlaps(study_population, VAC_P, by.x = c("person_id","start_follow_up", "end_follow_up"), nomatch = 0L, type = "any")

print("Add new variables to M_Studycohort")
study_population <- as.data.table(dbReadTable(mydb,"M_Studycohort"))[, birth_date := as.Date(birth_date, origin="1970-01-01")]
study_population <- merge(x = study_population, y = VAC, by = "person_id", all.x = T, all.y = F, allow.cartesian = F)

study_population <- study_population[, FIRST_PFIZER := as.Date(FIRST_PFIZER, origin="1970-01-01")]
study_population <- study_population[,YEAR_BIRTH := year(birth_date)]
study_population <- study_population[!is.na(FIRST_PFIZER),month_t0 := paste0(sprintf("%02d",month(FIRST_PFIZER)),"-",year(FIRST_PFIZER))]


#saveRDS(study_population,paste0(populations_dir,"M_Studycohort.rds"))

rm(VAC)

code <- ConceptTimeSelection_SQL(
  ID = "person_id",
  Concept = "(SELECT person_id, Date,NB FROM VACCINES WHERE sheet = 'VAC_INFLUENZA')",
  c.Concept = "NB",
  v.date = "Date",
  population = "(SELECT person_id, op_start_date, op_end_date FROM M_Studycohort)",
  v.obs.start = "op_start_date",
  #t.prior =  1000,
  method = "d.range",
  v.obs.end = "op_end_date",
  join = "inner"
)

p <- dbSendStatement(mydb,paste0("CREATE TABLE INFLUENZA AS ",code,";"))
dbClearResult(p)


#INF <- as.data.table(dbGetQuery(mydb,code))


#dbWriteTable(mydb, "INFLUENZA",INF, overwrite = F, append = F)
dbWriteTable(mydb, "M_Studycohort",study_population , overwrite = T, append = F)

p <- dbSendStatement(mydb, paste0("CREATE INDEX M_Studycohort_index ON M_Studycohort(sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH,month_t0,person_id)"))
dbClearResult(p)

dbDisconnect(mydb)

rm(p,study_population)








