mydb <- dbConnect(RSQLite::SQLite(), paste0(tmp,"database.db"))

nb_rows <- 10000

p <- dbSendStatement(mydb, "CREATE TABLE EXPOSED AS SELECT person_id,FIRST_PFIZER,FIRST_OTHER,sex_at_instance_creation,YEAR_BIRTH,month_t0 FROM M_Studycohort WHERE FIRST_PFIZER NOT NULL")
dbClearResult(p)

x <- dbGetQuery(mydb, "SELECT count(*) FROM EXPOSED")[1,1]

i = 1

last_line <- i * nb_rows
first_line <- ((i - 1) * nb_rows) + 1

p <- dbSendStatement(mydb, paste0("CREATE TABLE TEMP AS SELECT * FROM EXPOSED WHERE ROWID >= ",first_line," and ROWID <= ",last_line," ;"))
dbClearResult(p)

p <- dbSendStatement(mydb, "CREATE INDEX TEMP_index ON TEMP(sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH,month_t0,person_id)")
dbClearResult(p)
#p <- dbSendStatement(mydb, paste0("CREATE TABLE TEMP AS SELECT * FROM EXPOSED LIMIT ",nb_rows," OFFSET ",first_line," ;"))
#dbClearResult(p)

p <- dbSendStatement(mydb,
                     
                     "
  CREATE TABLE PREMATCH AS
  SELECT DISTINCT
  t1.person_id as EXPOSED,
  t1.FIRST_PFIZER as Date_T0,
  t1.month_t0,
  t2.person_id as Posible_Control
  
  from EXPOSED t1 
  
  inner join M_Studycohort t2
  
  on(
  
  (t1.sex_at_instance_creation = t2.sex_at_instance_creation) 
  
  AND
  
  (t1.FIRST_PFIZER < t2.FIRST_PFIZER OR t1.FIRST_PFIZER < t2.FIRST_OTHER OR (t2.FIRST_PFIZER IS NULL AND t2.FIRST_OTHER IS NULL))
  
  
  AND
  
  (t1.YEAR_BIRTH BETWEEN t2.YEAR_BIRTH - 1 AND t2.YEAR_BIRTH + 1 )
  
  
  
  )
  "
                     
                     
                     
)

dbClearResult(p)

p <- dbSendStatement(mydb, "CREATE INDEX PREMATCH_index ON PREMATCH(EXPOSED, Posible_Control, month_t0)")
dbClearResult(p)

p <- dbSendStatement(mydb,
                     
                     "
  CREATE TABLE PREMATCH2 AS
  SELECT 
  t1.*,
  t2.SUM_YEAR as NB_Exposed,
  t3.SUM_YEAR as NB_Posible_Control
  
  from PREMATCH t1 
  
  left join NB_Diagnoses t2 on (t1.EXPOSED = t2.rn and t1.month_t0 = t2.month)
  
  left join NB_Diagnoses t3 on (t1.Posible_Control = t3.rn and t1.month_t0 = t3.month)
  
  WHERE t2.SUM_YEAR = t3.SUM_YEAR OR (t2.SUM_YEAR IS NULL AND t3.SUM_YEAR IS NULL)
  
  "
                     
                     
                     
)


dbClearResult(p)

#x2 <- dbGetQuery(mydb, "SELECT EXPOSED, Posible_Control FROM PREMATCH2 GROUP BY EXPOSED ORDER BY RANDOM() LIMIT 1 ")
x3 <- as.data.table(dbGetQuery(mydb, "SELECT * FROM PREMATCH2  "))

MATCH <- x3[sample(.N), c(.SD[1], .N), keyby = EXPOSED]


p <- dbSendStatement(mydb, "DROP TABLE TEMP")
dbClearResult(p)

p <- dbSendStatement(mydb, "DROP TABLE EXPOSED")
dbClearResult(p)

p <- dbSendStatement(mydb, "DROP TABLE PREMATCH")
dbClearResult(p)


p <- dbSendStatement(mydb, "DROP TABLE PREMATCH2")
dbClearResult(p)



#x3 <- dbGetQuery(mydb,"Select * FROM EXPOSED")

INDEXES <- dbGetQuery(mydb,"SELECT * FROM sqlite_master WHERE type = 'index'")

dbDisconnect(mydb)


