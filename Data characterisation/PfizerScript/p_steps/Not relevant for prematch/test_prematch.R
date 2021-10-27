


TEST <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
TEST <- TEST[, FIRST_COV_INF2 :=  fifelse(is.na(FIRST_COV_INF),"F","T")]


TEST <- TEST[,YEAR_BIRTH := year(birth_date)]
TEST <- TEST[!is.na(FIRST_PFIZER),month_t0 := paste0(sprintf("%02d",month(FIRST_PFIZER)),"-",year(FIRST_PFIZER))]

# system.time(TEST2 <- sqldf(
#   
#   "
#   SELECT DISTINCT
#   t1.person_id as EXPOSED,
#   t1.FIRST_PFIZER as Date_T0,
#   t1.month_t0,
#   t2.person_id as Posible_Control
#   
#   from TEST t1 
#   
#   inner join TEST t2
#   
#   on(
#   
#   (t1.FIRST_PFIZER NOT NULL)
#   
#   AND
#   
#   (t1.sex_at_instance_creation = t2.sex_at_instance_creation) 
#   
#   AND
#   
#   (t1.FIRST_PFIZER < t2.FIRST_PFIZER OR t2.FIRST_PFIZER IS NULL)
#   
#   
#   AND
#   
#   (t1.YEAR_BIRTH BETWEEN t2.YEAR_BIRTH - 1 AND t2.YEAR_BIRTH + 1 )
#   
#   )
#   "
#   
#   
#   
# ))

system.time(TEST2 <- sqldf(
  
  "
  SELECT DISTINCT
  t1.person_id as EXPOSED,
  t1.FIRST_PFIZER as Date_T0,
  t1.month_t0,
  t2.person_id as Posible_Control
  
  from (SELECT person_id,FIRST_PFIZER,sex_at_instance_creation,YEAR_BIRTH,FIRST_COV_INF2,  month_t0 FROM TEST WHERE FIRST_PFIZER NOT NULL) t1 
  
  inner join TEST t2
  
  on(
  
  (t1.sex_at_instance_creation = t2.sex_at_instance_creation) 
  
  AND
  
  (t1.FIRST_PFIZER < t2.FIRST_PFIZER OR t1.FIRST_PFIZER < t2.FIRST_OTHER OR (t2.FIRST_PFIZER IS NULL AND t2.FIRST_OTHER IS NULL))
  
  
  AND
  
  (t1.YEAR_BIRTH BETWEEN t2.YEAR_BIRTH - 1 AND t2.YEAR_BIRTH + 1 )
  
  AND
  
  (t1.FIRST_COV_INF2 = t2.FIRST_COV_INF2)
  
  )
  "
  
  
  
))




DIAG <- readRDS(paste0(populations_dir,"NB_Diagnoses.rds"))


TEST3 <- as.data.table(sqldf(
  
  "
  SELECT 
  t1.*,
  t2.SUM_YEAR as NB_Exposed,
  t3.SUM_YEAR as NB_Posible_Control
  
  from TEST2 t1 
  
  left join DIAG t2 on (t1.EXPOSED = t2.rn and t1.month_t0 = t2.month)
  
  left join DIAG t3 on (t1.Posible_Control = t3.rn and t1.month_t0 = t3.month)
  
  "
  
  
  
))

TEST3 <- TEST3[is.na(NB_Exposed), NB_Exposed := 0]
TEST3 <- TEST3[is.na(NB_Posible_Control), NB_Posible_Control := 0]
TEST3 <- TEST3[NB_Exposed != NB_Posible_Control,Exclude_comb := T]
TEST3 <- TEST3[,Date_T0 := as.Date(Date_T0, origin="1970-01-01") ]


#check <- c(TEST2[["EXPOSED"]],TEST2[["Posible_Control"]])[DIAG[["rn"]] %in%  c(TEST2[["EXPOSED"]],TEST2[["Posible_Control"]])]

#check <- unique(c(TEST2[["EXPOSED"]],TEST2[["Posible_Control"]]))[unique(c(TEST2[["EXPOSED"]],TEST2[["Posible_Control"]])) %in%  unique(DIAG[["rn"]])]


#check0 <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
check0 <- TEST


check <- merge(x = TEST3[,.(EXPOSED,Posible_Control)], y = check0, by.x = "EXPOSED",by.y = "person_id")

check <- merge(x = check, y = check0, by.x = "Posible_Control",by.y = "person_id")

check1 <- check[,.(sex_at_instance_creation.x,sex_at_instance_creation.y)][sex_at_instance_creation.x == sex_at_instance_creation.y, x := "correct"]
check2 <- check[,.(birth_date.x,birth_date.y)][,x := birth_date.x - birth_date.y]
check3 <- check[,.(FIRST_PFIZER.x,FIRST_PFIZER.y,FIRST_OTHER.y)][FIRST_PFIZER.x < FIRST_PFIZER.y, x := "correct"  ][FIRST_PFIZER.x < FIRST_OTHER.y, x := "correct"  ][is.na(FIRST_PFIZER.y) & is.na(FIRST_OTHER.y), x := "correct"  ]
check4 <- check[,.(FIRST_COV_INF2.x,FIRST_COV_INF2.y)][FIRST_COV_INF2.x == FIRST_COV_INF2.y, x := "correct"]






