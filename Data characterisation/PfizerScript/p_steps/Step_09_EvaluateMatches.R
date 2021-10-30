


check0 <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
TEST <- readRDS(paste0(populations_dir,"MATCH_PAIRS.rds"))

check <- merge(x = TEST[,.(Exposed,Control)], y = check0, by.x = "Exposed",by.y = "person_id")

check <- merge(x = check, y = check0, by.x = "Control",by.y = "person_id")

check1 <- check[,.(sex_at_instance_creation.x,sex_at_instance_creation.y)][sex_at_instance_creation.x == sex_at_instance_creation.y, x := "correct"]
check2 <- check[,.(birth_date.x,birth_date.y)][,x := birth_date.x - birth_date.y]
check3 <- check[,.(FIRST_PFIZER.x,FIRST_PFIZER.y,FIRST_OTHER.y)][FIRST_PFIZER.x < FIRST_PFIZER.y, x := "correct"  ][FIRST_PFIZER.x < FIRST_OTHER.y, x := "correct"  ][is.na(FIRST_PFIZER.y) & is.na(FIRST_OTHER.y), x := "correct"  ]
check4 <- check[,.(FIRST_PFIZER.x,FIRST_COV_INF.x,FIRST_COV_INF.y)][FIRST_COV_INF.x < FIRST_PFIZER.x &  FIRST_COV_INF.y < FIRST_PFIZER.x, x := "correct"]
check4 <- check4[is.na(FIRST_COV_INF.x) & is.na(FIRST_COV_INF.y), x := "correct"]
check4 <- check4[FIRST_COV_INF.x > FIRST_PFIZER.x &  FIRST_COV_INF.y > FIRST_PFIZER.x, x := "correct"]
check4 <- check4[FIRST_COV_INF.x > FIRST_PFIZER.x &  is.na(FIRST_COV_INF.y), x := "correct"]
check4 <- check4[FIRST_COV_INF.y > FIRST_PFIZER.x &  is.na(FIRST_COV_INF.x), x := "correct"]





