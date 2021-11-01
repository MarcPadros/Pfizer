


check0 <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
TEST <- readRDS(paste0(populations_dir,"MATCH_PAIRS.rds"))
#TEST <- readRDS(paste0(populations_dir,"MATCH_PAIRS_SQL.rds"))

check <- merge(x = TEST[,.(Exposed,Control)], y = check0, by.x = "Exposed",by.y = "person_id")

check <- merge(x = check, y = check0, by.x = "Control",by.y = "person_id")

check1 <- check[,.(sex_at_instance_creation.x,sex_at_instance_creation.y)][sex_at_instance_creation.x == sex_at_instance_creation.y, x := "correct"]
check2 <- check[,.(birth_date.x,birth_date.y)][,x0 := birth_date.x - birth_date.y][x0 %between% list(-731, 731), x := "correct"]
check3 <- check[,.(FIRST_PFIZER.x,FIRST_PFIZER.y,FIRST_OTHER.y)][FIRST_PFIZER.x < FIRST_PFIZER.y, x := "correct"  ][FIRST_PFIZER.x < FIRST_OTHER.y, x := "correct"  ][is.na(FIRST_PFIZER.y) & is.na(FIRST_OTHER.y), x := "correct"  ]
check4 <- check[,.(FIRST_PFIZER.x,FIRST_COV_INF.x,FIRST_COV_INF.y)][FIRST_COV_INF.x < FIRST_PFIZER.x &  FIRST_COV_INF.y < FIRST_PFIZER.x, x := "correct"]
check4 <- check4[is.na(FIRST_COV_INF.x) & is.na(FIRST_COV_INF.y), x := "correct"]
check4 <- check4[FIRST_COV_INF.x > FIRST_PFIZER.x &  FIRST_COV_INF.y > FIRST_PFIZER.x, x := "correct"]
check4 <- check4[FIRST_COV_INF.x > FIRST_PFIZER.x &  is.na(FIRST_COV_INF.y), x := "correct"]
check4 <- check4[FIRST_COV_INF.y > FIRST_PFIZER.x &  is.na(FIRST_COV_INF.x), x := "correct"]



INF <- readRDS(paste0(populations_dir,"INFLUENZA.rds"))
check5 <- merge(x = TEST[,.(T0,Exposed,Control)], y = INF, by.x = "Exposed",by.y = "person_id", all.x = T)
check5 <- merge(x = check5, y = INF, by.x = "Control",by.y = "person_id", all.x = T)[,.(Exposed,Control,T0,Date.x,Date.y)]
check5 <- check5[, c.EXP := (T0 - Date.x)/365.25 ][,c.EXP2 := fifelse(c.EXP < 5,T,F)]
check5 <- check5[, c.CON := (T0 - Date.y)/365.25 ][,c.CON2 :=fifelse(c.CON < 5, T,F)]

check5a <- check5[c.EXP2 == T,][,.(Exposed, Date.x)]
check5a <- check5a[, x := T][,.(Exposed, x)]
check5b <- check5[c.CON2 == T,][,.(Control, Date.y)]
check5b <- check5b[, x := T][,.(Control, x)]
check5 <- merge(x = TEST[,.(T0,Exposed,Control)], y = unique(check5a), by.x = "Exposed",by.y = "Exposed", all.x = T)
check5 <- merge(x = check5, y = unique(check5b), by.x = "Control",by.y = "Control", all.x = T)[,.(Exposed,Control,T0,x.x,x.y)]
check5 <- check5[(x.x == T & x.y == T) | (is.na(x.x & is.na(x.y))), x := "correct"]

for(i in c("check1", "check2", "check3", "check4", "check5")){
  temp <- get(i)
  print(sum(temp[["x"]] == "correct") == nrow(temp) & length(temp[["x"]] == "correct") == nrow(TEST))
  rm(temp)
}

