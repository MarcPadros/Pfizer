
nb <- 1000

M_Studycohort <- readRDS(paste0(populations_dir,"M_Studycohort.rds"))
Exposed <- M_Studycohort[!is.na(FIRST_PFIZER),]

VAC_INFLUENZA <- readRDS(paste0(vaccins_dir,"VAC_INFLUENZA.rds"))
VAC_INFLUENZA <- VAC_INFLUENZA[, .(person_id ,Date)][,x := 1]


library("parallel")
n.cores <- detectCores()

clust <- makeCluster(n.cores/2, setup_timeout = 5000)
clusterExport(clust, varlist =  c("M_Studycohort","VAC_INFLUENZA","Exposed","nb"))

x <- c(1:round(nrow(Exposed)/nb))
x <- x[1:12]
y <- 2

system.time(aatest <- parLapply(cl = clust,x, function(y){

library("Rcpp")
library("sqldf")
library("data.table")

Exposed_temp <- Exposed[(1 + ((y-1) * nb)) :(nb * y),]

test <- sqldf(
  
  "
    SELECT DISTINCT
    t1.person_id as EXPOSED,
    t1.FIRST_PFIZER as Date_T0,
    t2.person_id as Posible_Control,
    t3.x as x1,
    t4.x as x2
    
    from Exposed_temp t1 
    
    inner join M_Studycohort t2
    
    on(
      
      (t1.sex_at_instance_creation = t2.sex_at_instance_creation) 
      
      AND
      
      (t1.FIRST_PFIZER < t2.FIRST_PFIZER OR t1.FIRST_PFIZER < t2.FIRST_OTHER OR (t2.FIRST_PFIZER IS NULL AND t2.FIRST_OTHER IS NULL))
      
      
      AND
      
      (t1.YEAR_BIRTH BETWEEN t2.YEAR_BIRTH - 1 AND t2.YEAR_BIRTH + 1 )
      
      AND
      
      ((t1.FIRST_COV_INF IS NULL and t2.FIRST_COV_INF IS NULL)  OR (t1.FIRST_COV_INF < t1.FIRST_PFIZER AND t2.FIRST_COV_INF < t1.FIRST_PFIZER))
      
      
      
    )
    
    left join VAC_INFLUENZA t3 on (t1.person_id = t3.person_id AND  t3.Date = t1.FIRST_PFIZER)
    
    left join VAC_INFLUENZA t4 on (t1.person_id = t4.person_id AND  t4.Date = t1.FIRST_PFIZER)
    
    
    WHERE ((x1 = x2) OR (x1 IS NULL and x2 IS NULL ))
    
    "
)                    
              





test <- as.data.table(test)
test2 <- test[sample(.N), c(.SD[1], .N), keyby = EXPOSED]



}
))

stopCluster(clust)

aatest <- do.call(rbind, aatest)



















system.time(test <- sqldf(
  
  "
    SELECT DISTINCT
    t1.person_id as EXPOSED,
    t1.FIRST_PFIZER as Date_T0,
    t2.person_id as Posible_Control,
    t3.x as x1,
    t4.x as x2
    
    from Exposed t1 
    
    inner join M_Studycohort t2
    
    on(
      
      (t1.sex_at_instance_creation = t2.sex_at_instance_creation) 
      
      AND
      
      (t1.FIRST_PFIZER < t2.FIRST_PFIZER OR t1.FIRST_PFIZER < t2.FIRST_OTHER OR (t2.FIRST_PFIZER IS NULL AND t2.FIRST_OTHER IS NULL))
      
      
      AND
      
      (t1.YEAR_BIRTH BETWEEN t2.YEAR_BIRTH - 1 AND t2.YEAR_BIRTH + 1 )
      
      AND
      
      ((t1.FIRST_COV_INF IS NULL and t2.FIRST_COV_INF IS NULL)  OR (t1.FIRST_COV_INF < t1.FIRST_PFIZER AND t2.FIRST_COV_INF < t1.FIRST_PFIZER))
      
      
      
    )
    
    left join VAC_INFLUENZA t3 on (t1.person_id = t3.person_id AND  t3.Date = t1.FIRST_PFIZER)
    
    left join VAC_INFLUENZA t4 on (t1.person_id = t4.person_id AND  t4.Date = t1.FIRST_PFIZER)
    
    
    WHERE ((x1 = x2) OR (x1 IS NULL and x2 IS NULL ))
    
    "
)                    
)                 



test <- as.data.table(test)
test2 <- test[sample(.N), c(.SD[1], .N), keyby = EXPOSED]
























mydb <- dbConnect(RSQLite::SQLite(), "")

dbWriteTable(mydb, "M_Studycohort", M_Studycohort)
dbWriteTable(mydb, "Exposed", Exposed)
dbWriteTable(mydb, "VAC_INFLUENZA", VAC_INFLUENZA)

dbListTables(mydb)

p <- dbSendStatement(mydb, "CREATE INDEX M_Studycohort_index ON M_Studycohort(sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH,person_id)")
dbClearResult(p)

p <- dbSendStatement(mydb, "CREATE INDEX Exposed_index ON Exposed(sex_at_instance_creation, FIRST_PFIZER, FIRST_OTHER, YEAR_BIRTH,person_id)")
dbClearResult(p)

p <- dbSendStatement(mydb, "CREATE INDEX VAC_INFLUENZA_index ON VAC_INFLUENZA(person_id, Date)")
dbClearResult(p)

system.time(test <- dbGetQuery(mydb,

    "
    SELECT DISTINCT
    t1.person_id as EXPOSED,
    t1.FIRST_PFIZER as Date_T0,
    t2.person_id as Posible_Control,
    t3.x as x1,
    t4.x as x2
    
    from Exposed t1 
    
    inner join M_Studycohort t2
    
    on(
      
      (t1.sex_at_instance_creation = t2.sex_at_instance_creation) 
      
      AND
      
      (t1.FIRST_PFIZER < t2.FIRST_PFIZER OR t1.FIRST_PFIZER < t2.FIRST_OTHER OR (t2.FIRST_PFIZER IS NULL AND t2.FIRST_OTHER IS NULL))
      
      
      AND
      
      (t1.YEAR_BIRTH BETWEEN t2.YEAR_BIRTH - 1 AND t2.YEAR_BIRTH + 1 )
      
      AND
      
      ((t1.FIRST_COV_INF IS NULL and t2.FIRST_COV_INF IS NULL)  OR (t1.FIRST_COV_INF < t1.FIRST_PFIZER AND t2.FIRST_COV_INF < t1.FIRST_PFIZER))
      
      
      
    )
    
    left join VAC_INFLUENZA t3 on (t1.person_id = t3.person_id AND  t3.Date = t1.FIRST_PFIZER)
    
    left join VAC_INFLUENZA t4 on (t1.person_id = t4.person_id AND  t4.Date = t1.FIRST_PFIZER)
    
    
    WHERE ((x1 = x2) OR (x1 IS NULL and x2 IS NULL ))
    
    "
)                    
)                 

test <- as.data.table(test)
test2 <- test[sample(.N), c(.SD[1], .N), keyby = EXPOSED]

dbDisconnect(mydb)


system.time(test <- sqldf(
                               
                               "
    SELECT DISTINCT
    t1.person_id as EXPOSED,
    t1.FIRST_PFIZER as Date_T0,
    t2.person_id as Posible_Control,
    t3.x as x1,
    t4.x as x2
    
    from Exposed t1 
    
    inner join M_Studycohort t2
    
    on(
      
      (t1.sex_at_instance_creation = t2.sex_at_instance_creation) 
      
      AND
      
      (t1.FIRST_PFIZER < t2.FIRST_PFIZER OR t1.FIRST_PFIZER < t2.FIRST_OTHER OR (t2.FIRST_PFIZER IS NULL AND t2.FIRST_OTHER IS NULL))
      
      
      AND
      
      (t1.YEAR_BIRTH BETWEEN t2.YEAR_BIRTH - 1 AND t2.YEAR_BIRTH + 1 )
      
      AND
      
      ((t1.FIRST_COV_INF IS NULL and t2.FIRST_COV_INF IS NULL)  OR (t1.FIRST_COV_INF < t1.FIRST_PFIZER AND t2.FIRST_COV_INF < t1.FIRST_PFIZER))
      
      
      
    )
    
    left join VAC_INFLUENZA t3 on (t1.person_id = t3.person_id AND  t3.Date = t1.FIRST_PFIZER)
    
    left join VAC_INFLUENZA t4 on (t1.person_id = t4.person_id AND  t4.Date = t1.FIRST_PFIZER)
    
    
    WHERE ((x1 = x2) OR (x1 IS NULL and x2 IS NULL ))
    
    "
)                    
)                 



