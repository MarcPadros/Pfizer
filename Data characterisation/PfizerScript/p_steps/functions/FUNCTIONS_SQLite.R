
IMPORT_PATTERN_SQL <- function(pat,dir, colls = NULL, exprss = NULL, date.colls = NULL, T.NAME){
  
  obs_files<-list.files(dir, pattern=pat)
  if(length(obs_files) > 0){
    for(i in 1:length(obs_files)){
      TEMP <- fread(paste0(dir,"/",obs_files[i]), stringsAsFactors = F)
      if(!is.null(colls)){ TEMP <- TEMP[, ..colls] }
      
      #TEMP[, (colnames(TEMP)) := lapply(.SD, as.character), .SDcols = colnames(TEMP)]
      invisible(lapply(colnames(TEMP), function (x) if (class(TEMP[[x]]) != "character") TEMP[, eval(x) := as.character(get(x)) ]))
      
      if(!is.null(date.colls)){lapply(date.colls, function (x)  TEMP[, eval(x) := as.Date(get(x),"%Y%m%d") ])}
      
      if(!is.null(exprss)){
        
        rem1 <- nrow(TEMP) 
        TEMP <- TEMP[eval(exprss),]
        rem2 <- nrow(TEMP)
        print(paste0(rem2," selected rows from ",rem1," rows after evaluation of exprss"))
      }
      
      #NEW
      ####################
      
      
      if(i == 1) dbWriteTable(mydb, T.NAME,TEMP, overwrite = F, append = F)
      if(i > 1) dbWriteTable(mydb, T.NAME,TEMP, overwrite = F, append = T)
      
      
      
      
      ##################
      rm(TEMP)
      gc()
      
      
    }
  }else FILE <- NULL 
  
  print(dbListTables(mydb))
  
  
  rm(obs_files)
  gc()
}





CreateConceptDatasets_SQL <- function(codesheet,file, c.voc, c.concept,c.codes,c.startwith,f.code,f.voc, f.name = NULL ){
  
  codesheet <- copy(codesheet)
  
  dbWriteTable(mydb, paste0(f.name,"_CONCEPTS_CODES"),codesheet, overwrite = F, append = F)
  
  p <- dbSendStatement(mydb,paste0("ALTER TABLE ",f.name,"_CONCEPTS_CODES ADD code_no_dot varchar"))
  dbClearResult(p)
  p <- dbSendStatement(mydb,paste0("UPDATE ",f.name,"_CONCEPTS_CODES SET code_no_dot = REPLACE(",c.codes,",'.','') ;"))
  dbClearResult(p)
  
  p <- dbSendStatement(mydb,paste0("ALTER TABLE ",f.name," ADD code_no_dot2 varchar"))
  dbClearResult(p)
  p <- dbSendStatement(mydb,paste0("UPDATE ",f.name," SET code_no_dot2 = REPLACE(",f.code,",'.','') ;"))
  dbClearResult(p)
  
  p <- dbSendStatement(mydb,paste0("ALTER TABLE ",f.name,"_CONCEPTS_CODES ADD start_with varchar"))
  dbClearResult(p)
  
  p <- dbSendStatement(mydb,paste0("UPDATE ",f.name,"_CONCEPTS_CODES SET start_with = 
                         
                         (CASE
                         WHEN (substring(",c.codes,",length(",c.codes,"),1) = '.' OR ",c.voc," IN (",c.startwith,")) THEN 'T'
                         ELSE 'F'
                         END
                         )
                         
                         
                         ;"))
  
  dbClearResult(p)
  
  p <- dbSendStatement(mydb, paste0("CREATE INDEX index1 ON ",i,"(",f.voc,",code_no_dot2)"))
  dbClearResult(p)
  
  p <- dbSendStatement(mydb, paste0("CREATE INDEX index2 ON ",i,"_CONCEPTS_CODES(",c.voc,",code_no_dot)"))
  dbClearResult(p)
  
  code <- gsub("\n", "",paste0("CREATE TABLE TEMP as 
                                      select distinct t1.*, t2.",c.concept," from ",f.name," t1 
                                      inner join ",f.name,"_CONCEPTS_CODES t2 on (
                                      
                                      t1.",f.voc," = t2.",c.voc,"
                                      
                                      AND
                                      
                                      (
    
                                      (
    
                                      t2.start_with = 'T'
                                      
                                      AND
                                      
                                      substr(t1.code_no_dot2,1,length(t2.code_no_dot)) = substr(t2.code_no_dot,1,length(t2.code_no_dot))
                                      )
    
                                      OR
    
                                      (
                                      t2.start_with = 'F'
                                      
                                      AND
                                      
                                      t1.code_no_dot2 = t2.code_no_dot
                                      )
                                      )
    
                  )
    
    
              "))
  
  
  
  p <- dbSendStatement(mydb,"DROP INDEX index1;")
  dbClearResult(p)
  
  p <- dbSendStatement(mydb,"DROP INDEX index2;")
  dbClearResult(p)
  
  p <- dbSendStatement(mydb,code)
  dbClearResult(p)
  
  p <- dbSendStatement(mydb,paste0("DROP TABLE ",f.name,";"))
  dbClearResult(p)
  
  
  p <- dbSendStatement(mydb,paste0("ALTER TABLE TEMP RENAME TO ",f.name,";"))
  dbClearResult(p)
  
  dbListTables(mydb)
  
  rm(p,codesheet)
  gc()
  
}




ConceptTimeSelection_SQL <- function(ID, Concept,c.Concept = NULL, v.date,method = "d.range", population,c.population = NULL, v.obs.start, v.obs.end, t.prior =  NULL, set.boolean = F, join = "inner"){
  
  c.Concept <- paste0("t1.",c(v.date,c.Concept), collapse = ",")
  c.population <- paste0("t2.",c(ID,v.obs.start,v.obs.end,c.population), collapse = ",")
  
  if(method == "d.range") expr1 <- paste0("t1.",v.date," between t2.",v.obs.start," and t2.",v.obs.end)
  if(method == "d.prior") expr1 <- paste0("t1.",v.date," between t2.",v.obs.start," - ",t.prior," and t2.",v.obs.start)
  
  
  if(join == "inner") tables <- paste0("select distinct ",c.Concept,",",c.population," from ",Concept," t1 inner join ",population," t2 on (") 
  if(join == "left") tables <- paste0("select t2.*, t1.",v.date," from ",population," t2 left join ",Concept," t1 on (") 
  
  expr2 <-  paste0("t1.",ID," = t2.",ID," and ")
  
  return(paste0(tables,expr2,expr1,")"))
  
}

