

Concepts <- list.files(concepts_dir)
study_population <- readRDS(paste0(populations_dir,"ALL_study_population.rds"))[,.(person_id,start_follow_up, end_follow_up)]

for(i in 1:length(Concepts)){
  
  print(Concepts[i])
  
  TEMP <- readRDS(paste0(concepts_dir,Concepts[i]))
  
  TEMP <- TEMP[,.(person_id, start_date_record)][,start_date_record := as.Date(as.character(start_date_record), "%Y%m%d") ]
  
  
  TEMP2 <- ConceptTimeSelection(
    ID = "person_id",
    Concept = TEMP,
    v.date = "start_date_record",
    population = study_population,
    v.obs.start = "start_follow_up",
    #t.prior =  1000,
    method = "d.range",
    v.obs.end = "end_follow_up"
  )

  saveRDS(TEMP2,paste0(concepts_dir,"T_",Concepts[i]))
  rm(TEMP,TEMP2)
  gc()
  
  
}

rm(study_population)