FILE <- fread(paste0(pre_dir,"EVENTS_CODES.csv"))[,.(Coding_system,Code,sheet)]
PERSONS_OF_INTEREST <- unique(readRDS(paste0(populations_dir,"M_Studycohort.rds"))[["person_id"]])

EVENTS <- IMPORT_PATTERN(
  dir = path_dir, 
  pat = "EVENTS",
  colls = c("person_id", "start_date_record","event_code","event_record_vocabulary"),
  date.colls = c("start_date_record"),
  exprss = expression(start_date_record > (start_study_date - 366 * 10) & start_date_record < end_study_date & person_id %in% PERSONS_OF_INTEREST)
)

CreateConceptDatasets(

  codesheet = FILE,
  c.voc = "Coding_system",
  c.concept = "sheet",
  c.codes = "Code",
  file = EVENTS,
  f.code = "event_code",
  f.voc =  "event_record_vocabulary",
  c.startwith = c("ICD10/CM","ICD9CM","ICD9"),
  path = concepts_dir,
  method = "SQL",
  group = F, 
  f.name = "AESI"

)

rm(EVENTS,FILE)
gc()

VACCINES <- IMPORT_PATTERN(
  dir = path_dir, 
  pat = "VACCINES",
  colls = c("person_id","vx_record_date","vx_admin_date","vx_atc","vx_manufacturer"),
  date.colls = c("vx_record_date","vx_admin_date"),
  exprss = expression(start_study_date - vx_record_date < (366*6) | start_study_date - vx_admin_date < (366*6) & person_id %in% PERSONS_OF_INTEREST)
  
)

print("If vx_admin_date is empty, replace with vx_record_date??")
VACCINES <- VACCINES[,Date := fifelse(is.na(vx_record_date),vx_admin_date,vx_record_date)][,vx_admin_date := NULL][,vx_record_date := NULL][,voc := "ATC"]

exp <- expression(substring(ATC,nchar(ATC),nchar(ATC)+1) == "*")
FILE <- fread(paste0(pre_dir,"VACCINES_CODES.csv"))
FILE <- FILE[, Coding_system :="ATC"]
FILE <- FILE[, Code := fifelse(eval(exp),gsub("\\*","\\.",ATC),ATC)]

CreateConceptDatasets(
  
  codesheet = FILE,
  c.voc = "Coding_system",
  c.concept = "sheet",
  c.codes = "Code",
  file = VACCINES,
  f.code = "vx_atc",
  f.voc =  "voc",
  c.startwith = c("ATC"),
  path =  vaccins_dir,
  method = "SQL",
  group = F, 
  f.name = "VACCINES"
  
)

rm(FILE,VACCINES)




