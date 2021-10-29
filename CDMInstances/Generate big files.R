rm(list = ls(all=TRUE))

Folder <- "Medium"
Nrows <- 100

if (!require("rstudioapi")) install.packages("rstudioapi")
if (!require("data.table")) install.packages("data.table")
#library(peakRAM)

folder <- dirname(rstudioapi::getSourceEditorContext()$path)

CreateLargeSimulatedDatasetCSV <- function(Inputfolder, Outputfolder, N, Identifier_name, Delimiter){
  files <- list.files(Inputfolder, pattern = paste0("*.", "csv"))  
  dir.create(Outputfolder, showWarnings = FALSE)
  
  for(i in files){  
    File<-fread(paste0(Inputfolder, '/', i), sep = Delimiter, stringsAsFactors = F)
    if(!any(colnames(File) == Identifier_name)){
      fwrite(File, file = paste0(Outputfolder, "/", i), sep = Delimiter, col.names = F, row.names = F, na = "", append = F)
      next
    }
    for(j in 1:N){
      File_temp <- copy(File)
      File_temp[,eval(Identifier_name) := paste0(get(Identifier_name), "_", j)]
      if(j == 1) fwrite(File_temp, file = paste0(Outputfolder, "/", i), sep = Delimiter, row.names = F, na = "", append = F)
      if(j > 1) fwrite(File_temp, file = paste0(Outputfolder, "/", i), sep = Delimiter, col.names = F, row.names = F, na = "", append = T)
    }
    
  }
}

CreateLargeSimulatedDatasetCSV(
  Inputfolder = paste0(folder,"/Pfizer"), 
  Outputfolder = paste0(folder,"/",Folder), 
  N = Nrows, 
  Identifier_name = "person_id", 
  Delimiter = ";"
  )

SOURCE <- fread(paste0(folder,"/Pfizer/CDM_SOURCE.csv"))
fwrite(SOURCE,file = paste0(folder,"/",Folder,"/CDM_SOURCE.csv"), sep = ";")

###########################

VAC <- fread(paste0(folder,"/",Folder,"/VACCINES.csv"))
PER <- fread(paste0(folder,"/",Folder,"/PERSONS.csv"))


EVENTS <- fread(paste0(folder,"/",Folder,"/EVENTS_NEW.csv"))[0]
person_id <- unique(sample(PER[["person_id"]], 0.7 * nrow(PER), replace = T))
event_code<- rep("U071.1",length(person_id))
event_record_vocabulary <- rep("ICD10CM",length(person_id))
start_date_record <- sample(seq(as.Date("20200101", "%Y%m%d"),as.Date("20210801", "%Y%m%d"),1),length(person_id), replace = T)
start_date_record <- paste0(year(start_date_record),sprintf("%02d",month(start_date_record)),sprintf("%02d",day(start_date_record)))


EVENTS <- rbindlist(list(as.data.table(cbind(person_id,event_code,event_record_vocabulary, start_date_record)), EVENTS), fill = T, use.names = T)

fwrite(EVENTS,file = paste0(folder,"/",Folder,"/EVENTS_NEW3.csv"), sep = ";")


days <- seq(as.Date("19500101", "%Y%m%d"),as.Date("20200413", "%Y%m%d"),1)
days <- sample(days,nrow(PER), replace = T)
day <- day(days)
month <- month(days)
year <- year(days)

PER[["day_of_birth"]] <-day
PER[["month_of_birth"]] <- month          
PER[["year_of_birth"]] <- year

fwrite(PER,file = paste0(folder,"/",Folder,"/PERSONS.csv"), sep = ";")

VAC_INF <- VAC[substr(vx_atc,1,5) == "J07BB",]

PER2 <- PER[year_of_birth < 1975,]

person_id <- unique(sample(PER2[["person_id"]], 0.7 * nrow(PER), replace = T))
vx_atc <- rep("J07BB02",length(person_id))
vx_record_date <- sample(seq(as.Date("20170101", "%Y%m%d"),as.Date("20200801", "%Y%m%d"),1),length(person_id), replace = T)
vx_record_date <- paste0(year(vx_record_date),sprintf("%02d",month(vx_record_date)),sprintf("%02d",day(vx_record_date)))

INF_NEW <- rbindlist(list(as.data.table(cbind(person_id,vx_atc,vx_record_date)), VAC[0]), fill = T, use.names = T)

fwrite(INF_NEW,file = paste0(folder,"/",Folder,"/VACCINES_NEW.csv"), sep = ";")


VAC_COV <- VAC[substr(vx_atc,1,7) == "J07BX03" & vx_manufacturer == "MODERNA", vx_manufacturer := "PFIZER"]
fwrite(VAC_COV,file = paste0(folder,"/",Folder,"/VACCINES.csv"), sep = ";")













