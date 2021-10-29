


rm(list = ls(all=TRUE))


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
  Outputfolder = paste0(folder,"/Medium"), 
  N = 1000, 
  Identifier_name = "person_id", 
  Delimiter = ";"
  )



