

setwd(projectFolder)
setwd('..')
setwd('..')

dir_base <- getwd()

path_dir<-paste0(dir_base,"/CDMInstances/",StudyName,"/")

#Set the path to where you want your report to be saved(make sure that the output folder already exists)
output_dir<-paste0(projectFolder,"/g_output/")

pre_dir<-paste0(projectFolder,"/p_steps/")

g_intermediate<-paste0(projectFolder,"/g_intermediate/")
tmp<-paste0(g_intermediate,"tmp/")
populations_dir<-paste0(g_intermediate,"populations/")
concepts_dir<-paste0(g_intermediate,"concepts/")
medication_dir<-paste0(g_intermediate,"medications/")
vaccins_dir<-paste0(g_intermediate,"vaccins/")
