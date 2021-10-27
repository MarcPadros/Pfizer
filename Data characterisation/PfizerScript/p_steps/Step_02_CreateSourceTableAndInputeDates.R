#Author: Roel Elbers MSc.
#email: r.j.h.elbers@umcutrecht.nl
#Organisation: UMC Utrecht, Utrecht, The Netherlands
#Date: 15/07/2021

print('Import and append persons files')
PERSONS <- IMPORT_PATTERN(
  pat = "PERSONS", 
  dir = path_dir,
  colls = c("person_id","day_of_birth","month_of_birth","year_of_birth","day_of_death","month_of_death","year_of_death","sex_at_instance_creation")
  )

if(any(duplicated(PERSONS[["person_id"]]))) stop("Duplicates in person table") 

print('Set date variables (day/month/year) to integer')
dates_persons <- c("year_of_birth", "month_of_birth","day_of_birth","year_of_death", "month_of_death","day_of_death")
invisible(lapply(dates_persons, function (x) if (class(PERSONS[[x]]) != "integer") PERSONS[, eval(x) := as.integer(get(x)) ]))

SPELLS <- readRDS(paste0(tmp,"OBS_SPELLS.rds")) 
before1 <- nrow(SPELLS)
before2 <- nrow(PERSONS)

if(any(duplicated(SPELLS[,.(person_id)]))) stop("Duplicates in person or observation_period table") 

print("Inner join person table with observation_periods/spells table")
setkey(PERSONS,"person_id")
setkey(SPELLS,"person_id")

SOURCE_POPULATION <- merge(PERSONS,SPELLS, all.x = F, all.y = F)

after <- nrow(SOURCE_POPULATION)

print(paste0("Before th inner join there where ",before1," rows in the spells table and ",before2," rows in the persons table. After the inner join there are ",after," in the Source Population table"))
rm(before1,before2,after,SPELLS,PERSONS)

print('Inpute birth and death day and month')
SOURCE_POPULATION[is.na(day_of_birth) & is.na(month_of_birth) & !is.na(year_of_birth), ':=' (day_of_birth = 1, month_of_birth = 6, inputed_birth_day = T,inputed_birth_month = T)]
SOURCE_POPULATION[is.na(day_of_birth) & !is.na(year_of_birth), ':=' (day_of_birth = 16, inputed_birth_day = T)]
SOURCE_POPULATION[is.na(month_of_birth) & !is.na(year_of_birth), ':=' (month_of_birth = 6, inputed_birth_month = T)]

SOURCE_POPULATION[is.na(day_of_death) & is.na(month_of_death) & !is.na(year_of_death), ':=' (day_of_death = 1, month_of_death = 6, inputed_death_day = T,inputed_death_month = T)]
SOURCE_POPULATION[is.na(day_of_death) & !is.na(year_of_death), ':=' (day_of_death = 16, inputed_death_day = T)]
SOURCE_POPULATION[is.na(month_of_death) & !is.na(year_of_death), ':=' (month_of_death = 6, inputed_death_month = T)]

INPUTED <- SOURCE_POPULATION[inputed_birth_day == T |inputed_birth_month == T|inputed_death_day == T| inputed_death_month == T ,.(person_id,inputed_birth_day,inputed_birth_month,inputed_death_day,inputed_death_month)]
saveRDS(INPUTED,file = paste0(tmp,"INPUTED.rds"))
rm(INPUTED)
gc()

lapply(c("inputed_birth_day","inputed_birth_month","inputed_death_day","inputed_death_month"), function (x) SOURCE_POPULATION <- SOURCE_POPULATION[,eval(x) := NULL])

print('Create birth and death dates')
#PERSONS[!is.na(day_of_birth) & !is.na(month_of_birth) & !is.na(year_of_birth),birth_date := as.IDate(paste0(year_of_birth, sprintf("%02d",month_of_birth),sprintf("%02d",day_of_birth)),"%Y%m%d")]
#PERSONS[!is.na(day_of_death) & !is.na(month_of_death) & !is.na(year_of_death),death_date := as.IDate(paste0(year_of_death, sprintf("%02d",month_of_death),sprintf("%02d",day_of_death)),"%Y%m%d")]
SOURCE_POPULATION[!is.na(day_of_birth) & !is.na(month_of_birth) & !is.na(year_of_birth),birth_date := as.Date(paste0(year_of_birth, sprintf("%02d",month_of_birth),sprintf("%02d",day_of_birth)),"%Y%m%d")]
SOURCE_POPULATION[!is.na(day_of_death) & !is.na(month_of_death) & !is.na(year_of_death),death_date := as.Date(paste0(year_of_death, sprintf("%02d",month_of_death),sprintf("%02d",day_of_death)),"%Y%m%d")]


print("If op_start_date is before birth_date replace op_start_date with birth_date")
SOURCE_POPULATION <- SOURCE_POPULATION[op_start_date < birth_date, op_start_date := birth_date]

print("If op_end_date is after death_date replace op_end_date with death_date")
SOURCE_POPULATION <- SOURCE_POPULATION[op_end_date > death_date, op_end_date := death_date]

saveRDS(SOURCE_POPULATION,file = paste0(tmp,"source_population.rds"))
 
rm(SOURCE_POPULATION)
gc()
