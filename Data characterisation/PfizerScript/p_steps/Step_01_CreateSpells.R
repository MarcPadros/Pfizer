
#Author: Roel Elbers MSc.
#email: r.j.h.elbers@umcutrecht.nl
#Organisation: UMC Utrecht, Utrecht, The Netherlands
#Date: 15/07/2021
print('Import and append observation periods files. Select only rows with an op_end_date that is within 11 years from start_study date')

OBSERVATION_PERIODS <- IMPORT_PATTERN(
  pat = "OBSERVATION_PERIODS", 
  dir = path_dir, 
  colls = c("person_id","op_start_date","op_end_date"), 
  date.colls = c("op_start_date","op_end_date"),
  exprss = expression(!is.na(op_start_date))
  #exprss = expression(as.numeric(substr(start_study_date2,1,4)) - as.numeric(substr(op_end_date,1,4)) < (11))
  )

print('Set start and end date to date format and if end date is empty fill with end study date')
#lapply(c("op_start_date","op_end_date"), function (x) OBSERVATION_PERIODS <- OBSERVATION_PERIODS[,eval(x) := as.IDate(as.character(get(x)),"%Y%m%d")])
#lapply(c("op_start_date","op_end_date"), function (x) OBSERVATION_PERIODS <- OBSERVATION_PERIODS[,eval(x) := as.Date(as.character(get(x)),"%Y%m%d")])


OBSERVATION_PERIODS <- OBSERVATION_PERIODS[is.na(op_end_date), op_end_date := end_study_date]

FlowChartCreateSpells <- list()

print("Run create spells")

before <- nrow(OBSERVATION_PERIODS)

OBSERVATION_PERIODS1 <- CreateSpells(
  dataset=OBSERVATION_PERIODS,
  id="person_id" ,
  start_date = "op_start_date",
  end_date = "op_end_date",
  overlap = FALSE,
  only_overlaps = F
)

rm(OBSERVATION_PERIODS)
gc()

after <- nrow(OBSERVATION_PERIODS1)

FlowChartCreateSpells[["Spells1"]]$step <- "Run_CreateSpells"
FlowChartCreateSpells[["Spells1"]]$before <- before
FlowChartCreateSpells[["Spells1"]]$after <- after
rm(before,after)

setnames(OBSERVATION_PERIODS1, "entry_spell_category", "op_start_date")
setnames(OBSERVATION_PERIODS1, "exit_spell_category", "op_end_date")
#OBSERVATION_PERIODS1[,op_start_date := as.IDate(op_start_date)]
#OBSERVATION_PERIODS1[,op_end_date := as.IDate(op_end_date)]
#OBSERVATION_PERIODS1[,op_start_date := as.Date(op_start_date)]
#OBSERVATION_PERIODS1[,op_end_date := as.Date(op_end_date)]


#########
print("Allow some time (20 days) between to spells. So make 1 spell of it")
#########

#max_spells_gap <- 20

before <- nrow(OBSERVATION_PERIODS1)
test <- copy(OBSERVATION_PERIODS1)
#test <- OBSERVATION_PERIODS1[person_id %in% OBSERVATION_PERIODS1[duplicated(OBSERVATION_PERIODS1[["person_id"]]), person_id],]
#test <- rbind(test,test[person_id == "ConCDM_SIM_200421_00299" & num_spell == 1,][,op_start_date := op_start_date - 365][,op_end_date := op_end_date - 365][, num_spell := 0])
#test <- rbind(test,test[person_id == "ConCDM_SIM_200421_00299" & num_spell == 0,][,op_start_date := op_start_date - 2000][,op_end_date := op_end_date - 2000][, num_spell := -1])
#test <- rbind(test,test[person_id == "ConCDM_SIM_200421_00299" & num_spell == -1,][,op_start_date := op_start_date - 400][,op_end_date := op_end_date - 400][, num_spell := -2])

setkeyv(test, c("person_id", "op_start_date")) 
test <- test[, row := rownames(test)]
test <- test[, shift := shift(op_end_date,n = 1), by = person_id][, gap := op_start_date - shift ]
test <- test[gap < max_spells_gap, divide := T]
test <- test[, shift2 := shift(divide,n = -1), by = person_id][divide | shift2, group := T]
test <- test[is.na(divide) & shift2, group_id := row]

test <- test[group == T, forward_fill := group_id[1], .(cumsum(!is.na(group_id)))]
test <- test[group == T, op_start_date := min(op_start_date), by = forward_fill ]
test <- test[group == T, op_end_date := max(op_end_date), by = forward_fill ]

setorder(test, person_id, op_start_date)
OBSERVATION_PERIODS2 <- unique(test[,.(person_id,op_start_date,op_end_date)])[, num_spell := cumsum(!is.na(op_start_date)), by = person_id]
setcolorder(OBSERVATION_PERIODS2, colnames(OBSERVATION_PERIODS1))

rm(OBSERVATION_PERIODS1,test)
gc()

after <- nrow(OBSERVATION_PERIODS2)

FlowChartCreateSpells[["Spells2"]]$step <- "Close max_spell_gaps"
FlowChartCreateSpells[["Spells2"]]$before <- before
FlowChartCreateSpells[["Spells2"]]$after <- after
rm(before,after)


#check <- OBSERVATION_PERIODS1 == OBSERVATION_PERIODS2

#library(Rcpp)
#library(sqldf)

#sqldf("select t1.*, t2.op_start_date as op_start_date2, t2.op_end_date as op_end_date2 from test t1 inner join test t2 on()")

print("Select latest spell")
before <- nrow(OBSERVATION_PERIODS2)
OBSERVATION_PERIODS2 <- OBSERVATION_PERIODS2[,temp := lapply(.SD, max), by = c("person_id"), .SDcols = "num_spell"][temp == num_spell,][,temp := NULL]
after <- nrow(OBSERVATION_PERIODS2)

FlowChartCreateSpells[["Spells3"]]$step <- "Select latest spell"
FlowChartCreateSpells[["Spells3"]]$before <- before
FlowChartCreateSpells[["Spells3"]]$after <- after
rm(before,after)

FlowChartCreateSpells <- as.data.table(do.call(rbind,FlowChartCreateSpells))

saveRDS(OBSERVATION_PERIODS2, file = paste0(tmp,"OBS_SPELLS.rds"))
saveRDS(FlowChartCreateSpells, file = paste0(tmp,"FlowChartCreateSpells.rds"))


rm(OBSERVATION_PERIODS2,FlowChartCreateSpells)
gc()




