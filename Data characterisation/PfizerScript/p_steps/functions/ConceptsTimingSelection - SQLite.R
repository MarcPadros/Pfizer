

ConceptTimeSelection <- function(ID, Concept, v.date,method = "d.range", population, v.obs.start, v.obs.end, t.prior =  NULL, set.boolean = F, join = "inner"){
    
    Concept <- copy(Concept)[,eval(v.date) := as.Date(get(v.date))]
    population <- copy(population)
    #[,eval(v.obs.start) := as.Date(get(v.obs.start))][,eval(v.obs.end) := as.Date(get(v.obs.end))]
    
    Concept <- as.data.frame(Concept)
    population <- as.data.frame(population)
    
    if(method == "d.range") expr1 <- paste0("t1.",v.date," between t2.",v.obs.start," and t2.",v.obs.end)
    if(method == "d.prior") expr1 <- paste0("t1.",v.date," between t2.",v.obs.start," - ",t.prior," and t2.",v.obs.start)
      
    
    if(join == "inner") tables <- paste0("select distinct t1.*,t2.* from Concept t1 inner join population t2 on (") 
    if(join == "left") tables <- paste0("select t2.*, t1.",v.date," from population t2 left join Concept t1 on (") 
      
    expr2 <-  paste0("t1.",ID," = t2.",ID," and ")
      
    TEMP <- sqldf(paste0(tables,expr2,expr1,")"))  
    
    TEMP <- as.data.table(TEMP)  
    #TEMP <- TEMP[,eval(v.obs.start) := as.IDate(get(v.obs.start))][,eval(v.obs.end) := as.IDate(get(v.obs.end))][,eval(v.date) := as.IDate(get(v.date))]  
    
    if(set.boolean){
      
       TEMP <- TEMP[[ID]]
      
    }
    
    return(TEMP)

}


