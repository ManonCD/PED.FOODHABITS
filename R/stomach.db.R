#' @title stomach.db
#' @description This function is the main workhorse to pull data from databases and some initial filtering of data used in fish food diet analyses. Results are saved and can be reloaded using this function.
#' @param DS default is \code{'complete.redo'} This is the main switch that selects which data source to load or operate.
#' Options for DS include: 'complete' (Others can be added later).
#' Any of these arguments called as listed return the data object - 'complete' loads ALL data sources.
#' To make the data file from scratch would require a 'XXXX.redo', where XXXX is the option listed above.
#' @param this.oracle.server This is the server
#' @param this.oracle.username This is the username
#' @param this.oracle.password This is the password
#' @param datadirectory This is where you want to store your data (or where your data is already stored)
#' @param showprogress default is FALSE
#' @importFrom lubridate year
#' @importFrom utils write.csv
#' @importFrom lubridate month
#' @return Data objects that contain the data for use in further analyses.
# @examples stomach.db('complete.redo') # makes the data objects for all available data.
# stomach.db('complete') #loads the object alldata
#' @export
#'

stomach.db = function( DS="complete.redo",
                       this.oracle.server=oracle.server,
                       this.oracle.username=oracle.stomach.username,
                       this.oracle.password=oracle.stomach.password,
                       datadirectory = datadirectory,
                       showprogress = F) {
  
  DS = tolower(DS)   #make DS parameter case-insensitive
  ts <- Sys.Date()   #create time stamp
  
  #create the folder to store extractions products (rdata and csvs)
  if (is.null(datadirectory)){
    cat("Requires a value for datadirectory.  Aborting\n")
    return()
  }
  
  if (!dir.exists(datadirectory)){
    #if the specified datadirectory doesn't exist, it could be an error or intentional -
    #ask the user if they want to create it
    #if they do, it is implied we are now doing an extraction - not a load
    #ensure that the values for DS have ".redo" on the end to force the extraction
    create_dir = toupper(readline(prompt = "The specified data directory does not exist.\nType 'y' to create this folder and extract the data into it (i.e. do a *.redo).  Press any other key to cancel the operation. \n"))
    if (create_dir !="Y")return()
    dir.create(datadirectory, recursive = TRUE, showWarnings = FALSE )
    if (showprogress) cat(paste("<new folder> datadirectory: ",datadirectory))
    if (!all(grepl(x = DS,pattern = ".redo"))){
      goodDS = DS[grepl('.redo$', DS)]
      badDS = DS[!grepl('.redo$', DS)]
      badDS=paste(badDS,".redo",sep="")
      DS = c(goodDS,badDS)
    }
  }else{
    if (showprogress) cat(paste("datadirectory:",datadirectory,"\n"))
  }
  
  rdataPath = file.path(datadirectory, 'ODBCDump')
  csvPath = file.path(rdataPath,'csv')
  
  if (!dir.exists(rdataPath)){
    #check if necessary folders exist, create them if necessary
    dir.create(rdataPath, recursive = TRUE, showWarnings = FALSE )
    if (showprogress) cat(paste("<new folder> .rdata files:",rdataPath,"\n"))
  }else{
    if (showprogress) cat(paste(".rdata files:",rdataPath,"\n"))
  }
  
  if (!dir.exists(csvPath)){
    dir.create(csvPath, recursive = TRUE, showWarnings = FALSE )
    if (showprogress) cat(paste("<new folder> .csv files:",csvPath,"\n"))
  }else{
    if (showprogress) cat(paste(".csv files:",csvPath,"\n"))
  }
  
############################ HELPER FUNCTIONS #################################
  convert.dd.dddd<-function(x){
    dat<-data.frame(ddmm.mm=x,dd.dddd=NA)
    #degrees-minutes-seconds -> degrees
    ddmmss<-dat$ddmm.mm[!is.na(dat$ddmm.mm)&abs(dat$ddmm.mm)>9000]
    ddmm.ss<-ddmmss/100
    ddmm<-trunc(ddmm.ss)
    ss<-(ddmm.ss-ddmm)*100
    dd.mm<-ddmm/100
    dd<-trunc(dd.mm)
    mm<-(dd.mm-dd)*100
    dat$dd.dddd[!is.na(dat$ddmm.mm)&abs(dat$ddmm.mm)>9000]<-dd+mm/60+ss/3600
    #degrees-minutes -> degrees
    dd.mmmm<-dat$ddmm.mm[!is.na(dat$ddmm.mm)&abs(dat$ddmm.mm)>90&abs(dat$ddmm.mm)<9000]/100
    dd<-trunc(dd.mmmm)
    mm.mm<-(dd.mmmm-dd)*100
    dat$dd.dddd[!is.na(dat$ddmm.mm)&abs(dat$ddmm.mm)>90&abs(dat$ddmm.mm)<9000]<-dd+mm.mm/60
    #degrees -> degrees
    dat$dd.dddd[!is.na(dat$ddmm.mm)&abs(dat$ddmm.mm)<90]<-dat$ddmm.mm[!is.na(dat$ddmm.mm)&abs(dat$ddmm.mm)<90]
    return(dat$dd.dddd)
  }
  
  ############################# FOOD HABITS DATA HANDLING FUNCTIONS  #############
  # The processes below are now discrete functions. Each takes a 'redo'
  # parameter.  If redo=T, than the data is re-extracted from Oracle prior to
  # loading.  If F, than the data is simply loaded from the
  do.groundfish<-function(con=NULL, redo = F, this_showprogress=showprogress){
    ############################# STOMACH DATA VIEW ##########################
    r_nm = file.path(rdataPath, "GS.data.rdata")
    if (redo){
      c_nm = paste0(file.path(csvPath,paste0("GS.data.",ts)),".csv")
      Sys.setenv(TZ = 'GMT')
      Sys.setenv(ORA_SDTZ = 'GMT')
      STOMACH_DATA_VW<-ROracle::dbGetQuery(con,"select * from MFD_STOMACH.STOMACH_DATA_VW")
      STOMACH_DATA_VW$YEAR<-lubridate::year(STOMACH_DATA_VW$SDATE)
      STOMACH_DATA_VW$DATE <- paste0(lubridate::year(STOMACH_DATA_VW$SDATE),"-",
                                     sprintf("%02d",lubridate::month(STOMACH_DATA_VW$SDATE)),"-",
                                     sprintf("%02d",lubridate::day(STOMACH_DATA_VW$SDATE)))
      GS.data<-subset(STOMACH_DATA_VW,DATASOURCE=="GS")
      save(GS.data, file=r_nm, compress=T)
      utils::write.csv(GS.data, c_nm,row.names = F)
      if (this_showprogress)cat(paste("Saved:\n\t",r_nm,"\n\t",c_nm,"\n"))
    }
    load(r_nm, .GlobalEnv)
    if (this_showprogress)cat(paste("Loaded:",r_nm,"\n"))
  }

  do.speclist<-function(con=NULL, redo = F, this_showprogress=showprogress){
    ############################# STOMACH SPECIES LIST DATA ##########################
    r_nm = file.path(rdataPath, "species.data.rdata")
    if (redo){
      c_nm = paste0(file.path(csvPath,paste0("species.data",ts)),".csv")
      
      species.data<-ROracle::dbGetQuery(con,"select * from MFD_STOMACH.SDSPEC")
      save(species.data, file=r_nm, compress=T)
      utils::write.csv(species.data, c_nm,row.names = F)
      if (this_showprogress)cat(paste("Saved:\n\t",r_nm,"\n\t",c_nm,"\n"))
    }
    load(r_nm, .GlobalEnv)
    if (this_showprogress)cat(paste("Loaded:",r_nm,"\n"))
  }
  
# Make the oracle connection
  thiscon <- ROracle::dbConnect(DBI::dbDriver("Oracle"), this.oracle.username, this.oracle.password, this.oracle.server)
  if (is.null(thiscon)){
    cat("No valid connection, aborting\n")
    return()
  }
  
  if (any(DS %in% c("complete","complete.redo"))) {
    complete.flag = ifelse(any(DS %in% c("complete.redo")),T,F)
    do.speclist(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
    do.groundfish(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
#    do.details(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
#    do.observer(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
#    do.millim(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
#    do.totals(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
#    do.totalsfemtran(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
#    do.juveniles(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
  }else{
    if (grepl(DS, pattern = "speclist")){
      speclist.flag = ifelse(DS %in% c("speclist.redo"),T,F)
      do.speclist(con=thiscon,redo = speclist.flag, this_showprogress=showprogress)
    }else{
    if (grepl(DS, pattern = "groundfish")){
     groundfish.flag = ifelse(DS %in% c("groundfish.redo"),T,F)
     do.groundfish(con=thiscon,redo = groundfish.flag, this_showprogress=showprogress)
    }
#    if (grepl(DS, pattern = "details")){
#      details.flag = ifelse(DS %in% c("details.redo"),T,F)
#      do.details(con=thiscon,redo=details.flag, this_showprogress=showprogress)
#    }
#   if (grepl(DS, pattern = "observer")){
#      observer.flag = ifelse(DS %in% c("observer.redo"),T,F)
#      do.observer(con=thiscon,redo=observer.flag, this_showprogress=showprogress)
#    }
#   if (grepl(DS, pattern = "millim")){
#      millim.flag = ifelse(DS %in% c("millim.redo"),T,F)
#      do.millim(con=thiscon,redo= millim.flag, this_showprogress=showprogress)
#    }
#    if (grepl(DS, pattern = "totalsfemtran")){
#      totalsfemtran.flag = ifelse(DS %in% c("totalsfemtran.redo"),T,F)
#      do.totalsfemtran(con=thiscon,redo=totalsfemtran.flag, this_showprogress=showprogress)
#    }
#    if (grepl(DS, pattern = "totals") & nchar(DS) < 12){
#      # to prevent this check from accidentally grabbing
#      # 'totalsfemtran', it is more specific than the others (i.e. nchar(DS) <12)
#      # this will catch 'totals' or 'totals.redo'
#      totals.flag = ifelse(DS %in% c("totals.redo"),T,F)
#      do.totals(con=thiscon,redo=totals.flag, this_showprogress=showprogress)
#    }
#    if (grepl(DS, pattern = "juveniles")){
#      juveniles.flag = ifelse(DS %in% c("juveniles.redo"),T,F)
#      do.juveniles(con=thiscon,redo=juveniles.flag, this_showprogress=showprogress)
#    }
# }
#   if (any(DS %in% c("complete","complete.redo"))) {
#       complete.flag = ifelse(any(DS %in% c("complete.redo")),T,F)
#       do.complete(con=thiscon,redo=complete.flag, this_showprogress=showprogress)
#  }else{
    }
  }
  gc()
  #RODBC::odbcClose(thiscon)
}
