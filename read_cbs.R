# Read CBS Data functions
#
# Helper functions to read data from Statline CBS

require('cbsodataR')   # CBS Opendata Statline
require(sjlabelled)    # CBS Data comes in with labels


#' Title  cbs_set_columns
#'
#' Rename columns of CBS data frames
#'
#' @param df   dataframe with names from CBS_GET_DATA
#' @param name report number of the dataframe. This is used as a lookup
#'             for the variable names.
#'
#' @return
#' @export
#'
#' @examples
cbs_set_columns <- function(df, name) {
  
  # Select dataa to be replaced for this reprot
  cn <-
    column_names %>% 
    filter(report == name)
  
  if (nrow(cn) == 0) {
    print (paste("ERROR: Report not found ", name))
    break
  }
 
  
  # Check for each row in the dataframe
  for(i in 1:nrow(cn)) {
    column_name <- cn[[i,"column_name"]]
    new_name <- cn[[i, "new_name"]]
    
    # If the column name exists - replace it
    if(column_name %in% colnames(df)) {
      names(df)[names(df) == column_name] <- new_name
    }
    else {
      print (paste(column_name,"not found in",name))
    }
  }
  
  # Renaming columns generated with CBS_ADD_DATE_COLUMN()
  if("Perioden" %in% colnames(df)) {
    names(df)[names(df) == "Perioden"] <- "period"
  }
  if("Perioden_Date" %in% colnames(df)) {
    names(df)[names(df) == "Perioden_Date"] <- "date"
  }
  if("Perioden_freq" %in% colnames(df)) {
    names(df)[names(df) == "Perioden_freq"] <- "freq"
  }
  
  # Standard CBS suplies data with labels. I rmove these for ease.  
  df<- remove_all_labels(df)
  
  print(colnames(df))
  
  # Select columns
  df <- select (df, 
                all_of(c("period","date","freq",
                         c(cn$new_name) ))
  )
  
  return (df)
} 


get_cbs_data <- function (tables) {
  
  for(i in 1:nrow(tables)) {
    report <- tables[[i,"report"]]
    table_name <- tables[[i, "table_name"]]
    
    df <- cbs_get_data(report) %>%
      cbs_add_date_column() %>% 
      cbs_add_label_columns() %>%
      cbs_set_columns(report)
    
    # assign the tables in the global environment  
    assign(table_name, df, envir = .GlobalEnv )
    
    print (paste("Report name ", report, "loaded in table", table_name))
    
  }
}
