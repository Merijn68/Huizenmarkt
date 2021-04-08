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
  
  # Select data to be replaced for this report
  cn <-
    column_names %>% 
    filter(report == name)
  
  if (nrow(cn) == 0) {
    print (paste("ERROR: Report not found ", name))
    break
  }
 
  # Update each column in the dataframe
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
  
  # keep only new names + dates
  df <- select (df, 
                any_of(c("Perioden","Perioden_Date","Perioden_freq",
                         c(cn$new_name) )))
  
  return (df)
} 


#' Title  get_cbs_data
#'
#' Read CBS Data tables
#'
#' @param tables dataframe with reportnames and tablenames
#'
#' @return assigns data from CBS loaded from table to global environment
#' @export
#'
#' @examples
get_cbs_data <- function (tables) {
  
  for(i in 1:nrow(tables)) {
    report <- tables[[i,"report"]]
    table_name <- tables[[i, "table_name"]]

    # Add labels and rename columns
    df <- cbs_get_data(report) %>% 
      cbs_add_label_columns() %>% 
      cbs_set_columns(report) 
    
    # Generate date columns with CBS_ADD_DATE_COLUMN() and rename them
    if("Perioden" %in% colnames(df)) {
      
      df<-cbs_add_date_column(df) 
      names(df)[names(df) == "Perioden"] <- "period"
      
      if("Perioden_Date" %in% colnames(df)) {
        names(df)[names(df) == "Perioden_Date"] <- "date"
      }
      if("Perioden_freq" %in% colnames(df)) {
        names(df)[names(df) == "Perioden_freq"] <- "freq"
      }
    }
    
    # Standard CBS suplies data with labels. I rmove these for ease.  
    df<- remove_all_labels(df)
    
    # assign the tables in the global environment  
    assign(table_name, df, envir = .GlobalEnv )
    
    print(colnames(df))
    print (paste("Report name ", report, "loaded in table", table_name))
    
  }
}
