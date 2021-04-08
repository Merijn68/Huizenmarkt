# Read Reuters Datadstram 


# Helper functions to wrangle data


#' Not in function 
#' 
`%notin%` <- Negate(`%in%`)

#' Encode type - leidt een type woning af van een subtype
#' Het type woning kan zijn een eensgezinswoning (EGW), of meersgezinswoning (MGW)
#' Data frame must have column "subtype".
#' 
#' @param df The type of datafame to be enconded
#'
#' @return altered df - with a new column Type added
encode_type <- function (df) {
  
  if (!is.data.frame(df))
  {
    cat("First argument to function should be a data frame \n");
    stop
  }
  
  df <-
    df %>%
    mutate(type = case_when(
      subtype == "EGW" ~ "...",
      subtype == "Totaal woningen" ~ "Totaal",
      subtype == "Tussenwoning" ~ "EGW",
      subtype == "Hoekwoning" ~ "EGW",
      subtype == "2-onder-1-kapwoning" ~ "EGW",
      subtype == "2-onder-1-kap" ~ "EGW",
      subtype == "Vrijstaande woning" ~ "EGW",
      subtype == "Vrijstaand" ~ "EGW",
      subtype == "Appartement" ~ "MGW",
      subtype == "Appartement" ~ "MGW",
      TRUE ~ "Onbekend"
    ))
  return(df)
}

#' encode_qtryear
#' 
#' Takes the var column and generates a quarterly timeseries object
#'
#' @param df, var
#' 
#' Var should be per data column in the dataframe
#'
#' @return altered df - added column 'yearqtr'
encode_qtryear <- function (df, var) {
  
  if (!is.data.frame(df))
  {
    cat("First argument to function should be a data frame \n");
    stop
  }
 
  var <- enquo(var)
  
  df <-
    df %>%
    mutate(yearqtr = as.yearqtr(!!var,format="Q%q %Y")) %>%
    mutate(date = first_of_quarter(as.Date(yearqtr)))
  return(df)
}

#' encode_year
#' 
#' Takes the var column and generates 
#' a date column with the first day of the year
#'
#' @param df, var
#' 
#' Var should be per data column in the dataframe
#'
#' @return altered df - added column 'date'
encode_year <- function (df, var) {
  
  if (!is.data.frame(df))
  {
    cat("First argument to function should be a data frame \n");
    stop
  }
  
  var <- enquo(var)
  
  df <-
    df %>%
    mutate(date = first_of_year(as.Date(!!var)))
  return(df)
  
}



#' encode_mndyear
#' 
#' Takes the var column and generates a monthly timeseries object
#'
#' @param df, var
#' 
#' Var should be per data column in the dataframe
#'
#' @return
#' @export
#'
#' @examples
encode_mndyear <- function (df, var) {
  
  if (!is.data.frame(df))
  {
    cat("First argument to function should be a data frame \n");
    stop
  }
  
  var <- enquo(var)
  
  df <-
    df %>%
    mutate(monthly = as.yearmon(!!var, "%m/%Y"))
  return(df)
}

#' encode_leeftijd
#' 
#' Transforms the leeftijd categories to a factor
#'
#' @param df, with a column leeftijd with leeftijd in char format
#'
#' @return df, with leeftijd encoded in factors
encode_leeftijd <- function (df) {
  
  if (!is.data.frame(df))
  {
    cat("First argument to function should be a data frame \n");
    stop
  }
  
  leeftijd_levels <- c('< 25 jaar','25-35 jaar','35-45 jaar','45-55 jaar','55-65 jaar','> 65 jaar')
  
  df <-
    df %>%
    mutate(leeftijd = factor(leeftijd,levels = leeftijd_levels)) 
  return(df)
  
}


#' read_huizenmarkt
#'
#' reads data from Reuters Datastream Excel Spreadsheets
#' Column names are taken from row 4
#' Data is read from row 18
#'
#' @param sheet - sheet to read data from
#'
#' @return df - with the data from the input sheet.
read_huizenmarkt <- function(sheet) {
  names <- read_excel(here("data",
                           "datastream",
                           "upload nl huizenmarkt unlinked.xlsm"),
                      sheet = sheet,
                      col_names = FALSE,
                      cell_limits(c(4, 3), c(4, NA))) %>%
    as.character()
  
  df = read_excel(here("data",
                       "datastream",
                       "upload nl huizenmarkt unlinked.xlsm"),
                  sheet = sheet,
                  col_names = FALSE,
                  skip = 18) %>%
    set_names(c("period", names))
  return (df)
}