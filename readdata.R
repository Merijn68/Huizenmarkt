# Tidy data Woningmarkt 

library(tidyverse)
library(readxl)
library(magrittr)
library(here)
library(zoo)
library(janitor)


# Helper functions to wrangle data
# These functions require fixed names for translations::
#  enquo() may be able to fix this...

#' Encode type - leidt een type woning af van een subtype
#' Het type woning kan zijn een eensgezinswoning (EGW), of meersgezinswoning (MGW)
#' Data frame must have column "subtype".
#' 
#' @param df The type of datafame to be enconded
#'
#' @return altered df - with a new column Type added
encode_type <- function (df) {
  
  df <-
    df %>%
    mutate(type = case_when(
      subtype == "EGW" ~ "...",
      subtype == "Totaal woningen" ~ "Totaal",
      subtype == "Tussenwoning" ~ "EGW",
      subtype == "Hoekwoning" ~ "EGW",
      subtype == "2-onder-1-kapwoning" ~ "EGW",
      subtype == "Vrijstaande woning" ~ "EGW",
      subtype == "Appartement" ~ "MGW",
      subtype == "Appartement" ~ "MGW",
      TRUE ~ "Onbekend"
    ))
  return(df)
}

#' encode_qtryear
#' 
#' Takes the period column and generates a quarterly timeseries object
#'
#' @param df, with a column named 'period'
#'
#' @return altered df - added column 'yearqtr'
encode_qtryear <- function (df) {
  
  df <-
    df %>%
    mutate(yearqtr = as.yearqtr(period,format="Q%q %Y")) %>%
    mutate(date = as.Date(yearqtr))
  return(df)
}

#' encode_mndyear
#' 
#' Takes the period column and generates a monthly timeseries object
#'
#' @param df, with a column named 'period' 
#'
#' @return
#' @export
#'
#' @examples
encode_mndyear <- function (df) {
  
  df <-
    df %>%
    mutate(monthly = as.yearmon(period, "%m/%Y"))
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

l_provincies = c('Drenthe', 'Flevoland','Fryslân',
                 'Gelderland','Groningen','Limburg',
                 'Noord-Brabant','Noord-Holland', 
                 'Overijssel','Utrecht','Zeeland','Zuid-Holland')

l_types = c('2-onder-1-kap','Appartement','Hoekwoning',
            'Onbekend','Tussenwoning','Vrijstaand','Totaal')



# # of households
# Source: datastream

huishoudens <- 
  read_excel(here("data",
                  "datastream",
                  "aantal huishoudens.xlsx"), skip = 1) %>% 
  set_names(c('date','number_of_households')) 

# consumer_confidence 
# Source Reuters 
# TSNLEHMI,NLCONCLMQ,NLCONPPRQ

consumenten_vertrouwen <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), skip = 1, range = ('A1:D420')) %>% 
  set_names(c('period','eigen_huis_market_indicator','consumer_confidence_economic_climate','consumer_confidence_purchase_propensity')) %>%
  encode_mndyear()

# Consumer_home_purchase_intention 
# Source Reuters
# NLEUSCHOQ 

consumenten_aankoopintentie <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), skip = 1, range = ('G1:H142')) %>% 
  drop_na() %>%
  set_names(c('period','home_purchase_intention_12M')) %>%
  encode_qtryear()

# # Price Index Existing houses
# Bron: Reuters 

prijsindex_woningen <- 
  read_excel(here("data",
                  "datastream",
                  "huizenprijzen en huren grafiek.xlsx"),
             sheet = 'Sheet2',
             skip = 1, 
             range = ('E2:H106')) %>% 
  set_names(c('period','house_price_index', 'imputed_rent_value','price_to_rent_ratio'))  %>%
  encode_qtryear()
  
# huisprijzen en inkomen
# Bron: Reuters

...


# Hypotheekrente
# Bron: Reuters
# NLMORTGR + NLMORTHPR - NL MORTGAGE RATE(DISC.) NADJ

hypotheek_rente <- 
  read_excel(here("data",
                  "datastream",
                  "hypotheekrente.xlsx"),
             skip = 1
             ) %>% 
  set_names(c('period','hypotheek_rente')) %>%
  encode_mndyear() %>%
  drop_na()
  
# hypotheekrente %>%
#   write.csv2(here("data",
#                   "tidy",
#                   "hypotheekrente.csv"),
#              row.names=FALSE)

# Huizenmarkt
# Bron: Datastream


aanbod_huizen <-
  read_huizenmarkt(sheet = 'NL TOTAAL') %>%
  set_names(c('datum',
            'Totaal~te_koop',
            'Totaal~gemiddelde_aanbodtijd',
            'Totaal~gemiddelde_vraagprijs',
            'EGW~te_koop',
            'EGW~gemiddelde_aanbodtijd',
            'EGW~gemiddelde_vraagprijs',
            'MGW~te_koop',
            'MGW~gemiddelde_aanbodtijd',
            'MGW~gemiddelde_vraagprijs')) %>%
  pivot_longer(-c('datum'),
               names_to = c("type", ".value"),
               names_pattern = "(.+)~(.+)") %>%
  mutate(type = str_trim(type)) %>%
  mutate(type = factor(type)) 


# Prijsindex betaande koopwoningen Nedereland per regio
# Bron: Datastream

prijsindex_per_regio <-
  read_huizenmarkt(sheet = 'prijsindex regio Q') %>%
  pivot_longer(-c('period'),
               names_prefix = "Prijsindex bestaande koopwoningen ",
               names_to = c('region'),
               values_to = 'prijsindex_bestaande_koopwoingen') %>%
  mutate(region = str_trim(region)) %>%
  mutate(region = factor(region))  %>%
  encode_qtryear()

# Prijsindex type woning
# Bron: Datastream
# Source: Datastream


prijsindex_type_woning <-
  read_huizenmarkt(sheet = 'prijsindex soort Q') %>%
  pivot_longer(-c('period'),
               names_prefix = "Prijsindex bestaande koopwoningen ",
               names_to = c('type_woning'),
               values_to = 'prijsindex_type_woning') %>%
  mutate(type_woning = str_trim(type_woning)) %>%
  encode_qtryear()

# Nieuwbouw

nieuwbouw_per_regio <- 
  read_huizenmarkt(sheet = 'nieuwbouw cbs') %>%
  pivot_longer(-c('period'),
               names_prefix = "Toevoeging woningvoorraad door nieuwbouw",
               names_to = c('region')) %>%
  mutate(region = str_trim(region)) %>%
  encode_mndyear()

# Transacties per leeftijd
# Waarom zijn dit er zo weinig?


transacties_per_leeftijd <-
  read_huizenmarkt(sheet = 'transacties per leeftijd M') %>%
  pivot_longer(-c('period'),
               names_prefix = 'Transacties ',
               names_to = c('leeftijd'),
               values_to = 'aantal_transacties') %>% 
  encode_mndyear() %>% 
  encode_leeftijd() %>% 
  drop_na()

# Koopsom 
# Kadaster
# Datastream
# Namen van Gemiddelde koopsom per provincie zijn weggevallen in de data

gemiddelde_koopsom_per_maand <-
  read_huizenmarkt(sheet = 'gemiddelde koopsom m') %>%
  encode_mndyear() %>%
  clean_names() %>%
  drop_na()

# kadaster 
# hypotheek_per_provincie


gemiddelde_hypotheek_per_maand <-
    read_huizenmarkt(sheet = 'Kadaster M') %>%
    select (period,
            starts_with("Gemiddelde hypotheek")) %>%
    pivot_longer(cols = -1,
             names_prefix = 'Gemiddelde hypotheek ',
             names_to = 'provincie',
             values_to = 'gemiddelde_hypotheek') %>%
    mutate(provincie = factor(str_trim(provincie), levels = l_provincies))
    
aantal_hypotheken_per_maand <-
  read_huizenmarkt(sheet = 'Kadaster M') %>%
  select (period,
          starts_with("Aantal hypotheken")) %>%
  pivot_longer(cols = -1,
               names_prefix = 'Aantal hypotheken ',
               names_to = 'provincie',
               values_to = 'aantal_hypotheken') %>%
  mutate(provincie = factor(str_trim(provincie), levels = l_provincies))
    
aantal_verkocht_per_maand <- 
  read_huizenmarkt(sheet = 'Kadaster M') %>%
  select (period,
          starts_with("aantal verkocht")) %>%
  select (-c(paste('aantal verkocht',l_types))) %>%
  pivot_longer(cols = -1,
               names_prefix = 'aantal verkocht ',
               names_to = 'provincie',
               values_to = 'aantal_verkocht') %>%
  mutate(provincie = factor(str_trim(provincie), levels = l_provincies))


# Provincie is hier nog str -> bevat meer dat alleen provincies...
verkochte_woningen_per_provincie_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Aantal verkochte woningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Aantal verkochte woningen ',
               names_to = 'provincie',
               values_to = 'aantal_verkochte_woningen') %>%
  mutate(provincie = str_trim(provincie)) %>%
  mutate(yearqtr = as.yearqtr(period,format="Q%q %Y")) %>%
  mutate(date = as.Date(yearqtr))


verkochte_woningen_per_type <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Verkochte bestaande koopwoningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Verkochte bestaande koopwoningen ',
               names_to = 'subtype',
               values_to = 'verkochte_bestaande_woningen') %>%
  mutate(subtype = str_trim(subtype)) %>%
  encode_type(subtype) %>%
  filter(subtype != "EGW") %>%
  encode_qtryear

# Aantal verkocht per kwartaal per kwartaal

verkochte_woningen_per_provincie_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Aantal verkochte woningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Aantal verkochte woningen ',
               names_to = 'provincie',
               values_to = 'aantal_verkochte_woningen') %>%
  mutate(provincie = str_trim(provincie)) %>%
  encode_qtryear
  
verkochte_woningen_per_type <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Verkochte bestaande koopwoningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Verkochte bestaande koopwoningen ',
               names_to = 'subtype',
               values_to = 'verkochte_bestaande_woningen') %>%
  mutate(subtype = str_trim(subtype)) %>%
  encode_type %>%
  filter(subtype != "EGW") %>%
  encode_qtryear

gemiddelde_verkoopprijs_per_type <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Gemiddelde verkoopprijs ")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Gemiddelde verkoopprijs ',
               names_to = 'subtype',
               values_to = 'gemiddelde_verkoopprijs') %>%
  mutate(subtype = str_trim(subtype)) %>%
  encode_type %>%
  encode_qtryear

# Saving the datasets...
# Not sure yet what would be the preferred option here...
# This saves all dataframes in the current session into Rdata objects
dfs<-Filter(function(x) is.data.frame(get(x)) , ls())
for(d in dfs) {
  save(list=d, file= here("data","tidy",paste0(d, ".Rdata") ))
}

