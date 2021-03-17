# Tidy data Woningmarkt 

library(tidyverse)     # Tidyverse data management
library(readxl)        # Read Excel files
library(here)          # location of files
library(zoo)           # for time serries data
library(janitor)       # naming variables
library('cbsodataR')   # CBS Opendata Statline

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
  
  var <- enquo(var)
  
  df <-
    df %>%
    mutate(yearqtr = as.yearqtr(period,format="Q%q %Y")) %>%
    mutate(date = as.Date(yearqtr))
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

aantal_huishoudens_per_jaar <- 
  read_excel(here("data",
                  "datastream",
                  "aantal huishoudens.xlsx"), skip = 1) %>% 
  set_names(c('date','aantal_huishoudens')) %>%
  drop_na()


# consumer_confidence 
# Source Reuters 
# TSNLEHMI,NLCONCLMQ,NLCONPPRQ

consumenten_vertrouwen <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), skip = 1, range = ('A1:D420')) %>% 
  set_names(c('period','eigen_huis_market_indicator','consumer_confidence_economic_climate','consumer_confidence_purchase_propensity')) %>%
  encode_mndyear(period)

# Consumer_home_purchase_intention 
# Source Reuters
# NLEUSCHOQ 

consumenten_aankoopintentie <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), skip = 1, range = ('G1:H142')) %>% 
  drop_na() %>%
  set_names(c('period','home_purchase_intention_12M')) %>%
  encode_qtryear(period)

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
  encode_qtryear(period)
  
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
  encode_mndyear(period) %>%
  drop_na()

# Huizenmarkt
# Bron: Datastream
aanbod_huizen_per_type_per_maand <-
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
               values_to = 'prijsindex_bestaande_koopwoningen') %>%
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
  encode_qtryear(period)

# Nieuwbouw

nieuwbouw_per_regio <- 
  read_huizenmarkt(sheet = 'nieuwbouw cbs') %>%
  pivot_longer(-c('period'),
               names_prefix = "Toevoeging woningvoorraad door nieuwbouw",
               names_to = c('region')) %>%
  mutate(region = str_trim(region)) %>%
  encode_mndyear(period)





# Transacties per leeftijd
# Waarom zijn dit er zo weinig?


transacties_per_leeftijd <-
  read_huizenmarkt(sheet = 'transacties per leeftijd M') %>%
  pivot_longer(-c('period'),
               names_prefix = 'Transacties ',
               names_to = c('leeftijd'),
               values_to = 'aantal_transacties') %>% 
  encode_mndyear(period) %>% 
  encode_leeftijd() %>% 
  drop_na()

# Koopsom 
# Kadaster
# Datastream
# Namen van Gemiddelde koopsom per provincie zijn weggevallen in de data

gemiddelde_koopsom_per_maand <-
  read_huizenmarkt(sheet = 'gemiddelde koopsom m') %>%
  encode_mndyear(period) %>%
  clean_names() %>%
  drop_na()

# fkadaster 
# hypotheek_per_provincie

gemiddelde_hypotheek_per_regio_maand <-
    read_huizenmarkt(sheet = 'Kadaster M') %>%
    select (period,
            starts_with("Gemiddelde hypotheek")) %>%
    pivot_longer(cols = -1,
             names_prefix = 'Gemiddelde hypotheek ',
             names_to = 'provincie',
             values_to = 'gemiddelde_hypotheek') %>%
    mutate(provincie = factor(str_trim(provincie), levels = l_provincies))

# Aantal hypotheken per regio
    
aantal_hypotheken_per_regio_per_maand <-
  read_huizenmarkt(sheet = 'Kadaster M') %>%
  select (period,
          starts_with("Aantal hypotheken")) %>%
  pivot_longer(cols = -1,
               names_prefix = 'Aantal hypotheken ',
               names_to = 'provincie',
               values_to = 'aantal_hypotheken') %>%
  mutate(provincie = factor(str_trim(provincie), levels = l_provincies))
    
# aantal verkochte woningen per regio

aantal_verkocht_per_regio_per_maand <- 
  read_huizenmarkt(sheet = 'Kadaster M') %>%
  select (period,
          starts_with("aantal verkocht")) %>%
  select (-c(paste('aantal verkocht',l_types))) %>%
  pivot_longer(cols = -1,
               names_prefix = 'aantal verkocht ',
               names_to = 'provincie',
               values_to = 'aantal_verkocht') %>%
  mutate(provincie = factor(str_trim(provincie), levels = l_provincies))

# aantal verkochte woningen per type per maand

aantal_verkocht_per_regio_per_type_per_maand <- 
  read_huizenmarkt(sheet = 'Kadaster M') %>%
  select (period, c(paste('aantal verkocht',l_types))) %>%
  pivot_longer(cols = -1,
               names_prefix = 'aantal verkocht ',
               names_to = 'subtype',
               values_to = 'aantal_verkocht') %>%
  encode_type %>%
  filter(subtype != "Totaal") %>%
  encode_mndyear(period)

aantal_verkocht_per_regio_per_type$subtype[aantal_verkocht_per_regio_per_type$subtype == 'Vrijstaand'] <- "Vrijstaande woning"
aantal_verkocht_per_regio_per_type$subtype[aantal_verkocht_per_regio_per_type$subtype == '2-onder-1-kap'] <- "2-onder-1-kapwoning"


# Provincie is hier nog str -> bevat meer dat alleen provincies...
verkochte_woningen_per_provincie_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Aantal verkochte woningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Aantal verkochte woningen ',
               names_to = 'provincie',
               values_to = 'aantal_verkochte_woningen') %>%
  mutate(provincie = str_trim(provincie)) %>%
  encode_qtryear

# Aantal verkocht per kwartaal per kwartaal
verkochte_woningen_per_type_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Verkochte bestaande koopwoningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Verkochte bestaande koopwoningen ',
               names_to = 'subtype',
               values_to = 'verkochte_bestaande_woningen') %>%
  mutate(subtype = str_trim(subtype)) %>%
  encode_type %>%
  filter(subtype != "EGW") %>%
  encode_qtryear(period)

# Gemiddelde verkoopprijs per type per kwartaal
gemiddelde_verkoopprijs_per_type_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Gemiddelde verkoopprijs ")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Gemiddelde verkoopprijs ',
               names_to = 'subtype',
               values_to = 'gemiddelde_verkoopprijs') %>%
  mutate(subtype = str_trim(subtype)) %>%
  encode_type %>%
  encode_qtryear(period)

# Aanbod Woningen per regio per kwartaal
aanbod_woningen_per_regio_per_maand <-
  read_huizenmarkt(sheet = 'huizenzoeker') %>%
  select(period, starts_with("Aanbod aantal woningen ")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Aanbod aantal woningen ',
               names_to = 'provincie',
               values_to = 'aanbod_woningen') %>%
  mutate(provincie = str_trim(provincie)) %>%
  encode_mndyear(period)

# gem vraagprijs per regio per kwartaal
gemiddelde_vraagprijs_per_regio_per_maand <-
  read_huizenmarkt(sheet = 'huizenzoeker') %>%
  select(period, starts_with("Gem. Vraagprijs per ")) %>%
  pivot_longer(-c('period'),
               names_prefix = "Gem. Vraagprijs per ",
               names_to = 'provincie',
               values_to = 'gem_vraagprijs') %>%
  mutate(provincie = str_trim(provincie)) %>%
  encode_mndyear(period)

# gem vraagprijs per regio per kwartaal

aanbod_per_type_per_provincie_per_maand_egw <-
  read_huizenmarkt(sheet = 'COROPCIJFERS AANTAL & PRIJS') %>%
  select(period, starts_with("EGW te koop")) %>%
  pivot_longer(-c('period'),
               names_prefix = "EGW te koop ",
               names_to = 'provincie',
               values_to = 'aantal_te_koop') %>%
  mutate(provincie = str_trim(provincie)) %>%
  mutate(type = "EGW") %>%
  encode_mndyear()

aanbod_per_type_per_provincie_per_maand_mgw <-
  read_huizenmarkt(sheet = 'COROPCIJFERS AANTAL & PRIJS MGW') %>%
  select(period, starts_with("MGW te koop")) %>%
  pivot_longer(-c('period'),
               names_prefix = "MGW te koop ",
               names_to = 'provincie',
               values_to = 'aantal_te_koop') %>%
  mutate(provincie = str_trim(provincie)) %>%
  mutate(type = "MGW") %>%
  encode_mndyear(period)

aanbod_per_type_per_provincie_per_maand <-
  union (aanbod_per_type_per_provincie_per_maand_egw,
         aanbod_per_type_per_provincie_per_maand_mgw)

aanbod_per_type_per_provincie_per_maand_egw <- NULL
aanbod_per_type_per_provincie_per_maand_mgw <- NULL



# CBS Statline ------------------------------------------------------------


# Inhoudsopgave <- cbs_get_toc("Language" = "nl") %>% 
#    filter(str_detect(ShortDescription, 'voorraad woningen'))
# View(Inhoudsopgave)
# ds_nl <- cbs_get_datasets("Language" = "nl")
# voorraad_woningen_meta <- cbs_get_meta("82235NED")$DataProperties

voorraad_woningen <- cbs_get_data("82235NED") %>%
  cbs_add_date_column() %>%
  cbs_add_label_columns()



# Saving the datasets...
# Not sure yet what would be the preferred option here...
# This saves all dataframes in the current session into Rdata objects
dfs<-Filter(function(x) is.data.frame(get(x)) , ls())
for(d in dfs) {
  save(list=d, file= here("data","tidy",paste0(d, ".Rdata") ))
}

