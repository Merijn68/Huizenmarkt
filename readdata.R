# Tidy data Woningmarkt 

library(tidyverse)     # Tidyverse data management
library(readxl)        # Read Excel files
library(here)          # location of files
library(zoo)           # for time series data
# install.packages("dint")
library(dint)          # more time series
library(lubridate)     # more time series
library(janitor)       # naming variables
library('cbsodataR')   # CBS Opendata Statline
library(sjlabelled)    # CBS Data comes in with labels

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
    mutate(yearqtr = as.yearqtr(!!var,format="Q%q %Y")) %>%
    mutate(date = first_of_quarter(as.Date(yearqtr)))
  return(df)
}

encode_year <- function (df, var) {
  
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
  drop_na() %>%
  encode_year(date)


# consumer_confidence 
# Source Reuters 
# TSNLEHMI,NLCONCLMQ,NLCONPPRQ

consumenten_vertrouwen <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), 
             skip = 1, range = ('A1:D420')) %>% 
  set_names(c('period','eigen_huis_market_indicator',
              'consumer_confidence_economic_climate',
              'consumer_confidence_purchase_propensity')) %>%
  encode_mndyear(period)

# Consumer_home_purchase_intention 
# Source Reuters
# NLEUSCHOQ 

consumenten_aankoopintentie <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), 
             skip = 1, range = ('G1:H142')) %>% 
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
  set_names(c('period',
              'house_price_index', 
              'imputed_rent_value',
              'price_to_rent_ratio'))  %>%
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
  encode_qtryear(period)

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
  encode_qtryear(period) %>%
  drop_na()

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

aantal_verkocht_per_regio_per_type_per_maand$subtype[aantal_verkocht_per_regio_per_type_per_maand$subtype == 'Vrijstaand'] <- "Vrijstaande woning"
aantal_verkocht_per_regio_per_type_per_maand$subtype[aantal_verkocht_per_regio_per_type_per_maand$subtype == '2-onder-1-kap'] <- "2-onder-1-kapwoning"


# Provincie is hier nog str -> bevat meer dat alleen provincies...
verkochte_woningen_per_provincie_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Aantal verkochte woningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Aantal verkochte woningen ',
               names_to = 'provincie',
               values_to = 'aantal_verkochte_woningen') %>%
  mutate(provincie = str_trim(provincie)) %>%
  encode_qtryear(period)

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
  encode_qtryear(period) %>% 
  drop_na

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
  encode_mndyear(period)

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



# CBS fStatline data--------------------------------------------------------


tables <-
  tribble(
    ~report, ~table_name,
    #----------/------------------
    "82235NED", "voorraad_woningen",
    "84064NED", "prijsindex_woningen_cbs",
    "82900NED", "voorraad_woningen_naar_eigendom",
    "83226NED", "prognose_huishoudens",
    "83908NED", "bouwkosten_prijsindex",
    "83739NED", "inkomen"
  )

column_names<-
  tribble(
  ~report, ~column_name,  ~new_name,
  #----------/----------------------/------------
  
  "82235NED", "BeginstandVoorraad_1","beginstand",
  "82235NED", "Nieuwbouw_2","nieuwbouw",
  "82235NED", "OverigeToevoeging_3","overige_toevoegingen",
  "82235NED", "Sloop_4","sloop",
  "82235NED", "OverigeOnttrekking_5","Overige_onttrekking",
  "82235NED", "Correctie_6","correctie",
  "82235NED", "SaldoVoorraad_7","saldo_voorraad",
  "82235NED", "EindstandVoorraad_8", "voorraad",
  "84064NED", "SoortKoopwoning_label","soort_koopwoning",
  "84064NED", "PrijsindexVerkoopprijzen_1","prijsindex",
  "82900NED", "StatusVanBewoning_label","status_bewoning",
  "82900NED", "TotaleWoningvoorraad_1","totale_woningvoorraad",
  "82900NED", "Koopwoningen_2","koopwoningen",
  "82900NED", "EigendomWoningcorporatie_4","eigendom_woningcooperatie",
  "82900NED", "EigendomOverigeVerhuurders_5","overige",
  "82900NED", "EigendomOnbekend_6","eigendom_onbekend",
  "83226NED", "Eenpersoonshuishouden_10","eenpersoonshuishoudens",
  "83226NED", "TotaalMeerpersoonshuishoudens_17","meerpersoonshuishoudens",
  "83908NED", "Prijsindex_1","prijsindex",
  "83739NED", "KenmerkenVanHuishoudens_label","kenmerk_huishouden",
  "83739NED", "GemiddeldBesteedbaarInkomen_2","besteedbaar_inkomen",
  "83739NED", "MediaanVermogen_5","mediaan_vermogen"
)



cbs_set_columns <- function(df, name) {
  
  names_vec <- 
    column_names %>% 
    filter(report == name) %>%
    pull(column_name)
  
  replace_vec <- 
    column_names %>% 
    filter(report == name) %>%
    pull(new_name)
  
  
  colnames(df)[colnames(df)      # Rename variable names
               %in% names_vec] <- replace_vec
  
  colnames(df)[colnames(df) == "Perioden"]       <- "period"
  colnames(df)[colnames(df) == "Perioden_Date"]  <- "date"
  colnames(df)[colnames(df) == "Perioden_freq"]  <- "freq"
  
  df<- remove_all_labels(df)
  
  # Select columns
  df <- select (df, 
                all_of(c("period","date","freq",replace_vec ))
  )
  
  return (df)
} 

get_cbs_data <- function (tables) {
  
  for(i in 1:nrow(tables)) {
    report <- tables[[i,"report"]]
    table_name <- tables[[i, "table_name"]]
    
    print ('---Load CBS data------')
    print (table_name)
    print (typeof(table_name))
    
  
    df <- cbs_get_data(report) %>%
      cbs_add_date_column() %>% 
      cbs_set_columns(report)
      
    assign(table_name, df, envir = .GlobalEnv )
    # %>%  # Add CBS date columns
    #   cbs_set_columns(report)
  }
}
  
get_cbs_data(tables)
# Inhoudsopgave <- cbs_get_toc("Language" = "nl") %>% 
#   filter(str_detect(ShortDescription, '83906NED'))
# View(Inhoudsopgave)
# ds_nl <- cbs_get_datasets("Language" = "nl")
# voorraad_woningen_meta <- cbs_get_meta("82235NED")$DataProperties

voorraad_woningen <- 
  cbs_get_data("82235NED") %>%
  cbs_add_date_column() %>%  # Add CBS date columns
  cbs_set_columns("82235NED")
  
voorraad_woningen_naar_eigendom <- cbs_get_data("82900NED", 
                                                RegioS = "NL01  ") %>%
  cbs_add_date_column() %>%
  cbs_add_label_columns() %>%
  cbs_set_columns("82900NED") 

prognose_huishoudens <- cbs_get_data("83226NED") %>%
  cbs_add_date_column() %>%
  cbs_set_columns("83226NED") 

bouwkosten_price_index <- cbs_get_data("83908NED")  %>%
  cbs_add_date_column() %>%
  filter(Perioden_freq == 'Q') %>%
  cbs_set_columns("83908NED") 

prijsindex_woningen_cbs <-cbs_get_data("84064NED") %>%
  cbs_add_date_column() %>%
  cbs_add_label_columns()  %>%
  cbs_set_columns("84064NED") %>%
  filter(freq == 'Q') %>%
  filter(soort_koopwoning != 'Totaal koopwoningen') 
  
inkomen <- cbs_get_data("83739NED")  %>%
  cbs_add_date_column() %>%
  cbs_add_label_columns()  %>%
  cbs_set_columns("83739NED") %>%
  filter(freq == 'Y') 



# Saving the datasets...
# Not sure yet what would be the preferred option here...
# This saves all dataframes in the current session into Rdata objects
dfs<-Filter(function(x) is.data.frame(get(x)) , ls())
for(d in dfs) {
  save(list=d, file= here("data","tidy",paste0(d, ".Rdata") ))
}

